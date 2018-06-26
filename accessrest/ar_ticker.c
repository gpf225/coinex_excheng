/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ar_config.h"
# include "ar_ticker.h"

static nw_timer update_timer;
static nw_timer market_timer;

static dict_t *dict_market;
static rpc_clt *marketprice;
static rpc_clt *matchengine;
static nw_state *state_context;

struct state_data {
    uint32_t cmd;
    char market[MARKET_NAME_MAX_LEN];
};

struct market_val {
    int     id;
    json_t *last;
};

static uint32_t dict_market_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_market_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *dict_market_key_dup(const void *key)
{
    return strdup(key);
}

static void dict_market_key_free(void *key)
{
    free(key);
}

static void *dict_market_val_dup(const void *key)
{
    struct market_val *obj = malloc(sizeof(struct market_val));
    memcpy(obj, key, sizeof(struct market_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    struct market_val *obj = val;
    if (obj->last)
        json_decref(obj->last);
    free(obj);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static int on_market_list_reply(struct state_data *state, json_t *result)
{
    static uint32_t update_id = 0;
    update_id += 1;

    for (size_t i = 0; i < json_array_size(result); ++i) {
        json_t *item = json_array_get(result, i);
        const char *name = json_string_value(json_object_get(item, "name"));
        dict_entry *entry = dict_find(dict_market, name);
        if (entry == NULL) {
            struct market_val val;
            memset(&val, 0, sizeof(val));
            val.id = update_id;
            dict_add(dict_market, (char *)name, &val);
            log_info("add market: %s", name);
        } else {
            struct market_val *info = entry->val;
            info->id = update_id;
        }
    }

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->id != update_id) {
            dict_delete(dict_market, entry->key);
            log_info("del market: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);

    return 0;
}

static int on_market_status_reply(struct state_data *state, json_t *result)
{
    dict_entry *entry = dict_find(dict_market, state->market);
    if (entry == NULL)
        return -__LINE__;
    struct market_val *info = entry->val;

    if (info->last == NULL) {
        info->last = json_object();
    }

    json_object_set(info->last, "vol", json_object_get(result, "volume"));
    json_object_set(info->last, "low", json_object_get(result, "low"));
    json_object_set(info->last, "high", json_object_get(result, "high"));
    json_object_set(info->last, "last", json_object_get(result, "last"));

    return 0;
}

static int on_order_depth_reply(struct state_data *state, json_t *result)
{
    dict_entry *entry = dict_find(dict_market, state->market);
    if (entry == NULL)
        return -__LINE__;
    struct market_val *info = entry->val;

    if (info->last == NULL) {
        info->last = json_object();
    }

    json_t *bids = json_object_get(result, "bids");
    if (json_array_size(bids) == 1) {
        json_t *buy = json_array_get(bids, 0);
        json_object_set(info->last, "buy", json_array_get(buy, 0));
    } else {
        json_object_set_new(info->last, "buy", json_string("0"));
    }

    json_t *asks = json_object_get(result, "asks");
    if (json_array_size(asks) == 1) {
        json_t *sell = json_array_get(asks, 0);
        json_object_set(info->last, "sell", json_array_get(sell, 0));
    } else {
        json_object_set_new(info->last, "sell", json_string("0"));
    }

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u, reply: %s",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, reply_str);
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        sdsfree(reply_str);
        return;
    }
    struct state_data *state = entry->data;

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sdsfree(reply_str);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_MARKET_LIST:
        ret = on_market_list_reply(state, result);
        if (ret < 0) {
            log_error("on_market_list_reply: %d, reply: %s", ret, reply_str);
        }
        break;
    case CMD_MARKET_STATUS:
        ret = on_market_status_reply(state, result);
        if (ret < 0) {
            log_error("on_market_status_reply: %d, reply: %s", ret, reply_str);
        }
        break;
    case CMD_ORDER_DEPTH:
        ret = on_order_depth_reply(state, result);
        if (ret < 0) {
            log_error("on_order_depth_reply: %d, reply: %s", ret, reply_str);
        }
        break;
    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    sdsfree(reply_str);
    json_decref(reply);
    nw_state_del(state_context, pkg->sequence);
}

static void on_timeout(nw_state_entry *entry)
{
    struct state_data *state = entry->data;
    log_fatal("query status timeout, state id: %u, command: %u", entry->id, state->cmd);
}

static int query_market_list(void)
{
    json_t *params = json_array();
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_MARKET_LIST;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    state->cmd = pkg.command;
    rpc_clt_send(matchengine, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(matchengine)), pkg.command, pkg.sequence, (char *)pkg.body);
    free(pkg.body);
    json_decref(params);

    return 0;
}

static int query_market_status(const char *market)
{
    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(86400));

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_MARKET_STATUS;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    state->cmd = pkg.command;
    rpc_clt_send(marketprice, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(marketprice)), pkg.command, pkg.sequence, (char *)pkg.body);
    free(pkg.body);
    json_decref(params);

    return 0;
}

static int query_market_depth(const char *market)
{
    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(1));
    json_array_append_new(params, json_string("0"));

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_ORDER_DEPTH;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    state->cmd = pkg.command;
    rpc_clt_send(matchengine, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(matchengine)), pkg.command, pkg.sequence, (char *)pkg.body);
    free(pkg.body);
    json_decref(params);

    return 0;
}

static void on_update_timer(nw_timer *timer, void *privdata)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        query_market_depth(market);
        query_market_status(market);
    }
    dict_release_iterator(iter);
}

static void on_market_timer(nw_timer *timer, void *privdata)
{
    query_market_list();
}

int init_ticker(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_market_hash_func;
    dt.key_compare = dict_market_key_compare;
    dt.key_dup = dict_market_key_dup;
    dt.key_destructor = dict_market_key_free;
    dt.val_dup = dict_market_val_dup;
    dt.val_destructor = dict_market_val_free;

    dict_market = dict_create(&dt, 64);
    if (dict_market == NULL)
        return -__LINE__;

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL)
        return -__LINE__;
    if (rpc_clt_start(marketprice) < 0)
        return -__LINE__;

    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL)
        return -__LINE__;
    if (rpc_clt_start(matchengine) < 0)
        return -__LINE__;

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL)
        return -__LINE__;

    nw_timer_set(&update_timer, settings.state_interval, true, on_update_timer, NULL);
    nw_timer_start(&update_timer);

    nw_timer_set(&market_timer, settings.market_interval, true, on_market_timer, NULL);
    nw_timer_start(&market_timer);

    on_update_timer(NULL, NULL);
    on_market_timer(NULL, NULL);

    return 0;
}

json_t *get_market_list(void)
{
    json_t *data = json_array();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        json_array_append_new(data, json_string(entry->key));
    }
    dict_release_iterator(iter);

    return data;
}

json_t *get_market_ticker(const void *market)
{
    dict_entry *entry = dict_find(dict_market, market);
    if (entry == NULL) {
        return NULL;
    }
    struct market_val *info = entry->val;
    if (info->last == NULL) {
        return NULL;
    }

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer(time(NULL)));
    json_object_set(data, "ticker", info->last);

    return data;
}

json_t *get_market_ticker_all(void)
{
    json_t *ticker = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        struct market_val *info = entry->val;
        json_object_set(ticker, market, info->last);
    }
    dict_release_iterator(iter);

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer(time(NULL)));
    json_object_set_new(data, "ticker", ticker);

    return data;
}

