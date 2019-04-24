/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ar_config.h"
# include "ar_ticker.h"
# include "ar_market.h"

static dict_t *dict_state;
static nw_timer update_timer;
static nw_state *state_context;
static rpc_clt *matchengine;

struct state_data {
    uint32_t cmd;
    char market[MARKET_NAME_MAX_LEN];
};

struct state_val {
    json_t  *last;
};

static uint32_t dict_market_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_market_compare(const void *value1, const void *value2)
{
    return strcmp(value1, value2);
}

static void *dict_market_dup(const void *value)
{
    return strdup(value);
}

static void dict_market_free(void *value)
{
    free(value);
}

static void *dict_state_val_dup(const void *key)
{
    struct state_val *obj = malloc(sizeof(struct state_val));
    memcpy(obj, key, sizeof(struct state_val));
    return obj;
}

static void dict_state_val_free(void *val)
{
    struct state_val *obj = val;
    if (obj->last) {
        json_decref(obj->last);
    }
    free(obj);
}

int status_ticker_update(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct state_val val;
        memset(&val, 0, sizeof(val));

        val.last = json_object();
        json_object_set(val.last, "vol", json_object_get(result, "volume"));
        json_object_set(val.last, "low", json_object_get(result, "low"));
        json_object_set(val.last, "open", json_object_get(result, "open"));
        json_object_set(val.last, "high", json_object_get(result, "high"));
        json_object_set(val.last, "last", json_object_get(result, "last"));

        dict_add(dict_state, (char *)market, &val);
        return 0;
    }

    struct state_val *info = entry->val;
    if (info->last == NULL) {
        info->last = json_object();
    }

    json_object_set(info->last, "vol", json_object_get(result, "volume"));
    json_object_set(info->last, "low", json_object_get(result, "low"));
    json_object_set(info->last, "open", json_object_get(result, "open"));
    json_object_set(info->last, "high", json_object_get(result, "high"));
    json_object_set(info->last, "last", json_object_get(result, "last"));

    return 0;
}

int depth_ticker_update(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct state_val val;
        memset(&val, 0, sizeof(val));

        val.last = json_object();
        entry = dict_add(dict_state, (char *)market, &val);
    }

    struct state_val *info = entry->val;
    if (info->last == NULL) {
        info->last = json_object();
    }

    json_t *bids = json_object_get(result, "bids");
    if (json_array_size(bids) == 1) {
        json_t *buy = json_array_get(bids, 0);
        json_object_set(info->last, "buy", json_array_get(buy, 0));
        json_object_set(info->last, "buy_amount", json_array_get(buy, 1));
    } else {
        json_object_set_new(info->last, "buy", json_string("0"));
        json_object_set_new(info->last, "buy_amount", json_string("0"));
    }

    json_t *asks = json_object_get(result, "asks");
    if (json_array_size(asks) >= 1) {
        json_t *sell = json_array_get(asks, 0);
        json_object_set(info->last, "sell", json_array_get(sell, 0));
        json_object_set(info->last, "sell_amount", json_array_get(sell, 1));
    } else {
        json_object_set_new(info->last, "sell", json_string("0"));
        json_object_set_new(info->last, "sell_amount", json_string("0"));
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
    case CMD_ORDER_DEPTH:
        ret = depth_ticker_update(state->market, result);  
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

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
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
    dict_t *dict_market = get_market();

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        query_market_depth(market);
    }
    dict_release_iterator(iter);
}

static void on_timeout(nw_state_entry *entry)
{
    struct state_data *state = entry->data;
    log_fatal("query timeout, state id: %u, command: %u", entry->id, state->cmd);
}

int init_ticker(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_market_hash_func;
    dt.key_dup        = dict_market_dup;
    dt.key_destructor = dict_market_free;
    dt.key_compare    = dict_market_compare;
    dt.val_dup        = dict_state_val_dup;
    dt.val_destructor = dict_state_val_free;

    dict_state = dict_create(&dt, 64);
    if (dict_state == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

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

    return 0;
}

json_t *get_market_ticker(const void *market)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL)
        return NULL;

    struct state_val *info = entry->val;
    if (info->last == NULL) {
        return NULL;
    }

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer((uint64_t)(current_timestamp() * 1000)));
    json_object_set(data, "ticker", info->last);

    return data;
}

json_t *get_market_ticker_all(void)
{
    json_t *ticker = json_object();

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_state);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        struct state_val *info = entry->val;
        json_object_set(ticker, market, info->last);
    }
    dict_release_iterator(iter);

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer((uint64_t)(current_timestamp() * 1000)));
    json_object_set_new(data, "ticker", ticker);

    return data;
}

