/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ar_config.h"
# include "ar_ticker.h"
# include "ar_market.h"

static nw_timer update_timer;
static rpc_clt *cache;
static nw_state *state_context;

struct state_data {
    uint32_t cmd;
    char market[MARKET_NAME_MAX_LEN];
};

struct market_val {
    int     id;
    json_t  *last;
};

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static int on_market_status_reply(struct state_data *state, json_t *result)
{
    dict_t *dict_market = get_market();
    dict_entry *entry = dict_find(dict_market, state->market);
    if (entry == NULL)
        return -__LINE__;
    struct market_val *info = entry->val;

    if (info->last != NULL)
        json_decref(info->last);

    info->last = json_object();
    json_object_set(info->last, "vol", json_object_get(result, "volume"));
    json_object_set(info->last, "low", json_object_get(result, "low"));
    json_object_set(info->last, "open", json_object_get(result, "open"));
    json_object_set(info->last, "high", json_object_get(result, "high"));
    json_object_set(info->last, "last", json_object_get(result, "last"));

    return 0;
}

static int on_order_depth_reply(struct state_data *state, json_t *result)
{
    dict_t *dict_market = get_market();
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
        json_object_set(info->last, "buy_amount", json_array_get(buy, 1));
    } else {
        json_object_set_new(info->last, "buy", json_string("0"));
        json_object_set_new(info->last, "buy_amount", json_string("0"));
    }

    json_t *asks = json_object_get(result, "asks");
    if (json_array_size(asks) == 1) {
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

    json_t *cache_result = json_object_get(reply, "cache_result");
    json_t *error = json_object_get(cache_result, "error");
    json_t *result = json_object_get(cache_result, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_CACHE_STATUS:
        ret = on_market_status_reply(state, result);
        if (ret < 0) {
            log_error("on_market_status_reply: %d, reply: %s", ret, reply_str);
        }
        break;
    case CMD_CACHE_DEPTH:
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
    log_fatal("query timeout, state id: %u, command: %u", entry->id, state->cmd);
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
    pkg.command   = CMD_CACHE_STATUS;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    state->cmd = pkg.command;
    rpc_clt_send(cache, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(cache)), pkg.command, pkg.sequence, (char *)pkg.body);
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
    pkg.command   = CMD_CACHE_DEPTH;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    state->cmd = pkg.command;
    rpc_clt_send(cache, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(cache)), pkg.command, pkg.sequence, (char *)pkg.body);
    free(pkg.body);
    json_decref(params);

    return 0;
}

static void on_update_timer(nw_timer *timer, void *privdata)
{
    dict_t *dict_market = get_market();
    if (dict_size(dict_market) == 0) {
        log_error("dict_market is null");
        return;
    }

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        query_market_depth(market);
        query_market_status(market);
    }
    dict_release_iterator(iter);
}

int init_ticker(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cache = rpc_clt_create(&settings.cache, &ct);
    if (cache == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache) < 0)
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
    dict_t *dict_market = get_market();
    dict_entry *entry = dict_find(dict_market, market);
    if (entry == NULL) {
        return NULL;
    }
    struct market_val *info = entry->val;
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
    dict_t *dict_market = get_market();
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
    json_object_set_new(data, "date", json_integer((uint64_t)(current_timestamp() * 1000)));
    json_object_set_new(data, "ticker", ticker);

    return data;
}

