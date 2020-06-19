/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ar_config.h"
# include "ar_server.h"
# include "ar_ticker.h"

# define TRADE_ZONE_SUFFIX       "_ZONE"
# define TRADE_ZONE_REAL_SUFFIX  "_ZONE_REAL"
# define INDEX_SUFFIX            "_INDEX"

static dict_t  *dict_state;
static rpc_clt *cache_state;
static json_t  *ticker_all;

struct state_val {
    int     id;
    json_t  *data;
};

static void *dict_state_val_dup(const void *key)
{
    struct state_val *obj = malloc(sizeof(struct state_val));
    memcpy(obj, key, sizeof(struct state_val));
    return obj;
}

static void dict_state_val_free(void *val)
{
    struct state_val *obj = val;
    if (obj->data)
        json_decref(obj->data);
    free(obj);
}

static int update_ticker(const char *market, int update_id, uint64_t date, json_t *state, json_t *depth)
{
    json_t *ticker = json_object();
    json_object_set(ticker, "vol",  json_object_get(state, "volume"));
    json_object_set(ticker, "low",  json_object_get(state, "low"));
    json_object_set(ticker, "open", json_object_get(state, "open"));
    json_object_set(ticker, "high", json_object_get(state, "high"));
    json_object_set(ticker, "last", json_object_get(state, "last"));
    json_object_set(ticker, "buy", json_object_get(depth, "buy"));
    json_object_set(ticker, "buy_amount", json_object_get(depth, "buy_amount"));
    json_object_set(ticker, "sell", json_object_get(depth, "sell"));
    json_object_set(ticker, "sell_amount", json_object_get(depth, "sell_amount"));

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer(date));
    json_object_set_new(data, "ticker", ticker);

    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct state_val val;
        memset(&val, 0, sizeof(val));
        entry = dict_add(dict_state, (char *)market, &val);
    }

    struct state_val *info = entry->val;
    info->id = update_id;
    if (info->data != NULL) {
        json_decref(info->data);
    }
    info->data = data;

    return 0;
}

static int update_market(json_t *row, int update_id)
{
    const char *market = json_string_value(json_object_get(row, "name"));
    if (market == NULL)
        return -__LINE__;

    uint64_t date = json_integer_value(json_object_get(row, "date"));
    if (date == 0)
        return -__LINE__;

    json_t *state = json_object_get(row, "state");
    if (state == NULL || !json_is_object(state))
        return -__LINE__;

    json_t *depth = json_object_get(row, "depth");
    if (depth == NULL || !json_is_object(depth))
        return -__LINE__;

    return update_ticker(market, update_id, date, state, depth);
}

static bool is_ticker_all(const char *market)
{
    if (strstr(market, TRADE_ZONE_SUFFIX) || strstr(market, TRADE_ZONE_REAL_SUFFIX) || strstr(market, INDEX_SUFFIX))
        return false;
    return true;
}

static void update_ticker_all(void)
{
    json_t *ticker = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_state);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        if (!is_ticker_all(market))
            continue;
        struct state_val *info = entry->val;
        if (info->data) {
            json_object_set(ticker, market, json_object_get(info->data, "ticker"));
        }
    }
    dict_release_iterator(iter);

    uint64_t date = (uint64_t)(current_timestamp() * 1000);
    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer(date));
    json_object_set_new(data, "ticker", ticker);

    if (ticker_all != NULL)
        json_decref(ticker_all);
    ticker_all = data;
}

// state update
static int on_state_update(json_t *result_array, nw_ses *ses, rpc_pkg *pkg)
{
    static uint32_t update_id = 0;
    update_id += 1;

    const size_t state_num = json_array_size(result_array);
    for (size_t i = 0; i < state_num; ++i) {
        json_t *row = json_array_get(result_array, i);
        if (!json_is_object(row)) {
            return -__LINE__;
        }

        int ret = update_market(row, update_id);
        if (ret < 0) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
            sdsfree(reply_str);
            return -__LINE__;
        }
    }

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_state);
    while ((entry = dict_next(iter)) != NULL) {
        struct state_val *info = entry->val;
        if (info->id != update_id) {
            dict_delete(dict_state, entry->key);
            log_info("delete market: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);

    update_ticker_all();

    return 0;
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

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence);
    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (!reply) {
        log_error("json_loadb fail");
        goto clean;
    }
    json_t *error = json_object_get(reply, "error");
    if (!error) {
        log_error("error param not find");
        goto clean;
    }
    if (!json_is_null(error)) {
        log_error("error is not null");
        goto clean;
    }

    json_t *result = json_object_get(reply, "result");
    if (!result) {
        log_error("result param not find");
        goto clean;
    }

    switch (pkg->command) {
    case CMD_CACHE_STATUS_UPDATE:
        on_state_update(result, ses, pkg);
        break;
    default:
        break;
    }

clean:
    if (reply)
        json_decref(reply);

    return;
}

int init_ticker(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cache_state = rpc_clt_create(&settings.cache_state, &ct);
    if (cache_state == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache_state) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.key_compare    = str_dict_key_compare;
    dt.val_dup        = dict_state_val_dup;
    dt.val_destructor = dict_state_val_free;

    dict_state = dict_create(&dt, 64);
    if (dict_state == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    return 0;
}

json_t *get_market_ticker(const void *market)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL)
        return NULL;

    struct state_val *info = entry->val;
    return json_incref(info->data);
}

json_t *get_market_ticker_all(void)
{
    return json_incref(ticker_all);
}

