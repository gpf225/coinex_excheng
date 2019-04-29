/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ar_config.h"
# include "ar_server.h"
# include "ar_ticker.h"

static dict_t  *dict_state;
static rpc_clt *cache_state;
static json_t  *ticker_all;

struct state_val {
    json_t  *last;
    json_t  *ticker;
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

static void *dict_state_val_dup(const void *key)
{
    struct state_val *obj = malloc(sizeof(struct state_val));
    memcpy(obj, key, sizeof(struct state_val));
    return obj;
}

static void dict_state_val_free(void *val)
{
    struct state_val *obj = val;
    if (obj->last)
        json_decref(obj->last);
    free(obj);
}

static int ticker_update(const char *market, json_t *result, json_t *depth)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct state_val val;
        memset(&val, 0, sizeof(val));
        entry = dict_add(dict_state, (char *)market, &val);
        if (entry == NULL) {
            log_fatal("dict_add fail");
            return -__LINE__;
        }
    }

    struct state_val *info = entry->val;
    if (info->ticker == NULL) {
        info->ticker = json_object();
    }

    json_t *ticker = json_object();
    json_object_set(ticker, "vol", json_object_get(result, "volume"));
    json_object_set(ticker, "low", json_object_get(result, "low"));
    json_object_set(ticker, "open", json_object_get(result, "open"));
    json_object_set(ticker, "high", json_object_get(result, "high"));
    json_object_set(ticker, "last", json_object_get(result, "last"));

    json_object_set(ticker, "buy", json_object_get(depth, "buy"));
    json_object_set(ticker, "buy_amount", json_object_get(depth, "buy_amount"));
    json_object_set(ticker, "sell", json_object_get(depth, "sell"));
    json_object_set(ticker, "sell_amount", json_object_get(depth, "sell_amount"));

    json_object_set    (info->ticker, "date", json_object_get(result, "date"));
    json_object_set_new(info->ticker, "ticker", ticker);

    // ticker_all
    if (json_object_get(ticker_all, "ticker") == NULL) {
        json_t *ticker_market = json_object();
        json_object_set    (ticker_market, market, ticker);
        json_object_set_new(ticker_all, "ticker", ticker_market);
    } else {
        json_t *ticker_market = json_object_get(ticker_all, "ticker");
        json_object_set(ticker_market, market, ticker);
    }
    json_object_set(ticker_all, "date", json_object_get(result, "date"));

    return 0;
}

// state update
static int on_state_update(json_t *result_array, nw_ses *ses, rpc_pkg *pkg)
{
    log_trace("state update");
    const size_t state_num = json_array_size(result_array);

    for (size_t i = 0; i < state_num; ++i) {
        json_t *row = json_array_get(result_array, i);
        if (!json_is_object(row)) {
            return -__LINE__;
        }

        const char *market = json_string_value(json_object_get(row, "name"));
        if (market == NULL) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
            sdsfree(reply_str);
            continue;
        }

        json_t *depth = json_object_get(row, "depth");
        json_t *result = json_object_get(row, "result");
        if (result == NULL || depth == NULL) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
            sdsfree(reply_str);
            continue;
        }

        // add to dict_state
        dict_entry *entry = dict_find(dict_state, market);
        if (entry == NULL) {
            struct state_val val;
            memset(&val, 0, sizeof(val));

            val.last = result;
            json_incref(result);

            entry = dict_add(dict_state, (char *)market, &val);
            if (entry == NULL) {
                log_fatal("dict_add fail");
                return -__LINE__;
            }

            ticker_update(market, result, depth);
        } else {
            struct state_val *info = entry->val;
            char *last_str = NULL;
            if (info->last)
                last_str = json_dumps(info->last, JSON_SORT_KEYS);
            char *curr_str = json_dumps(result, JSON_SORT_KEYS);

            if (info->last == NULL || strcmp(last_str, curr_str) != 0) {
                if (info->last)
                    json_decref(info->last);
                info->last = result;
                json_incref(result);

                json_t *depth = json_object_get(row, "depth");
                ticker_update(market, result, depth);
            }

            if (last_str != NULL)
                free(last_str);
            free(curr_str);
        }
    }

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

void direct_state_reply(nw_ses *ses, json_t *params, int64_t id)
{
    if (json_array_size(params) != 2) {
        reply_invalid_params(ses);
        return;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market) {
        reply_invalid_params(ses);
        return;
    }

    bool is_reply = false;
    dict_entry *entry = dict_find(dict_state, market);
    if (entry != NULL) {
        struct state_val *val = entry->val;
        if (val->last != NULL) {
            is_reply = true;
            reply_json(ses, val->last);
        }
    }

    if (!is_reply) {
        reply_result_null(ses);
        log_error("state not find result, market: %s", market);
    }

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
    dt.hash_function  = dict_market_hash_func;
    dt.key_dup        = dict_market_key_dup;
    dt.key_destructor = dict_market_key_free;
    dt.key_compare    = dict_market_key_compare;
    dt.val_dup        = dict_state_val_dup;
    dt.val_destructor = dict_state_val_free;

    dict_state = dict_create(&dt, 64);
    if (dict_state == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    ticker_all = json_object();
    return 0;
}

json_t *get_market_ticker(const void *market)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL)
        return NULL;

    struct state_val *info = entry->val;
    return info->ticker;
}

json_t *get_market_ticker_all(void)
{
    if (json_object_get(ticker_all, "ticker") == NULL)
        return NULL;
    return ticker_all;
}

