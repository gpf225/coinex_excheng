/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ar_server.h"
# include "ar_market.h"
# include "ar_ticker.h"
# include "ar_sub_all.h"
# include "ar_cache.h"

static http_svr *svr;
static rpc_clt *listener;
static dict_t *backend_cache;
static nw_state *state_context;
static dict_t *method_map;
static nw_timer timer;

static rpc_clt *matchengine;
static rpc_clt *marketprice;
static rpc_clt *cache;

struct state_data {
    nw_ses      *ses;
    uint64_t    ses_id;
    uint32_t    cmd;
    sds         cache_key;
    int         depth_limit;
};

struct cache_val {
    double      time;
    sds         data;
};

typedef int (*on_request_method)(nw_ses *ses, dict_t *params);

static int check_cache(nw_ses *ses, sds cache_key)
{
    dict_entry *entry = dict_find(backend_cache, cache_key);
    if (entry == NULL)
        return 0;

    struct cache_val *cache = entry->val;
    double now = current_timestamp();
    if ((now - cache->time) > settings.cache_timeout) {
        dict_delete(backend_cache, cache_key);
        return 0;
    }

    send_http_response_simple(ses, 200, cache->data, sdslen(cache->data));
    profile_inc("reply_cache", 1);

    return 1;
}

static struct cache_val *get_cache(sds cache_key)
{
    dict_entry *entry = dict_find(backend_cache, cache_key);
    if (entry == NULL)
        return 0;

    return entry->val;
}

static void update_cache(sds cache_key, char *message)
{
    struct cache_val val;
    val.time = current_timestamp();
    val.data = sdsnewlen(message, strlen(message));
    dict_replace(backend_cache, cache_key, &val);
}

static void clear_cache(sds cache_key)
{
    dict_delete(backend_cache, cache_key);
}

static int reply_error(nw_ses *ses, int code, const char *message, uint32_t status)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "code", json_integer(code));
    json_object_set_new(reply, "data", json_object());
    json_object_set_new(reply, "message", json_string(message));

    char *reply_str = json_dumps(reply, 0);
    send_http_response_simple(ses, status, reply_str, strlen(reply_str));
    free(reply_str);
    json_decref(reply);

    return 0;
}

static int reply_internal_error(nw_ses *ses)
{
    profile_inc("error_internal", 1);
    return reply_error(ses, 1, "internal error", 502);
}

static int reply_time_out(nw_ses *ses)
{
    profile_inc("error_timeout", 1);
    return reply_error(ses, 1, "service timeout", 504);
}

static int reply_not_found(nw_ses *ses)
{
    profile_inc("error_notfound", 1);
    return reply_error(ses, 1, "not found", 404);
}

int reply_invalid_params(nw_ses *ses)
{
    profile_inc("error_invalid_params", 1);
    return reply_error(ses, 2, "invalid params", 400);
}

int reply_result_null(nw_ses *ses)
{
    profile_inc("get_result_null", 1);
    return reply_error(ses, 3, "get result null", 400);
}

int reply_json(nw_ses *ses, json_t *data, sds cache_key)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "code", json_integer(0));
    json_object_set    (reply, "data", data);
    json_object_set_new(reply, "message", json_string("OK"));

    char *reply_str = json_dumps(reply, 0);
    send_http_response_simple(ses, 200, reply_str, strlen(reply_str));
    if (cache_key) {
        update_cache(cache_key, reply_str);
    }
    free(reply_str);
    json_decref(reply);
    profile_inc("reply_normal", 1);

    return 0;
}

static int on_ping(nw_ses *ses, dict_t *params)
{
    return send_http_response_simple(ses, 200, "pong", 4);
}

static int on_market_list(nw_ses *ses, dict_t *params)
{
    sds cache_key = sdsempty();
    cache_key = sdscatprintf(cache_key, "market_list");
    int ret = check_cache(ses, cache_key);
    if (ret > 0) {
        sdsfree(cache_key);
        return 0;
    }

    json_t *data = get_market_list();
    if (data == NULL) {
        sdsfree(cache_key);
        return reply_internal_error(ses);
    }

    reply_json(ses, data, cache_key);
    json_decref(data);
    sdsfree(cache_key);

    return 0;
}

static int on_market_info(nw_ses *ses, dict_t *params) 
{
    sds cache_key = sdsempty();
    cache_key = sdscatprintf(cache_key, "market_info_list");
    int ret = check_cache(ses, cache_key);
    if (ret > 0) {
        sdsfree(cache_key);
        return 0;
    }

    json_t *data = get_market_info_list();
    if (data == NULL) {
        sdsfree(cache_key);
        return reply_internal_error(ses);
    }

    reply_json(ses, data, cache_key);
    json_decref(data);
    sdsfree(cache_key);

    return 0;
}

static int on_market_ticker(nw_ses *ses, dict_t *params)
{
    dict_entry *entry;
    entry = dict_find(params, "market");
    if (entry == NULL)
        return reply_invalid_params(ses);
    char *market = entry->val;
    strtoupper(market);

    json_t *data = get_market_ticker(market);
    if (data == NULL) {
        return reply_invalid_params(ses);
    }

    reply_json(ses, data, NULL);
    json_decref(data);

    return 0;
}

static int on_market_ticker_all(nw_ses *ses, dict_t *params)
{
    json_t *data = get_market_ticker_all();
    if (data == NULL) {
        return reply_internal_error(ses);
    }

    reply_json(ses, data, NULL);
    json_decref(data);

    return 0;
}

static bool is_good_limit(int limit)
{
    for (int i = 0; i < settings.depth_limit.count; ++i) {
        if (settings.depth_limit.limit[i] == limit) {
            return true;
        }
    }

    return false;
}

static bool is_good_merge(const char *merge_str)
{
    mpd_t *merge = decimal(merge_str, 0);
    if (merge == NULL)
        return false;

    for (int i = 0; i < settings.depth_merge.count; ++i) {
        if (mpd_cmp(settings.depth_merge.limit[i], merge, &mpd_ctx) == 0) {
            mpd_del(merge);
            return true;
        }
    }

    mpd_del(merge);
    return false;
}

static int on_market_depth(nw_ses *ses, dict_t *params)
{
    dict_entry *entry;
    entry = dict_find(params, "market");
    if (entry == NULL)
        return reply_invalid_params(ses);
    char *market = entry->val;
    strtoupper(market);

    entry = dict_find(params, "merge");
    if (entry == NULL)
        return reply_invalid_params(ses);
    char *merge = entry->val;
    if (!is_good_merge(merge))
        return reply_invalid_params(ses);

    int limit = 20;
    entry = dict_find(params, "limit");
    if (entry) {
        limit = atoi(entry->val);
        if (!is_good_limit(limit)) {
            limit = 20;
        }
    }

    sds cache_key = sdsempty();
    cache_key = sdscatprintf(cache_key, "depth_%s_%s", market, merge);

    json_t *query_params = json_array();
    json_array_append_new(query_params, json_string(market));
    json_array_append_new(query_params, json_integer(limit));
    json_array_append_new(query_params, json_string(merge));

    int ret = check_exp_cache(ses, cache_key, CMD_CACHE_DEPTH, limit);
    if (ret > 0) {
        sdsfree(cache_key);
        return 0;
    }

    if (!rpc_clt_connected(cache)) {
        sdsfree(cache_key);
        json_decref(query_params);
        return reply_internal_error(ses);
    }

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->cache_key = cache_key;
    state->depth_limit = limit;

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_CACHE_DEPTH;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(query_params, 0);
    pkg.body_size = strlen(pkg.body);

    state->cmd = pkg.command;
    rpc_clt_send(cache, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(cache)), pkg.command, pkg.sequence, (char *)pkg.body);
    free(pkg.body);
    json_decref(query_params);

    return 0;
}

static int on_market_deals(nw_ses *ses, dict_t *params)
{
    dict_entry *entry;
    entry = dict_find(params, "market");
    if (entry == NULL)
        return reply_invalid_params(ses);
    char *market = entry->val;
    strtoupper(market);

    int last_id = 0;
    entry = dict_find(params, "last_id");
    if (entry) {
        last_id = atoi(entry->val);
        if (last_id < 0) {
            return reply_invalid_params(ses);
        }
    }

    direct_deals_result(ses, market, 1000, last_id);
    return 0;
}

static int convert_kline_type_to_interval(char *type)
{
    strtolower(type);

    if (strcmp(type, "1min") == 0) {
        return 60;
    } else if (strcmp(type, "3min") == 0) {
        return 180;
    } else if (strcmp(type, "5min") == 0) {
        return 300;
    } else if (strcmp(type, "15min") == 0) {
        return 900;
    } else if (strcmp(type, "30min") == 0) {
        return 1800;
    } else if (strcmp(type, "1hour") == 0) {
        return 3600;
    } else if (strcmp(type, "2hour") == 0) {
        return 7200;
    } else if (strcmp(type, "4hour") == 0) {
        return 14400;
    } else if (strcmp(type, "6hour") == 0) {
        return 21600;
    } else if (strcmp(type, "12hour") == 0) {
        return 43200;
    } else if (strcmp(type, "1day") == 0) {
        return 86400;
    } else if (strcmp(type, "3day") == 0) {
        return 259200;
    } else if (strcmp(type, "1week") == 0) {
        return 604800;
    } else {
        return 0;
    }
}

static int on_market_kline(nw_ses *ses, dict_t *params)
{
    dict_entry *entry;
    entry = dict_find(params, "market");
    if (entry == NULL)
        return reply_invalid_params(ses);
    char *market = entry->val;
    strtoupper(market);

    int limit = 1000;
    entry = dict_find(params, "limit");
    if (entry) {
        limit = atoi(entry->val);
        if (limit <= 0) {
            limit = 1000;
        }
    }

    entry = dict_find(params, "type");
    if (entry == NULL)
        return reply_invalid_params(ses);
    char *type = entry->val;
    int interval = convert_kline_type_to_interval(type);
    if (interval == 0)
        return reply_invalid_params(ses);

    bool is_reply = false;
    sds cache_key = sdsempty();
    cache_key = sdscatprintf(cache_key, "market_kline_%s_%d_%d", market, limit, interval);
    double now = current_timestamp();
    struct cache_val *cache_val = get_cache(cache_key);
    if (cache_val) {
        if ((now - cache_val->time) < (settings.cache_timeout * 2)) {
            send_http_response_simple(ses, 200, cache_val->data, sdslen(cache_val->data));
            profile_inc("reply_cache", 1);
            is_reply = true;
            if ((now - cache_val->time) < settings.cache_timeout) {
                sdsfree(cache_key);
                return 0;
            } else {
                cache_val->time = now;
            }
        } else {
            clear_cache(cache_key);
        }
    }

    if (!rpc_clt_connected(marketprice)) {
        sdsfree(cache_key);
        if (!is_reply) {
            return reply_internal_error(ses);
        } else {
            return -__LINE__;
        }
    }

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    if (!is_reply) {
        state->ses = ses;
        state->ses_id = ses->id;
    }
    state->cache_key = cache_key;

    time_t timestatmp = time(NULL);
    json_t *query_params = json_array();
    json_array_append_new(query_params, json_string(market));
    json_array_append_new(query_params, json_integer(timestatmp - (limit - 1) * interval - 1));
    json_array_append_new(query_params, json_integer(timestatmp));
    json_array_append_new(query_params, json_integer(interval));

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_MARKET_KLINE;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(query_params, 0);
    pkg.body_size = strlen(pkg.body);

    state->cmd = pkg.command;
    rpc_clt_send(marketprice, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(matchengine)), pkg.command, pkg.sequence, (char *)pkg.body);
    free(pkg.body);
    json_decref(query_params);

    return 0;
}

static int on_http_request(nw_ses *ses, http_request_t *request)
{
    profile_inc("visit", 1);
    http_params_t *params = http_parse_url_params(request->url);
    if (params == NULL) {
        log_error("parse url error: %s", request->url);
        return reply_invalid_params(ses);
    }

    dict_entry *entry = dict_find(method_map, params->path);
    if (entry) {
        profile_inc(params->path, 1);
        on_request_method handler = entry->val;
        int ret = handler(ses, params->params);
        if (ret < 0) {
            log_error("request fail: %d, url: %s", ret, request->url);
        }
    } else {
        reply_not_found(ses);
    }

    http_params_release(params);
    return 0;
}

static void on_timeout(nw_state_entry *entry)
{
    struct state_data *state = entry->data;
    log_fatal("query timeout, state id: %u, command: %u", entry->id, state->cmd);
    if (state->ses && state->ses->id == state->ses_id) {
        reply_time_out(state->ses);
    }
}

static void on_release(nw_state_entry *entry)
{
    struct state_data *state = entry->data;
    if (state->cache_key)
        sdsfree(state->cache_key);
}

static uint32_t dict_method_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_method_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *dict_method_key_dup(const void *key)
{
    return strdup(key);
}

static void dict_method_key_free(void *key)
{
    free(key);
}

static int add_handler(char *method, on_request_method func)
{
    if (dict_add(method_map, method, func) == NULL)
        return __LINE__;
    return 0;
}

static int init_svr(void)
{
    svr = http_svr_create(&settings.svr, on_http_request);
    if (svr == NULL)
        return -__LINE__;

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;
    st.on_release = on_release;

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_method_hash_func;
    dt.key_compare = dict_method_key_compare;
    dt.key_dup = dict_method_key_dup;
    dt.key_destructor = dict_method_key_free;

    method_map = dict_create(&dt, 64);
    if (method_map == NULL)
        return -__LINE__;

    add_handler("/ping",                    on_ping);
    add_handler("/v1/market/list",          on_market_list);
    add_handler("/v1/market/info",          on_market_info);
    add_handler("/v1/market/ticker",        on_market_ticker);
    add_handler("/v1/market/ticker/all",    on_market_ticker_all);
    add_handler("/v1/market/depth",         on_market_depth);
    add_handler("/v1/market/deals",         on_market_deals);
    add_handler("/v1/market/kline",         on_market_kline);
    
    return 0;
}

static json_t *generate_depth_data(json_t *array, int limit) 
{
    if (array == NULL)
        return json_array();

    json_t *new_data = json_array();
    int size = json_array_size(array) > limit ? limit : json_array_size(array);
    for (int i = 0; i < size; ++i) {
        json_t *unit = json_array_get(array, i);
        json_array_append(new_data, unit);
    }

    return new_data;
}

static json_t *pack_depth_result(json_t *result, uint32_t limit)
{
    json_t *asks_array = json_object_get(result, "asks");
    json_t *bids_array = json_object_get(result, "bids");

    json_t *new_result = json_object();
    json_object_set_new(new_result, "asks", generate_depth_data(asks_array, limit));
    json_object_set_new(new_result, "bids", generate_depth_data(bids_array, limit));
    json_object_set    (new_result, "last", json_object_get(result, "last"));
    json_object_set    (new_result, "time", json_object_get(result, "time"));

    return new_result;
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
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL)
        return;
    struct state_data *state = entry->data;

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (!reply) {
        reply_internal_error(state->ses);
        goto clean;
    }

    if (pkg->command == CMD_CACHE_DEPTH) {
        json_t *cache_result = json_object_get(reply, "cache_result");
        if (cache_result == NULL) { // error result, no cache_result
            json_t *error = json_object_get(reply, "error");
            if (!error) {
                reply_internal_error(state->ses);
            }
            if (!json_is_null(error)) {
                const char *message = json_string_value(json_object_get(error, "message"));
                if (message) {
                    reply_error(state->ses, 1, message, 200);
                } else {
                    reply_internal_error(state->ses);
                }
            }
        } else {
            json_t *result = json_object_get(cache_result, "result");
            if (!result) {
                reply_internal_error(state->ses);
                goto clean;
            }

            if (state->ses && state->ses->id == state->ses_id) {
                json_t *reply_depth = pack_depth_result(result, state->depth_limit);
                reply_json(state->ses, reply_depth, NULL);
                json_decref(reply_depth);
            }

            if (state->cache_key) {
                int ttl = json_integer_value(json_object_get(reply, "ttl"));
                struct cache_exp_val val;
                val.time_exp = current_millis() + ttl;
                val.result = result;
                json_incref(result);
                dict_replace_exp_cache(state->cache_key, &val);
            }
        }
    } else if (pkg->command == CMD_MARKET_KLINE) {
        json_t *error = json_object_get(reply, "error");
        if (!error) {
            reply_internal_error(state->ses);
            goto clean;
        }
        if (!json_is_null(error)) {
            const char *message = json_string_value(json_object_get(error, "message"));
            if (message) {
                reply_error(state->ses, 1, message, 200);
            } else {
                reply_internal_error(state->ses);
            }
            goto clean;
        }

        json_t *result = json_object_get(reply, "result");
        if (!result) {
            reply_internal_error(state->ses);
            goto clean;
        }

        if (state->ses && state->ses->id == state->ses_id) {
            reply_json(state->ses, result, state->cache_key);
        }
    } else {
        log_error("recv pkg from: %s, cmd: %u, sequence: %u",
                nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence);
        reply_internal_error(state->ses);
    }

clean:
    if (reply)
        json_decref(reply);
    nw_state_del(state_context, pkg->sequence);
}

static uint32_t cache_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sdslen((sds)key));
}

static int cache_dict_key_compare(const void *key1, const void *key2)
{
    return sdscmp((sds)key1, (sds)key2);
}

static void *cache_dict_key_dup(const void *key)
{
    return sdsdup((const sds)key);
}

static void cache_dict_key_free(void *key)
{
    sdsfree(key);
}

static void *cache_dict_val_dup(const void *val)
{
    struct cache_val *obj = malloc(sizeof(struct cache_val));
    memcpy(obj, val, sizeof(struct cache_val));
    return obj;
}

static void cache_dict_val_free(void *val)
{
    struct cache_val *obj = val;
    sdsfree(obj->data);
    free(val);
}

static void on_timer(nw_timer *timer, void *privdata)
{
    dict_clear(backend_cache);
}

static int init_backend(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL)
        return -__LINE__;
    if (rpc_clt_start(matchengine) < 0)
        return -__LINE__;

    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL)
        return -__LINE__;
    if (rpc_clt_start(marketprice) < 0)
        return -__LINE__;

    cache = rpc_clt_create(&settings.cache, &ct);
    if (cache == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = cache_dict_hash_function;
    dt.key_compare    = cache_dict_key_compare;
    dt.key_dup        = cache_dict_key_dup;
    dt.key_destructor = cache_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;

    backend_cache = dict_create(&dt, 64);
    if (backend_cache == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 60, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

static void on_listener_connect(nw_ses *ses, bool result)
{
    if (result) {
        log_info("connect listener success");
    } else {
        log_info("connect listener fail");
    }
}

static void on_listener_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    return;
}

static void on_listener_recv_fd(nw_ses *ses, int fd)
{
    if (nw_svr_add_clt_fd(svr->raw_svr, fd) < 0) {
        log_error("nw_svr_add_clt_fd: %d fail: %s", fd, strerror(errno));
        close(fd);
    }
}

static int init_listener_clt(void)
{
    rpc_clt_cfg cfg;
    nw_addr_t addr;
    memset(&cfg, 0, sizeof(cfg));
    cfg.name = strdup("listener");
    cfg.addr_count = 1;
    cfg.addr_arr = &addr;
    if (nw_sock_cfg_parse(AR_LISTENER_BIND, &addr, &cfg.sock_type) < 0)
        return -__LINE__;
    cfg.max_pkg_size = 1024;

    rpc_clt_type type;
    memset(&type, 0, sizeof(type));
    type.on_connect  = on_listener_connect;
    type.on_recv_pkg = on_listener_recv_pkg;
    type.on_recv_fd  = on_listener_recv_fd;

    listener = rpc_clt_create(&cfg, &type);
    if (listener == NULL)
        return -__LINE__;
    if (rpc_clt_start(listener) < 0)
        return -__LINE__;

    return 0;
}

int init_server(void)
{
    ERR_RET(init_svr());
    ERR_RET(init_backend());
    ERR_RET(init_listener_clt());

    return 0;
}

