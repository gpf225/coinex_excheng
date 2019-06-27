/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/18, create
 */

# include "mp_config.h"
# include "mp_server.h"
# include "mp_message.h"

static rpc_svr *svr;
static dict_t *dict_cache;
static nw_timer cache_timer;

struct cache_val {
    double      time;
    json_t      *result;
};

static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result, double ttl)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));
    json_object_set_new(reply, "ttl", json_integer((int)(ttl * 1000)));

    int ret = rpc_reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static bool process_cache(nw_ses *ses, rpc_pkg *pkg, sds *cache_key)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "%u", pkg->command);
    key = sdscatlen(key, pkg->body, pkg->body_size);
    dict_entry *entry = dict_find(dict_cache, key);
    if (entry == NULL) {
        *cache_key = key;
        return false;
    }

    struct cache_val *cache = entry->val;
    double now = current_timestamp();
    if ((now - cache->time) > settings.cache_timeout) {
        dict_delete(dict_cache, key);
        *cache_key = key;
        return false;
    }

    reply_result(ses, pkg, cache->result, cache->time + settings.cache_timeout - now);
    sdsfree(key);
    profile_inc("hit_cache", 1);

    return true;
}

static int add_cache(sds cache_key, json_t *result)
{
    struct cache_val cache;
    cache.time = current_timestamp();
    cache.result = result;
    json_incref(result);
    dict_replace(dict_cache, cache_key, &cache);

    return 0;
}

static int on_cmd_market_status(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    int period = json_integer_value(json_array_get(params, 1));
    if (period <= 0 || period > settings.sec_max)
        return rpc_reply_error_invalid_argument(ses, pkg);

    double task_start = current_timestamp();
    json_t *result = get_market_status(market, period);
    if (result == NULL) {
        return rpc_reply_error_internal_error(ses, pkg);
    }
    profile_inc("profile_status_times", 1);
    profile_inc("profile_status_costs", (int)((current_timestamp() - task_start) * 1000000));

    int ret = reply_result(ses, pkg, result, settings.cache_timeout);
    json_decref(result);
    return ret;
}

static int on_cmd_market_last(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    mpd_t *last = get_market_last_price(market);
    if (last == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    json_t *result = json_string_mpd(last);

    int ret = reply_result(ses, pkg, result, settings.cache_timeout);
    json_decref(result);
    return ret;
}

static int on_cmd_market_kline(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    time_t start = json_integer_value(json_array_get(params, 1));
    if (start <= 0)
        return rpc_reply_error_invalid_argument(ses, pkg);

    time_t end = json_integer_value(json_array_get(params, 2));
    time_t now = time(NULL);
    if (end > now)
        end = now;
    if (end <= 0 || start > end)
        return rpc_reply_error_invalid_argument(ses, pkg);

    int interval = json_integer_value(json_array_get(params, 3));
    if (interval <= 0)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if ((end - start) > (int64_t)interval * settings.kline_max)
        start = end - (int64_t)interval * settings.kline_max;

    sds cache_key = NULL;
    if (process_cache(ses, pkg, &cache_key))
        return 0;

    double task_start = current_timestamp();
    json_t *result = NULL;
    if (interval < 60) {
        if (60 % interval != 0) {
            sdsfree(cache_key);
            return rpc_reply_error_invalid_argument(ses, pkg);
        }
        result = get_market_kline_sec(market, start, end, interval);
    } else if (interval < 3600) {
        if (interval % 60 != 0 || 3600 % interval != 0) {
            sdsfree(cache_key);
            return rpc_reply_error_invalid_argument(ses, pkg);
        }
        result = get_market_kline_min(market, start, end, interval);
    } else if (interval < 86400) {
        if (interval % 3600 != 0 || 86400 % interval != 0) {
            sdsfree(cache_key);
            return rpc_reply_error_invalid_argument(ses, pkg);
        }
        result = get_market_kline_hour(market, start, end, interval);
    } else if (interval < 86400 * 7) {
        if (interval % 86400 != 0) {
            sdsfree(cache_key);
            return rpc_reply_error_invalid_argument(ses, pkg);
        }
        result = get_market_kline_day(market, start, end, interval);
    } else if (interval == 86400 * 7) {
        result = get_market_kline_week(market, start, end, interval);
    } else if (interval == 86400 * 30) {
        result = get_market_kline_month(market, start, end, interval);
    } else {
        sdsfree(cache_key);
        return rpc_reply_error_invalid_argument(ses, pkg);
    }

    if (result == NULL) {
        sdsfree(cache_key);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    profile_inc("profile_kline_times", 1);
    profile_inc("profile_kline_costs", (int)((current_timestamp() - task_start) * 1000000));

    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = reply_result(ses, pkg, result, settings.cache_timeout);
    json_decref(result);
    return ret;
}

static int on_cmd_market_deals(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    int limit = json_integer_value(json_array_get(params, 1));
    if (limit <= 0 || limit > MARKET_DEALS_MAX)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t last_id = json_integer_value(json_array_get(params, 2));

    sds cache_key = NULL;
    if (process_cache(ses, pkg, &cache_key))
        return 0;

    json_t *result = get_market_deals(market, limit, last_id);
    if (result == NULL) {
        sdsfree(cache_key);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = reply_result(ses, pkg, result, settings.cache_timeout);
    json_decref(result);
    return ret;
}

static int on_cmd_market_deals_last(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    sds cache_key = NULL;
    if (process_cache(ses, pkg, &cache_key))
        return 0;

    json_t *result = get_market_deals_last(market);
    if (result == NULL) {
        sdsfree(cache_key);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = reply_result(ses, pkg, result, settings.cache_timeout);
    json_decref(result);
    return ret;
}

static int on_cmd_market_deals_ext(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    int limit = json_integer_value(json_array_get(params, 1));
    if (limit <= 0 || limit > MARKET_DEALS_MAX)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t last_id = json_integer_value(json_array_get(params, 2));

    json_t *result = get_market_deals_ext(market, limit, last_id);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = reply_result(ses, pkg, result, settings.cache_timeout);
    json_decref(result);
    return ret;
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("from: %s cmd: %u, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, params_str);

    int ret;
    switch (pkg->command) {
    case CMD_MARKET_STATUS:
        profile_inc("cmd_market_status", 1);
        ret = on_cmd_market_status(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_status %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_LAST:
        profile_inc("cmd_market_last", 1);
        ret = on_cmd_market_last(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_last %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_KLINE:
        profile_inc("cmd_market_kline", 1);
        ret = on_cmd_market_kline(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_kline %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_DEALS:
        profile_inc("cmd_market_deals", 1);
        ret = on_cmd_market_deals(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_deals %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_DEALS_LAST:
        profile_inc("cmd_market_deals_last", 1);
        ret = on_cmd_market_deals_last(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_deals_last %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_DEALS_EXT:
        profile_inc("cmd_market_deals_ext", 1);
        ret = on_cmd_market_deals_ext(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_deals_ext %s fail: %d", params_str, ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

    sdsfree(params_str);
    json_decref(params);
    return;

decode_error:
    if (params) {
        json_decref(params);
    }
    sds hex = hexdump(pkg->body, pkg->body_size);
    log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", \
            nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
    sdsfree(hex);
    rpc_svr_close_clt(svr, ses);

    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
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
    json_decref(obj->result);
    free(val);
}

static void on_cache_timer(nw_timer *timer, void *privdata)
{
    dict_clear(dict_cache);
}

int init_server(int worker_id)
{
    if (settings.svr.bind_count != 1)
        return -__LINE__;
    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET)
        return -__LINE__;
    bind_arr->addr.in.sin_port = htons(ntohs(bind_arr->addr.in.sin_port) + worker_id + 1);

    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = sds_dict_hash_function;
    dt.key_compare    = sds_dict_key_compare;
    dt.key_dup        = sds_dict_key_dup;
    dt.key_destructor = sds_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;

    dict_cache = dict_create(&dt, 64);
    if (dict_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_timer, NULL);
    nw_timer_start(&cache_timer);

    return 0;
}

