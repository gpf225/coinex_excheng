/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# include "iw_config.h"
# include "iw_server.h"
# include "iw_depth.h"
# include "iw_deals.h"
# include "iw_order.h"
# include "iw_asset.h"
# include "iw_kline.h"
# include "ut_ws.h"

static ws_svr *svr;
static dict_t *method_map;
static dict_t *backend_cache;
static rpc_clt *listener;
static nw_state *state_context;
static nw_cache *privdata_cache;
static nw_timer timer;

static rpc_clt *matchengine;
static rpc_clt *marketprice;
static rpc_clt *readhistory;
static rpc_clt **cachecenter_clt_arr;

struct state_data {
    nw_ses      *ses;
    uint64_t    ses_id;
    uint64_t    request_id;
    sds         cache_key;
    int         depth_limit;
};

struct cache_val {
    uint64_t    time_cache;
    json_t      *result;
};

typedef int (*on_request_method)(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params);

static bool is_good_limit(int limit)
{
    for (int i = 0; i < settings.depth_limit.count; ++i) {
        if (settings.depth_limit.limit[i] == limit) {
            return true;
        }
    }

    return false;
}

static bool is_good_interval(const char *interval)
{
    if (interval == NULL || strlen(interval) > INTERVAL_MAX_LEN) {
        return false; 
    }

    mpd_t *merge = decimal(interval, 0);
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

static bool is_good_market(const char *market)
{
    if (market == NULL || strlen(market) == 0 || strlen(market) > MARKET_NAME_MAX_LEN) {
        return false;     
    }

    return true;
}

int ws_send_error_unknown_sub_user(nw_ses *ses, uint64_t id)
{
    profile_inc("error_unknown_sub_user", 1);
    return ws_send_error(ses, id, 1, "unknown sub user");
}

int ws_send_error_direct_result_null(nw_ses *ses, int64_t id)
{
    profile_inc("error_direct_result_null", 1);
    return ws_send_error(ses, id, 2, "direct result null");
}

static int on_method_server_ping(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    json_t *result = json_string("pong");
    int ret = ws_send_result(ses, id, result);
    json_decref(result);
    return ret;
}

static int on_method_server_time(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    json_t *result = json_integer(time(NULL));
    int ret = ws_send_result(ses, id, result);
    json_decref(result);
    return ret;
}

static int check_cache(nw_ses *ses, uint64_t id, sds key)
{
    dict_entry *entry = dict_find(backend_cache, key);
    if (entry == NULL)
        return 0;

    struct cache_val *cache = entry->val;
    if (current_millisecond() >= cache->time_cache) {
        dict_delete(backend_cache, key);
        return 0;
    }

    ws_send_result(ses, id, cache->result);
    profile_inc("hit_cache", 1);

    return 1;
}

static sds get_depth_cache_key(const char *market, const char *interval)
{
    sds key = sdsempty();
    return sdscatprintf(key, "%u-%s-%s", CMD_CACHE_DEPTH, market, interval);
}

static int check_depth_cache(nw_ses *ses, uint64_t id, sds key, int limit)
{
    dict_entry *entry = dict_find(backend_cache, key);
    if (entry == NULL)
        return 0;

    struct cache_val *cache = entry->val;
    if (current_millisecond() >= cache->time_cache) {
        dict_delete(backend_cache, key);
        return 0;
    }

    json_t *result = pack_depth_result(cache->result, limit);
    ws_send_result(ses, id, result);
    json_decref(result);
    profile_inc("hit_cache", 1);

    return 1;
}

void update_depth_cache(json_t *data, const char *market, const char *interval, int ttl)
{
    sds key = get_depth_cache_key(market, interval);
    struct cache_val val;
    val.time_cache = current_millisecond() + ttl;
    val.result = data;
    json_incref(data);
    dict_replace(backend_cache, key, &val);
    sdsfree(key);

    return;
}

static int on_method_kline_query(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (!rpc_clt_connected(marketprice))
        return ws_send_error_internal_error(ses, id);

    sds key = sdsempty();
    char *params_str = json_dumps(params, 0);
    key = sdscatprintf(key, "%u-%s", CMD_MARKET_KLINE, params_str);
    int ret = check_cache(ses, id, key);
    if (ret > 0) {
        sdsfree(key);
        free(params_str);
        return 0;
    }

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;
    state->cache_key = key;

    rpc_request_json(marketprice, CMD_MARKET_KLINE, entry->id, id, params);
    free(params_str);

    return 0;
}

static int on_method_kline_subscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    const size_t sub_size = json_array_size(params);
    if (sub_size == 0) {
        return ws_send_error_invalid_argument(ses, id);
    }

    kline_unsubscribe(ses);
    for (size_t i = 0; i < sub_size; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        int interval = json_integer_value(json_array_get(item, 1));
        if (market == NULL || strlen(market) > MARKET_NAME_MAX_LEN || interval <= 0) {
            kline_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        if (kline_subscribe(ses, market, interval) < 0) {
            kline_unsubscribe(ses);
            return ws_send_error_internal_error(ses, id);
        }
    }

    return ws_send_success(ses, id);
}

static int on_method_kline_unsubscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    kline_unsubscribe(ses);
    return ws_send_success(ses, id);
}

static rpc_clt *get_cache_clt(const char *market)
{
    uint32_t hash = dict_generic_hash_function(market, strlen(market));
    return cachecenter_clt_arr[hash % settings.cachecenter_worker_num];
}

static int on_method_depth_query(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (json_array_size(params) != 3) {
        return ws_send_error_invalid_argument(ses, id);
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL) {
        return ws_send_error_invalid_argument(ses, id);
    }

    uint32_t limit = json_integer_value(json_array_get(params, 1));
    if (!is_good_limit(limit)) {
        limit = settings.depth_limit_default;
    }
    const char *interval = json_string_value(json_array_get(params, 2));
    if (interval == NULL || !is_good_interval(interval)) {
        return ws_send_error_invalid_argument(ses, id);
    }

    sds key = get_depth_cache_key(market, interval);
    int ret = check_depth_cache(ses, id, key, limit);
    if (ret > 0) {
        sdsfree(key);
        return 0;
    }

    rpc_clt *clt = get_cache_clt(market);
    if (!rpc_clt_connected(clt)) {
        sdsfree(key);
        return ws_send_error_internal_error(ses, id);
    }

    json_t *new_params = json_array();
    json_array_append_new(new_params, json_string(market));
    json_array_append_new(new_params, json_integer(limit));
    json_array_append_new(new_params, json_string(interval));

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;
    state->cache_key = key;
    state->depth_limit = limit;

    rpc_request_json(clt, CMD_CACHE_DEPTH, entry->id, id, new_params);
    json_decref(new_params);

    return 0;
}

static int on_method_depth_subscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    const size_t sub_size = json_array_size(params);
    if (sub_size == 0) {
        return ws_send_error_invalid_argument(ses, id);
    }

    depth_unsubscribe(ses);
    for (size_t i = 0; i < sub_size; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        if (market == NULL) {
            depth_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        int limit = json_integer_value(json_array_get(item, 1));
        const char *interval = json_string_value(json_array_get(item, 2));
        if (!is_good_market(market) || !is_good_interval(interval) || !is_good_limit(limit)) {
            depth_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        int ret = depth_subscribe(ses, market, limit, interval, false);
        if (ret == -1) {
            depth_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        } else if (ret < 0) {
            depth_unsubscribe(ses);
            return ws_send_error_internal_error(ses, id);
        }
    }

    ws_send_success(ses, id);
    for (size_t i = 0; i < sub_size; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        int limit = json_integer_value(json_array_get(item, 1));
        const char *interval = json_string_value(json_array_get(item, 2));
        depth_send_clean(ses, market, limit, interval);
    }
    return 0;
}

static int on_method_depth_unsubscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    depth_unsubscribe(ses);
    return ws_send_success(ses, id);
}

static int on_method_depth_subscribe_full(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    const size_t sub_size = json_array_size(params);
    if (sub_size == 0) {
        return ws_send_error_invalid_argument(ses, id);
    }

    depth_unsubscribe(ses);
    for (size_t i = 0; i < sub_size; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        if (market == NULL) {
            depth_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        int limit = json_integer_value(json_array_get(item, 1));
        const char *interval = json_string_value(json_array_get(item, 2));
        if (!is_good_market(market) || !is_good_interval(interval) || !is_good_limit(limit)) {
            depth_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        int ret = depth_subscribe(ses, market, limit, interval, true);
        if (ret == -1) {
            depth_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        } else if (ret < 0) {
            depth_unsubscribe(ses);
            return ws_send_error_internal_error(ses, id);
        }
    }

    ws_send_success(ses, id);
    for (size_t i = 0; i < sub_size; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        int limit = json_integer_value(json_array_get(item, 1));
        const char *interval = json_string_value(json_array_get(item, 2));
        depth_send_clean(ses, market, limit, interval);
    }
    return 0;
}

static int on_method_depth_unsubscribe_full(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    depth_unsubscribe(ses);
    return ws_send_success(ses, id);
}

static int on_method_deals_query(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    direct_deals_reply(ses, params, id);
    return 0;
}

static int on_method_deals_subscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    size_t sub_size = json_array_size(params);
    if (sub_size == 0)
        return ws_send_error_invalid_argument(ses, id);

    deals_unsubscribe(ses);
    for (size_t i = 0; i < sub_size; ++i) {
        const char *market = json_string_value(json_array_get(params, i));
        if (market == NULL || strlen(market) > MARKET_NAME_MAX_LEN)
            return ws_send_error_invalid_argument(ses, id);
        if (deals_subscribe(ses, market) < 0)
            return ws_send_error_internal_error(ses, id);
    }

    ws_send_success(ses, id);
    for (size_t i = 0; i < sub_size; ++i) {
        deals_sub_send_full(ses, json_string_value(json_array_get(params, i)));
    }

    return 0;
}

static int on_method_deals_unsubscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    deals_unsubscribe(ses);
    return ws_send_success(ses, id);
}

static int on_method_deals_query_user(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (!rpc_clt_connected(readhistory))
        return ws_send_error_internal_error(ses, id);

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;

    rpc_request_json(readhistory, CMD_MARKET_USER_DEALS, entry->id, id, params);

    return 0;
}

static int on_method_deals_subscribe_user(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    deals_unsubscribe_user(ses);
    size_t sub_size = json_array_size(params);
    if (sub_size == 0)
        return ws_send_error_invalid_argument(ses, id);

    deals_unsubscribe_user(ses);
    for (size_t i = 0; i < sub_size; ++i) {
        json_t *item = json_array_get(params, i);
        size_t item_size = json_array_size(item);
        if (item_size <= 1) {
            deals_unsubscribe_user(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        uint32_t user_id = json_integer_value(json_array_get(item, 0));
        if (user_id <= 0) {
            deals_unsubscribe_user(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        for (size_t j = 1; j < item_size; ++j) {
            const char *market = json_string_value(json_array_get(item, j));
            if (market == NULL || strlen(market) > MARKET_NAME_MAX_LEN) {
                deals_unsubscribe_user(ses);
                return ws_send_error_invalid_argument(ses, id);
            }

            if (deals_subscribe_user(ses, market, user_id) < 0) {
                deals_unsubscribe_user(ses);
                return ws_send_error_internal_error(ses, id);
            }  
        }
    }
    return ws_send_success(ses, id);
}

static int on_method_deals_unsubscribe_user(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    deals_unsubscribe_user(ses);
    return ws_send_success(ses, id);
}

static int on_method_order_query(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (!rpc_clt_connected(matchengine))
        return ws_send_error_internal_error(ses, id);

    if (json_array_size(params) <= 0) {
        return ws_send_error_invalid_argument(ses, id);
    }

    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id <= 0)
        return ws_send_error_invalid_argument(ses, id);

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;

    rpc_request_json_unique(matchengine, CMD_ORDER_PENDING, entry->id, id, user_id, params);

    return 0;
}

static int on_method_order_query_stop(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (!rpc_clt_connected(matchengine))
        return ws_send_error_internal_error(ses, id);

    if (json_array_size(params) <= 0) {
        return ws_send_error_invalid_argument(ses, id);
    }

    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id <= 0)
        return ws_send_error_invalid_argument(ses, id);

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;

    rpc_request_json_unique(matchengine, CMD_ORDER_PENDING_STOP, entry->id, id, user_id, params);
    return 0;
}

static int on_method_order_subscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (json_array_size(params) == 0)
        return ws_send_error_invalid_argument(ses, id);

    order_unsubscribe(ses);
    size_t params_size = json_array_size(params);
    for (size_t i = 0; i < params_size; ++i) {
        json_t *item = json_array_get(params, i);
        size_t item_size = json_array_size(item);
        if (item_size <= 1) {
            order_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        uint32_t user_id = json_integer_value(json_array_get(item, 0));
        if (user_id <= 0) {
            order_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        for (size_t j = 1; j < item_size; ++j) {
            const char *market = json_string_value(json_array_get(item, j));
            if (market == NULL || strlen(market) > MARKET_NAME_MAX_LEN) {
                order_unsubscribe(ses);
                return ws_send_error_invalid_argument(ses, id);
            }

            if (order_subscribe(user_id, ses, market) < 0) {
                order_unsubscribe(ses);
                return ws_send_error_internal_error(ses, id);
            }  
        }
    }
    return ws_send_success(ses, id);
}

static int on_method_order_unsubscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    order_unsubscribe(ses);
    return ws_send_success(ses, id);
}

static int on_method_asset_query(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (!rpc_clt_connected(matchengine))
        return ws_send_error_internal_error(ses, id);

    if (json_array_size(params) < 1)
        return ws_send_error_invalid_argument(ses, id);

    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id <= 0)
        return ws_send_error_invalid_argument(ses, id);

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;

    rpc_request_json_unique(matchengine, CMD_ASSET_QUERY, entry->id, id, user_id, params);
    return 0;
}

static int on_method_asset_query_all(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (!rpc_clt_connected(matchengine))
        return ws_send_error_internal_error(ses, id);

     if (json_array_size(params) < 1)
        return ws_send_error_invalid_argument(ses, id);

    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id <= 0)
        return ws_send_error_invalid_argument(ses, id);

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;

    rpc_request_json_unique(matchengine, CMD_ASSET_QUERY_ALL, entry->id, id, user_id, params);
    return 0;
}

static int on_method_asset_subscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    size_t params_size = json_array_size(params);
    if (params_size == 0)
        return ws_send_error_invalid_argument(ses, id);

    asset_unsubscribe(ses);
    for (size_t i = 0; i < params_size; ++i) {
        json_t *item = json_array_get(params, i);
        size_t item_size = json_array_size(item);
        if (item_size <= 0) {
            asset_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        uint32_t user_id = json_integer_value(json_array_get(item, 0));
        if (user_id <= 0) {
            asset_unsubscribe(ses);
            return ws_send_error_invalid_argument(ses, id);
        }

        if (item_size == 1) {
            if (asset_subscribe(user_id, ses, "") < 0) {
                asset_unsubscribe(ses);
                return ws_send_error_internal_error(ses, id);
            }
        } else {
            for (size_t j = 1; j < item_size; ++j) {
                const char *asset = json_string_value(json_array_get(item, j));
                if (asset == NULL || strlen(asset) > ASSET_NAME_MAX_LEN) {
                    asset_unsubscribe(ses);
                    return ws_send_error_internal_error(ses, id);
                }

                if (asset_subscribe(user_id, ses, asset) < 0) {
                    asset_unsubscribe(ses);
                    return ws_send_error_internal_error(ses, id);
                }   
            }
        }
    }
    return ws_send_success(ses, id);
}

static int on_method_asset_unsubscribe(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    asset_unsubscribe(ses);
    return ws_send_success(ses, id);
}

static int on_message(nw_ses *ses, double timestamp, const char *remote, const char *url, void *message, size_t size)
{
    struct clt_info *info = ws_ses_privdata(ses);
    log_trace("new websocket message from: %"PRIu64":%s, url: %s, size: %zu", ses->id, remote, url, size);
    json_t *msg = json_loadb(message, size, 0, NULL);
    if (msg == NULL) {
        goto decode_error;
    }

    json_t *id = json_object_get(msg, "id");
    if (!id || !json_is_integer(id)) {
        goto decode_error;
    }
    json_t *method = json_object_get(msg, "method");
    if (!method || !json_is_string(method)) {
        goto decode_error;
    }
    json_t *params = json_object_get(msg, "params");
    if (!params || !json_is_array(params)) {
        goto decode_error;
    }

    sds _msg = sdsnewlen(message, size);
    log_trace("remote: %"PRIu64":%s message: %s", ses->id, remote, _msg);

    uint64_t _id = json_integer_value(id);
    const char *_method = json_string_value(method);
    dict_entry *entry = dict_find(method_map, _method);
    if (entry) {
        on_request_method handler = entry->val;
        int ret = handler(ses, _id, info, params);
        if (ret < 0) {
            log_error("remote: %"PRIu64":%s, request fail: %d, request: %s", ses->id, remote, ret, _msg);
        } else {
            profile_inc(json_string_value(method), 1);
        }
    } else {
        log_error("remote: %"PRIu64":%s, unknown method, request: %s", ses->id, remote, _msg);
        ws_send_error_unknown_method(ses, json_integer_value(id));
    }

    sdsfree(_msg);
    json_decref(msg);

    return 0;

decode_error:
    if (msg)
        json_decref(msg);
    sds hex = hexdump(message, size);
    log_error("remote: %"PRIu64":%s, decode request fail, request body: \n%s", ses->id, remote, hex);
    sdsfree(hex);
    return -__LINE__;
}

static void on_upgrade(nw_ses *ses, const char *remote)
{
    log_trace("remote: %"PRIu64":%s upgrade to websocket", ses->id, remote);
    struct clt_info *info = ws_ses_privdata(ses);
    memset(info, 0, sizeof(struct clt_info));
    info->remote = strdup(remote);
    profile_inc("connection_new", 1);
}

static void on_close(nw_ses *ses, const char *remote)
{
    log_trace("remote: %"PRIu64":%s websocket connection close", ses->id, remote);
    kline_unsubscribe(ses);
    depth_unsubscribe(ses);
    deals_unsubscribe(ses);
    order_unsubscribe(ses);
    asset_unsubscribe(ses);
    profile_inc("connection_close", 1);
}

static void *on_privdata_alloc(void *svr)
{
    return nw_cache_alloc(privdata_cache);
}

static void on_privdata_free(void *svr, void *privdata)
{
    struct clt_info *info = privdata;
    if (info->source)
        free(info->source);
    if (info->remote)
        free(info->remote);
    nw_cache_free(privdata_cache, privdata);
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

static void on_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
    struct state_data *state = entry->data;
    if (state->ses->id == state->ses_id) {
        ws_send_error_service_timeout(state->ses, state->request_id);
    }
}

static void on_release(nw_state_entry *entry)
{
    struct state_data *state = entry->data;
    if (state->cache_key)
        sdsfree(state->cache_key);
}

static int init_svr(void)
{
    ws_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_upgrade = on_upgrade;
    type.on_close = on_close;
    type.on_message = on_message;
    type.on_privdata_alloc = on_privdata_alloc;
    type.on_privdata_free = on_privdata_free;

    svr = ws_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;

    privdata_cache = nw_cache_create(sizeof(struct clt_info));
    if (privdata_cache == NULL)
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

    ERR_RET_LN(add_handler("server.ping",               on_method_server_ping));
    ERR_RET_LN(add_handler("server.time",               on_method_server_time));

    ERR_RET_LN(add_handler("kline.query",               on_method_kline_query));
    ERR_RET_LN(add_handler("kline.subscribe",           on_method_kline_subscribe));
    ERR_RET_LN(add_handler("kline.unsubscribe",         on_method_kline_unsubscribe));

    ERR_RET_LN(add_handler("depth.query",               on_method_depth_query));
    ERR_RET_LN(add_handler("depth.subscribe",           on_method_depth_subscribe));
    ERR_RET_LN(add_handler("depth.unsubscribe",         on_method_depth_unsubscribe));
    ERR_RET_LN(add_handler("depth.subscribe_full",      on_method_depth_subscribe_full));
    ERR_RET_LN(add_handler("depth.unsubscribe_full",    on_method_depth_unsubscribe_full));

    ERR_RET_LN(add_handler("deals.query",               on_method_deals_query));
    ERR_RET_LN(add_handler("deals.subscribe",           on_method_deals_subscribe));
    ERR_RET_LN(add_handler("deals.unsubscribe",         on_method_deals_unsubscribe));
    
    ERR_RET_LN(add_handler("deals.query_user",          on_method_deals_query_user));
    ERR_RET_LN(add_handler("deals.subscribe_user",      on_method_deals_subscribe_user));
    ERR_RET_LN(add_handler("deals.unsubscribe_user",    on_method_deals_unsubscribe_user));

    ERR_RET_LN(add_handler("order.query",               on_method_order_query));
    ERR_RET_LN(add_handler("order.query_stop",          on_method_order_query_stop));
    ERR_RET_LN(add_handler("order.subscribe",           on_method_order_subscribe));
    ERR_RET_LN(add_handler("order.unsubscribe",         on_method_order_unsubscribe));

    ERR_RET_LN(add_handler("asset.query",               on_method_asset_query));
    ERR_RET_LN(add_handler("asset.query_all",           on_method_asset_query_all));
    ERR_RET_LN(add_handler("asset.subscribe",           on_method_asset_subscribe));
    ERR_RET_LN(add_handler("asset.unsubscribe",         on_method_asset_unsubscribe));

    return 0;
}

static int ws_send_depth(struct state_data *state, rpc_pkg *pkg, sds message)
{
    int ret = 0;
    json_t *reply_json = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (!reply_json) {
        ret = ws_send_error_internal_error(state->ses, state->request_id);
        goto clean;
    }

    json_t *error = json_object_get(reply_json, "error");
    if (!error) {
        ret = ws_send_error_internal_error(state->ses, state->request_id);
        goto clean;
    }
    if (!json_is_null(error)) {
        ret = ws_send_text(state->ses, message);
        goto clean;
    }

    json_t *result = json_object_get(reply_json, "result");
    if (!result) {
        ret = ws_send_error_internal_error(state->ses, state->request_id);
        goto clean;
    }

    json_t *reply_depth = pack_depth_result(result, state->depth_limit);
    ret = ws_send_result(state->ses, state->request_id, reply_depth);
    json_decref(reply_depth);

clean:
    if (reply_json)
        json_decref(reply_json);
    return ret;
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
    if (entry == NULL) {
        return;
    }

    struct state_data *state = entry->data;
    if (state->ses->id == state->ses_id) {
        sds message = sdsnewlen(pkg->body, pkg->body_size);
        log_trace("send response to: %"PRIu64", size: %zu, message: %s", state->ses->id, sdslen(message), message);

        if (pkg->command == CMD_CACHE_DEPTH) {
            ws_send_depth(state, pkg, message);
        } else {
            ws_send_text(state->ses, message);
        }
        sdsfree(message);
        profile_inc("success", 1);
    }

    json_t *reply = NULL;
    if (state->cache_key) {
        reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
        if (reply == NULL)
            goto end;
        json_t *result = json_object_get(reply, "result");
        if (!result || json_is_null(result))
            goto end;
        uint64_t ttl = json_integer_value(json_object_get(reply, "ttl"));
        if (ttl == 0)
            goto end;

        struct cache_val val;
        val.time_cache = current_millisecond() + ttl;
        val.result = result;
        json_incref(result);
        dict_replace(backend_cache, state->cache_key, &val);
    }

end:
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
    if (obj->result)
        json_decref(obj->result);
    free(val);
}

static void on_timer(nw_timer *timer, void *privdata)
{
    double now = current_millisecond();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(backend_cache);
    while ((entry = dict_next(iter)) != NULL) {
        struct cache_val *val = entry->val;
        if (now >= val->time_cache) {
            dict_delete(backend_cache, entry->key);
        }
    }
    dict_release_iterator(iter);

    profile_set("connections", svr->raw_svr->clt_count);
    profile_set("subscribe_kline", kline_subscribe_number());
    profile_set("subscribe_depth", depth_subscribe_number());
    profile_set("subscribe_deals", deals_subscribe_number());
    profile_set("subscribe_order", order_subscribe_number());
    profile_set("subscribe_asset", asset_subscribe_number());
    profile_set("subscribe_depth_market_interval", depth_subscribe_market_interval());
    profile_set("subscribe_depth_market_interval_limit", depth_subscribe_market_interval_limit());
}

static int init_cache_backend(rpc_clt_type *ct)
{
    cachecenter_clt_arr = malloc(sizeof(void *) * settings.cachecenter_worker_num);
    for (int i = 0; i < settings.cachecenter_worker_num; ++i) {
        char clt_name[100];
        snprintf(clt_name, sizeof(clt_name), "cachecenter_%d", i);
        char clt_addr[100];
        snprintf(clt_addr, sizeof(clt_addr), "tcp@%s:%d", settings.cachecenter_host, settings.cachecenter_port + i);

        rpc_clt_cfg cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.name = clt_name;
        cfg.addr_count = 1;
        cfg.addr_arr = malloc(sizeof(nw_addr_t));
        if (nw_sock_cfg_parse(clt_addr, &cfg.addr_arr[0], &cfg.sock_type) < 0)
            return -__LINE__;
        cfg.max_pkg_size = 1024 * 1024;

        cachecenter_clt_arr[i] = rpc_clt_create(&cfg, ct);
        if (cachecenter_clt_arr[i] == NULL)
            return -__LINE__;
        if (rpc_clt_start(cachecenter_clt_arr[i]) < 0)
            return -__LINE__;
    }

    return 0;
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

    readhistory = rpc_clt_create(&settings.readhistory, &ct);
    if (readhistory == NULL)
        return -__LINE__;
    if (rpc_clt_start(readhistory) < 0)
        return -__LINE__;

    if (init_cache_backend(&ct) < 0)
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
    if (nw_sock_cfg_parse(AW_LISTENER_BIND, &addr, &cfg.sock_type) < 0)
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

