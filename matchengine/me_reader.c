/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */
# include "me_config.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_reader.h"
# include "me_reply.h"
# include "me_load.h"
# include "ut_queue.h"

static rpc_svr *svr;
static cli_svr *svrcli;
static dict_t *dict_cache;
static nw_timer cache_timer;
static int reader_id;
static queue_t queue_reader;

struct cache_val {
    double      time;
    json_t      *result;
};

static bool check_cache(nw_ses *ses, rpc_pkg *pkg, sds *cache_key)
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

    reply_result(ses, pkg, cache->result);
    sdsfree(key);
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
    json_decref(obj->result);
    free(val);
}

static void on_cache_timer(nw_timer *timer, void *privdata)
{
    dict_clear(dict_cache);
}

static int init_cache()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = cache_dict_hash_function;
    dt.key_compare    = cache_dict_key_compare;
    dt.key_dup        = cache_dict_key_dup;
    dt.key_destructor = cache_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;

    dict_cache = dict_create(&dt, 64);
    if (dict_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_timer, NULL);
    nw_timer_start(&cache_timer);
    return 0;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static int on_cmd_asset_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < settings.asset_num; ++i) {
        json_t *asset = json_object();
        json_object_set_new(asset, "name", json_string(settings.assets[i].name));
        json_object_set_new(asset, "prec", json_integer(settings.assets[i].prec_show));
        json_array_append_new(result, asset);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t request_size = json_array_size(params);
    if (request_size == 0)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id == 0)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    if (request_size == 1) {
        for (size_t i = 0; i < settings.asset_num; ++i) {
            const char *asset = settings.assets[i].name;
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_available(user_id, asset);
            if (prec_save != prec_show) {
                mpd_rescale(available, available, -prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "available", available);
            mpd_del(available);

            mpd_t *frozen = balance_frozen(user_id, asset);
            if (prec_save != prec_show) {
                mpd_rescale(frozen, frozen, -prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "frozen", frozen);
            mpd_del(frozen);

            json_object_set_new(result, asset, unit);
        }
    } else {
        for (size_t i = 1; i < request_size; ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (!asset || !asset_exist(asset)) {
                json_decref(result);
                return reply_error_invalid_argument(ses, pkg);
            }
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_available(user_id, asset);
            if (prec_save != prec_show) {
                mpd_rescale(available, available, -prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "available", available);
            mpd_del(available);

            mpd_t *frozen = balance_frozen(user_id, asset);
            if (prec_save != prec_show) {
                mpd_rescale(frozen, frozen, -prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "frozen", frozen);
            mpd_del(frozen);

            json_object_set_new(result, asset, unit);
        }
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query_lock(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t request_size = json_array_size(params);
    if (request_size == 0)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id == 0)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    if (request_size == 1) {
        for (size_t i = 0; i < settings.asset_num; ++i) {
            const char *asset = settings.assets[i].name;
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *lock = balance_lock(user_id, asset);
            if (prec_save != prec_show) {
                mpd_rescale(lock, lock, -prec_show, &mpd_ctx);
            }
            if (mpd_cmp(lock, mpd_zero, &mpd_ctx) > 0) {
                json_object_set_new_mpd(result, asset, lock);
            }
            mpd_del(lock);
        }
    } else {
        for (size_t i = 1; i < request_size; ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (!asset || !asset_exist(asset)) {
                json_decref(result);
                return reply_error_invalid_argument(ses, pkg);
            }
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *lock = balance_lock(user_id, asset);
            if (prec_save != prec_show) {
                mpd_rescale(lock, lock, -prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(result, asset, lock);
            mpd_del(lock);
        }
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_pending(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 5)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    market_t *market = NULL;
    if (json_is_string(json_array_get(params, 1))) {
        const char *market_name = json_string_value(json_array_get(params, 1));
        market = get_market(market_name);
        if (market == NULL)
            return reply_error_invalid_argument(ses, pkg);
    }

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 3));

    // limit
    if (!json_is_integer(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 4));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *orders = json_array();
    skiplist_t *order_list = market_get_order_list(market, user_id);
    if (order_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        size_t count = 0;
        size_t total = 0;
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(order_list);
        for (size_t i = 0; (node = skiplist_next(iter)) != NULL; i++) {
            order_t *order = node->value;
            if (side && order->side != side)
                continue;
            total += 1;
            if (i >= offset && count < limit) {
                count += 1;
                json_array_append_new(orders, get_order_info(order));
            }
        }
        skiplist_release_iterator(iter);
        json_object_set_new(result, "total", json_integer(total));
    }

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 1));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));

    uint64_t total;
    skiplist_iter *iter;
    if (side == MARKET_ORDER_SIDE_ASK) {
        iter = skiplist_get_iterator(market->asks);
        total = market->asks->len;
        json_object_set_new(result, "total", json_integer(total));
    } else {
        iter = skiplist_get_iterator(market->bids);
        total = market->bids->len;
        json_object_set_new(result, "total", json_integer(total));
    }

    json_t *orders = json_array();
    if (offset < total) {
        for (size_t i = 0; i < offset; i++) {
            if (skiplist_next(iter) == NULL)
                break;
        }
        size_t index = 0;
        skiplist_node *node;
        while ((node = skiplist_next(iter)) != NULL && index < limit) {
            index++;
            order_t *order = node->value;
            json_array_append_new(orders, get_order_info(order));
        }
    }
    skiplist_release_iterator(iter);

    json_object_set_new(result, "orders", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_stop_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 1));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));

    uint64_t total;
    skiplist_iter *iter;
    if (side == MARKET_ORDER_SIDE_ASK) {
        iter = skiplist_get_iterator(market->stop_asks);
        total = market->asks->len;
        json_object_set_new(result, "total", json_integer(total));
    } else {
        iter = skiplist_get_iterator(market->stop_bids);
        total = market->bids->len;
        json_object_set_new(result, "total", json_integer(total));
    }

    json_t *orders = json_array();
    if (offset < total) {
        for (size_t i = 0; i < offset; i++) {
            if (skiplist_next(iter) == NULL)
                break;
        }
        size_t index = 0;
        skiplist_node *node;
        while ((node = skiplist_next(iter)) != NULL && index < limit) {
            index++;
            stop_t *order = node->value;
            json_array_append_new(orders, get_stop_info(order));
        }
    }
    skiplist_release_iterator(iter);

    json_object_set_new(result, "orders", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static json_t *get_depth(market_t *market, size_t limit)
{
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    int count = 1;
    size_t index = 0;
    while (node && index < limit) {
        if (count > settings.depth_merge_max) {
            break;
        }
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    count = 1;
    index = 0;
    while (node && index < limit) {
        if (count > settings.depth_merge_max) {
            break;
        }
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);
    json_object_set_new_mpd(result, "last", market->last);
    json_object_set_new(result, "time", json_integer(current_millis()));


    return result;
}

static json_t *get_depth_merge(market_t* market, size_t limit, mpd_t *interval)
{
    mpd_t *q = mpd_new(&mpd_ctx);
    mpd_t *r = mpd_new(&mpd_ctx);
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    size_t count = 1;
    size_t index = 0;
    while (node && index < limit) {
        if (count > settings.depth_merge_max) {
            break;
        }
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        if (mpd_cmp(r, mpd_zero, &mpd_ctx) != 0) {
            mpd_add(price, price, interval, &mpd_ctx);
        }
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) >= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    count = 1;
    index = 0;
    while (node && index < limit) {
        if (count > settings.depth_merge_max) {
            break;
        }
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) <= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }

        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(q);
    mpd_del(r);
    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);
    json_object_set_new_mpd(result, "last", market->last);
    json_object_set_new(result, "time", json_integer(current_millis()));

    return result;
}

static int on_cmd_order_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // limit
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 1));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    // interval
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *interval = decimal(json_string_value(json_array_get(params, 2)), market->money_prec);
    if (!interval)
        return reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(interval);
        return reply_error_invalid_argument(ses, pkg);
    }

    sds cache_key = NULL;
    if (check_cache(ses, pkg, &cache_key)) {
        mpd_del(interval);
        return 0;
    }

    profile_inc("get_depth", 1);
    json_t *result = NULL;
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) == 0) {
        result = get_depth(market, limit);
    } else {
        result = get_depth_merge(market, limit, interval);
    }
    mpd_del(interval);

    if (result == NULL) {
        sdsfree(cache_key);
        return reply_error_internal_error(ses, pkg);
    }

    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 1));

    order_t *order = market_get_order(market, order_id);
    json_t *result = NULL;
    if (order == NULL) {
        result = json_null();
        result = market_get_fini_order(order_id);
        if (result == NULL)
            result = json_null();
    } else {
        result = get_order_info(order);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_pending_stop(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 5)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    market_t *market = NULL;
    if (json_is_string(json_array_get(params, 1))) {
        const char *market_name = json_string_value(json_array_get(params, 1));
        market = get_market(market_name);
        if (market == NULL)
            return reply_error_invalid_argument(ses, pkg);
    }

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 3));

    // limit
    if (!json_is_integer(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 4));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *stops = json_array();
    skiplist_t *stop_list = market_get_stop_list(market, user_id);
    if (stop_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        size_t count = 0;
        size_t total = 0;
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(stop_list);
        for (size_t i = 0; (node = skiplist_next(iter)) != NULL; i++) {
            stop_t *stop = node->value;
            if (side && stop->side != side)
                continue;
            total += 1;
            if (i >= offset && count < limit) {
                count += 1;
                json_array_append_new(stops, get_stop_info(stop));
            }
        }
        skiplist_release_iterator(iter);
        json_object_set_new(result, "total", json_integer(total));
    }

    json_object_set_new(result, "records", stops);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_market_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < settings.market_num; ++i) {
        json_t *market = json_object();
        json_object_set_new(market, "name", json_string(settings.markets[i].name));
        json_object_set_new(market, "stock", json_string(settings.markets[i].stock));
        json_object_set_new(market, "money", json_string(settings.markets[i].money));
        json_object_set_new(market, "fee_prec", json_integer(settings.markets[i].fee_prec));
        json_object_set_new(market, "stock_prec", json_integer(settings.markets[i].stock_prec));
        json_object_set_new(market, "money_prec", json_integer(settings.markets[i].money_prec));
        json_object_set_new_mpd(market, "min_amount", settings.markets[i].min_amount);
        json_array_append_new(result, market);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_update_asset_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret;
    ret = update_asset_config();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    ret = update_balance();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    log_info("update asset config success!");
    return reply_success(ses, pkg);
}

static int on_cmd_update_market_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret;
    ret = update_market_config();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    ret = update_trade();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    log_info("update market config success!");
    return reply_success(ses, pkg);
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_ASSET_LIST:
        profile_inc("cmd_asset_list", 1);
        ret = on_cmd_asset_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_QUERY:
        profile_inc("cmd_asset_query", 1);
        ret = on_cmd_asset_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query %s fail: %d", params_str, ret);
        }
    case CMD_ASSET_QUERY_LOCK:
        profile_inc("cmd_asset_query_lock", 1);
        ret = on_cmd_asset_query_lock(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_lock %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PENDING:
        profile_inc("cmd_order_pending", 1);
        ret = on_cmd_order_pending(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_pending %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_BOOK:
        profile_inc("cmd_order_book", 1);
        ret = on_cmd_order_book(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_DEPTH:
        profile_inc("cmd_order_depth", 1);
        ret = on_cmd_order_depth(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_depth %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PENDING_DETAIL:
        profile_inc("cmd_order_detail", 1);
        ret = on_cmd_order_detail(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_detail %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PENDING_STOP:
        profile_inc("cmd_order_pending_stop", 1);
        ret = on_cmd_pending_stop(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_pending_stop %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_STOP_BOOK:
        profile_inc("cmd_order_stop_book", 1);
        ret = on_cmd_stop_book(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_stop_book %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_LIST:
        profile_inc("cmd_market_list", 1);
        ret = on_cmd_market_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_CONFIG_UPDATE_ASSET:
        profile_inc("cmd_config_update_asset", 1);
        ret = on_cmd_update_asset_config(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_update_asset_config fail: %d", ret);
        }
        break;
    case CMD_CONFIG_UPDATE_MARKET:
        profile_inc("cmd_config_update_market", 1);
        ret = on_cmd_update_market_config(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_update_market_config fail: %d", ret);
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

static int init_server()
{
    if (settings.svr.bind_count != 1)
        return -__LINE__;
    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET)
        return -__LINE__;
    bind_arr->addr.in.sin_port = htons(ntohs(bind_arr->addr.in.sin_port) + reader_id + 2);

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

    return 0;
}

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    uint32_t mem_num = 0;
    uint32_t mem_size = 0;
    queue_stat(&queue_reader, &mem_num, &mem_size);
    reply = sdscatprintf(reply, "queue num: %u, size: %u\n", mem_num, mem_size);
    return reply;
}

static int init_cli()
{
    if (settings.cli.addr.family == AF_INET) {
        settings.cli.addr.in.sin_port = htons(ntohs(settings.cli.addr.in.sin_port) + reader_id + 2);
    } else if (settings.cli.addr.family == AF_INET6) {
        settings.cli.addr.in6.sin6_port = htons(ntohs(settings.cli.addr.in6.sin6_port) + reader_id + 2);
    }

    svrcli = cli_svr_create(&settings.cli);
    if (svrcli == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svrcli, "status", on_cmd_status);
    return 0;
}

static void on_message(void *data, uint32_t size)
{
    json_t *detail = json_loadb(data, size, 0, NULL);
    if (detail == NULL || !json_is_object(detail)) {
        log_error("read operlog error from queue: %d", reader_id);
        return;
    }
    
    char *detail_str = (char *)malloc(size + 1);
    memset(detail_str, 0, size + 1);
    memcpy(detail_str, data, size);
    log_info("read operlog %s from queue %d", detail_str, reader_id);

    int ret = load_oper(detail);
    if (ret < 0) {
        log_error("load operlog failed, ret: %d", ret);
    }
}

static int init_queue()
{
    queue_type type;
    memset(&type, 0, sizeof(type));
    type.on_message  = on_message;

    sds queue_name = sdsempty();
    queue_name = sdscatprintf(queue_name, "%s_%d", QUEUE_NAME, reader_id);

    sds queue_pipe_path = sdsempty();
    queue_pipe_path = sdscatprintf(queue_pipe_path, "%s_%d", QUEUE_PIPE_PATH, reader_id);

    key_t queue_shm_key = QUEUE_SHMKEY_START + reader_id;

    int ret = queue_reader_init(&queue_reader, &type, queue_name, queue_pipe_path, queue_shm_key, QUEUE_MEM_SIZE);

    sdsfree(queue_name);
    sdsfree(queue_pipe_path);

    if (ret < 0) {
        log_error("queue reader: %d fail ret: %d", reader_id, ret);
    }

    return ret;
}

int init_reader(int id)
{
    reader_id = id;

    int ret;
    ret = init_queue();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_cache();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_cli();
    if (ret < 0) {
        return -__LINE__;
    }
    
    ret = market_set_reader();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_server();
    if (ret < 0) {
        return -__LINE__;
    }
    
    return 0;
}
