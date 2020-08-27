/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */
# include "me_config.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_asset.h"
# include "me_reader.h"
# include "me_load.h"
# include "me_request.h"
# include "ut_queue.h"

static rpc_svr *svr;
static cli_svr *svrcli;
static dict_t *dict_cache;
static nw_timer cache_timer;
static int reader_id;
static queue_t queue_reader;

#define MAX_QUERY_ASSET_USER_NUM 2000

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

    rpc_reply_result(ses, pkg, cache->result);
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

static int rpc_push_error_reader_unavailable(nw_ses *ses, uint32_t command)
{
    profile_inc("error_reader_unavailable", 1);
    return rpc_push_error(ses, command, 1, "reader unavailable");
}

static int init_cache()
{
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
    json_t *result = get_asset_config();
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));
    if (!account_exist(account))
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = balance_query_list(user_id, account, params);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query_users(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 0));
    if (account == 0 || !account_exist(account))
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_array(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    json_t *users = json_array_get(params, 1);

    if (json_array_size(users) > MAX_QUERY_ASSET_USER_NUM)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = balance_query_users(account, users);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query_all(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    json_t *result = balance_query_all(user_id);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query_lock(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));
    if (!account_exist(account))
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = balance_query_lock_list(user_id, account, params);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *asset = json_string_value(json_array_get(params, 0));
    if (!asset) {
        return rpc_reply_error_invalid_argument(ses, pkg);
    }

    int account = -1;
    if (json_array_size(params) >= 2) {
        if(!json_is_integer(json_array_get(params, 1)))
            return rpc_reply_error_invalid_argument(ses, pkg);
        account = json_integer_value(json_array_get(params, 1));
        if(!asset_exist(account, asset))
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    json_t *result = balance_get_summary(asset, account);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_pending(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    int account = json_integer_value(json_array_get(params, 1));

    // market
    market_t *market = NULL;
    if (json_is_string(json_array_get(params, 2))) {
        const char *market_name = json_string_value(json_array_get(params, 2));
        market = get_market(market_name);
        if (market == NULL)
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 4)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 4));

    // limit
    if (!json_is_integer(json_array_get(params, 5)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 5));
    if (limit > ORDER_LIST_MAX_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *orders = json_array();
    skiplist_t *order_list = get_user_order_list(market, user_id, account);
    if (order_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        size_t count = 0;
        size_t total = 0;
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(order_list);
        while((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (side && order->side != side)
                continue;

            if (total >= offset && count < limit) {
                count += 1;
                json_array_append_new(orders, get_order_info(order, false));
            }
            total += 1;
        }
        skiplist_release_iterator(iter);
        json_object_set_new(result, "total", json_integer(total));
    }

    json_object_set_new(result, "records", orders);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 1));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_BOOK_MAX_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);

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
            json_array_append_new(orders, get_order_info(order, false));
        }
    }
    skiplist_release_iterator(iter);

    json_object_set_new(result, "orders", orders);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_stop_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // state
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t state = json_integer_value(json_array_get(params, 1));
    if (state != STOP_STATE_LOW && state != STOP_STATE_HIGH)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_BOOK_MAX_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));

    uint64_t total;
    skiplist_iter *iter;
    if (state == STOP_STATE_LOW) {
        iter = skiplist_get_iterator(market->stop_low);
        total = market->stop_low->len;
        json_object_set_new(result, "total", json_integer(total));
    } else {
        iter = skiplist_get_iterator(market->stop_high);
        total = market->stop_high->len;
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
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

inline static bool is_hidden_order(order_t *order)
{
    return (order->option & OPTION_HIDDEN) ? true : false;
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
        order_t *order = node->value;

        if (is_hidden_order(order)) {
            node = skiplist_next(iter);
            continue;
        }
        index++;

        mpd_copy(price, order->price, &mpd_ctx);
        if (market->call_auction && mpd_cmp(price, market->last, &mpd_ctx) < 0) {
            node = skiplist_next(iter);
            continue;
        }

        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;

            if (is_hidden_order(order)) {
                continue;
            }
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
        order_t *order = node->value;

        if (is_hidden_order(order)) {
            node = skiplist_next(iter);
            continue;
        }
        index++;

        mpd_copy(price, order->price, &mpd_ctx);
        if (market->call_auction && mpd_cmp(price, market->last, &mpd_ctx) > 0) {
            node = skiplist_next(iter);
            continue;
        }

        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;

            if (is_hidden_order(order)) {
                continue;
            }
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
    json_object_set_new(result, "time", json_integer(current_millisecond()));

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
        order_t *order = node->value;

        if (is_hidden_order(order)) {
            node = skiplist_next(iter);
            continue;
        }
        index++;

        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        if (mpd_cmp(r, mpd_zero, &mpd_ctx) != 0) {
            mpd_add(price, price, interval, &mpd_ctx);
        }

        if (market->call_auction && mpd_cmp(price, market->last, &mpd_ctx) < 0) {
            node = skiplist_next(iter);
            continue;
        }

        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;

            if (is_hidden_order(order)) {
                continue;
            }

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
        order_t *order = node->value;

        if (is_hidden_order(order)) {
            node = skiplist_next(iter);
            continue;
        }
        index++;

        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        if (market->call_auction && mpd_cmp(price, market->last, &mpd_ctx) > 0) {
            node = skiplist_next(iter);
            continue;
        }

        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            if (++count > settings.depth_merge_max) {
                break;
            }
            order = node->value;
    
            if (is_hidden_order(order)) {
                continue;
            }
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
    json_object_set_new(result, "time", json_integer(current_millisecond()));

    return result;
}

static int on_cmd_order_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // limit
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 1));
    if (limit > DEPTH_MAX_LIMIT)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // interval
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    mpd_t *interval = decimal(json_string_value(json_array_get(params, 2)), market->money_prec);
    if (!interval)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(interval);
        return rpc_reply_error_invalid_argument(ses, pkg);
    }

    //update_id
    if (!json_is_integer(json_array_get(params, 3))) {
        mpd_del(interval);
        return rpc_reply_error_invalid_argument(ses, pkg);
    }
    uint64_t update_id = json_integer_value(json_array_get(params, 3));

    sds cache_key = NULL;
    if (check_cache(ses, pkg, &cache_key)) {
        mpd_del(interval);
        return 0;
    }

    json_t *result = NULL;
    if (update_id > 0 && market->update_id == update_id) {
        result = json_object();
        json_object_set_new_mpd(result, "last", market->last);
        json_object_set_new(result, "update_id", json_integer(market->update_id));
        int ret = rpc_reply_result(ses, pkg, result);
        json_decref(result);
        mpd_del(interval);
        sdsfree(cache_key);
        return ret;
    }

    profile_inc("get_depth", 1);
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) == 0) {
        result = get_depth(market, limit);
    } else {
        result = get_depth_merge(market, limit, interval);
    }
    mpd_del(interval);
    
    if (result == NULL) {
        sdsfree(cache_key);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    json_object_set_new(result, "update_id", json_integer(market->update_id));
    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 1));

    json_t *result = NULL;
    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        result = market_get_fini_order(order_id);
        if (result == NULL)
            result = json_null();
    } else {
        result = get_order_info(order, false);
    }

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_pending_stop(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    int account = json_integer_value(json_array_get(params, 1));

    // market
    market_t *market = NULL;
    if (json_is_string(json_array_get(params, 2))) {
        const char *market_name = json_string_value(json_array_get(params, 2));
        market = get_market(market_name);
        if (market == NULL)
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 4)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 4));

    // limit
    if (!json_is_integer(json_array_get(params, 5)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 5));
    if (limit > ORDER_LIST_MAX_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *stops = json_array();
    skiplist_t *stop_list = get_user_stop_list(market, user_id, account);
    if (stop_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        size_t count = 0;
        size_t total = 0;
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(stop_list);
        while((node = skiplist_next(iter)) != NULL) {
            stop_t *stop = node->value;
            if (side && stop->side != side)
                continue;

            if (total >= offset && count < limit) {
                count += 1;
                json_array_append_new(stops, get_stop_info(stop));
            }
            total += 1;
        }
        skiplist_release_iterator(iter);
        json_object_set_new(result, "total", json_integer(total));
    }

    json_object_set_new(result, "records", stops);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_market_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = get_market_config();
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_market_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market_name = json_string_value(json_array_get(params, 0));
    json_t *result = get_market_detail(market_name);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_market_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = market_get_summary(market);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_asset_config_callback(json_t *reply, nw_ses *ses, rpc_pkg *pkg)
{
    if (!reply) {
        log_info("update asset config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    if (settings.asset_cfg)
        json_decref(settings.asset_cfg);
    settings.asset_cfg = reply;

    int ret = update_asset();
    if (ret < 0) {
        log_info("update asset config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    log_info("update asset config success");
    return rpc_reply_success(ses, pkg);
}

static int on_market_config_callback(json_t *reply, nw_ses *ses, rpc_pkg *pkg)
{
    if (!reply) {
        log_info("update market config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    if (settings.market_cfg)
        json_decref(settings.market_cfg);
    settings.market_cfg = reply;

    int ret = update_trade();
    if (ret < 0) {
        log_info("update market config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    log_info("update market config success");
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_update_asset_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret = update_assert_config(ses, pkg, on_asset_config_callback);
    if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }
    return 0;
}

static int on_cmd_update_market_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret = update_market_config(ses, pkg, on_market_config_callback);
    if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }
    return 0;
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
        break;
    case CMD_ASSET_QUERY_USERS:
        profile_inc("cmd_asset_query_users", 1);
        ret = on_cmd_asset_query_users(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_users %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_QUERY_ALL:
        profile_inc("cmd_asset_query_all", 1);
        ret = on_cmd_asset_query_all(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_all %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_QUERY_LOCK:
        profile_inc("cmd_asset_query_lock", 1);
        ret = on_cmd_asset_query_lock(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_lock %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_SUMMARY:
        profile_inc("cmd_asset_summary", 1);
        ret = on_cmd_asset_summary(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_summary %s fail: %d", params_str, ret);
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
    case CMD_MARKET_DETAIL:
        profile_inc("cmd_market_detail", 1);
        ret = on_cmd_market_detail(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_detail %s faile: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_SUMMARY:
        profile_inc("cmd_market_summary", 1);
        ret = on_cmd_market_summary(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_summary %s fail: %d", params_str, ret);
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

static sds queue_status(sds reply)
{
    uint32_t mem_num = 0;
    uint32_t mem_size = 0;
    queue_stat(&queue_reader, &mem_num, &mem_size);
    reply = sdscatprintf(reply, "queue used num: %u, used size: %u\n", mem_num, mem_size);
    return reply;
}

static sds on_cmd_unfreeze(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    if (argc != 4) {
        return sdscatprintf(reply, "usage: %s user_id account asset amount\n", cmd);
    }

    uint32_t user_id = strtoul(argv[0], NULL, 0);
    uint32_t account = strtoul(argv[1], NULL, 0);
    const char *asset = argv[2];
    int prec = asset_prec_show(account, asset);
    if (prec < 0) {
        return sdscatprintf(reply, "failed, asset not exist\n");
    }

    mpd_t *amount = decimal(argv[3], prec);
    if (!amount) {
        return sdscatprintf(reply, "failed, amount invalid\n");
    }

    mpd_t *frozen = balance_unfreeze(user_id, account, BALANCE_TYPE_FROZEN, asset, amount);
    if (!frozen) {
        mpd_del(amount);
        return sdscatprintf(reply, "unfreeze failed, user_id: %d\n", user_id);
    }

    mpd_del(amount);
    return sdscatprintf(reply, "unfreeze success, user_id: %d\n", user_id);
}

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = queue_status(reply);
    reply = market_status(reply);
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
    cli_svr_add_cmd(svrcli, "unfreeze", on_cmd_unfreeze);
    return 0;
}

static int send_reader_error()
{
    nw_ses *curr = svr->raw_svr->clt_list_head;
    while (curr) {
        rpc_push_error_reader_unavailable(curr, CMD_REDER_ERROR);
        curr = curr->next;
    }
    return 0;
}

static void on_message(void *data, uint32_t size)
{
    json_t *detail = json_loadb(data, size, 0, NULL);
    if (detail == NULL || !json_is_object(detail)) {
        log_fatal("read operlog error from queue: %d", reader_id);
        send_reader_error();
        return;
    }
    
    char *detail_str = (char *)malloc(size + 1);
    memset(detail_str, 0, size + 1);
    memcpy(detail_str, data, size);
    
    log_trace("read operlog %s from queue %d", detail_str, reader_id);

    int ret = load_oper(detail);
    if (ret < 0) {
        log_fatal("load operlog failed, ret: %d, data: %s, queue: %d", ret, detail_str, reader_id);
        send_reader_error();
    }
    
    free(detail_str);
    json_decref(detail);
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
    
    ret = init_request();
    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}

