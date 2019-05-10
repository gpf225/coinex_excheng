/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"
# include "ut_comm_dict.h"

static dict_t *dict_user_orders;
static dict_t *dict_user_stops;
static dict_t *dict_fini_orders;

uint64_t order_id_start;
uint64_t deals_id_start;

static nw_timer timer_status;
static nw_timer timer_fini_order;
static bool is_reader;

struct dict_user_key {
    uint32_t    user_id;
};

struct dict_order_key {
    uint64_t    order_id;
};

static uint32_t dict_user_hash_function(const void *key)
{
    const struct dict_user_key *obj = key;
    return obj->user_id;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    const struct dict_user_key *obj1 = key1;
    const struct dict_user_key *obj2 = key2;
    if (obj1->user_id == obj2->user_id) {
        return 0;
    }
    return 1;
}

static void *dict_user_key_dup(const void *key)
{
    struct dict_user_key *obj = malloc(sizeof(struct dict_user_key));
    memcpy(obj, key, sizeof(struct dict_user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void dict_user_val_free(void *key)
{
    skiplist_release(key);
}

static uint32_t dict_order_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_order_key));
}

static int dict_order_key_compare(const void *key1, const void *key2)
{
    const struct dict_order_key *obj1 = key1;
    const struct dict_order_key *obj2 = key2;
    if (obj1->order_id == obj2->order_id) {
        return 0;
    }
    return 1;
}

static void *dict_order_key_dup(const void *key)
{
    struct dict_order_key *obj = malloc(sizeof(struct dict_order_key));
    memcpy(obj, key, sizeof(struct dict_order_key));
    return obj;
}

static void dict_order_key_free(void *key)
{
    free(key);
}

static void dict_order_value_free(void *value)
{
    json_decref((json_t *)value);
}

static int order_match_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp;
    if (order1->side == MARKET_ORDER_SIDE_ASK) {
        cmp = mpd_cmp(order1->price, order2->price, &mpd_ctx);
    } else {
        cmp = mpd_cmp(order2->price, order1->price, &mpd_ctx);
    }
    if (cmp != 0) {
        return cmp;
    }

    return order1->id > order2->id ? 1 : -1;
}

static int order_id_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    return order1->id > order2->id ? -1 : 1;
}

static int stop_match_compare(const void *value1, const void *value2)
{
    const stop_t *stop1 = value1;
    const stop_t *stop2 = value2;

    if (stop1->id == stop2->id) {
        return 0;
    }

    int cmp;
    if (stop1->side == MARKET_ORDER_SIDE_ASK) {
        cmp = mpd_cmp(stop2->stop_price, stop1->stop_price, &mpd_ctx);
    } else {
        cmp = mpd_cmp(stop1->stop_price, stop2->stop_price, &mpd_ctx);
    }

    if (cmp != 0) {
        return cmp;
    }

    return stop1->id > stop2->id ? 1 : -1;
}

static int stop_id_compare(const void *value1, const void *value2)
{
    const stop_t *stop1 = value1;
    const stop_t *stop2 = value2;

    if (stop1->id == stop2->id) {
        return 0;
    }

    return stop1->id > stop2->id ? -1 : 1;
}

static void order_free(order_t *order)
{
    mpd_del(order->price);
    mpd_del(order->amount);
    mpd_del(order->taker_fee);
    mpd_del(order->maker_fee);
    mpd_del(order->left);
    mpd_del(order->frozen);
    mpd_del(order->deal_stock);
    mpd_del(order->deal_money);
    mpd_del(order->deal_fee);
    mpd_del(order->asset_fee);
    mpd_del(order->fee_discount);

    free(order->market);
    free(order->source);
    if (order->fee_asset)
        free(order->fee_asset);

    free(order);
}

static void stop_free(stop_t *stop)
{
    mpd_del(stop->fee_discount);
    mpd_del(stop->stop_price);
    mpd_del(stop->price);
    mpd_del(stop->amount);
    mpd_del(stop->taker_fee);
    mpd_del(stop->maker_fee);
    free(stop->market);
    free(stop->source);
    free(stop->fee_asset);

    free(stop);
}

json_t *get_order_info(order_t *order)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "side", json_integer(order->side));
    json_object_set_new(info, "user", json_integer(order->user_id));
    json_object_set_new(info, "ctime", json_real(order->create_time));
    json_object_set_new(info, "mtime", json_real(order->update_time));
    json_object_set_new(info, "market", json_string(order->market));
    json_object_set_new(info, "source", json_string(order->source));

    json_object_set_new_mpd(info, "price", order->price);
    json_object_set_new_mpd(info, "amount", order->amount);
    json_object_set_new_mpd(info, "taker_fee", order->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", order->maker_fee);
    json_object_set_new_mpd(info, "left", order->left);
    json_object_set_new_mpd(info, "deal_stock", order->deal_stock);
    json_object_set_new_mpd(info, "deal_money", order->deal_money);
    json_object_set_new_mpd(info, "deal_fee", order->deal_fee);
    json_object_set_new_mpd(info, "asset_fee", order->asset_fee);
    json_object_set_new_mpd(info, "fee_discount", order->fee_discount);

    if (order->fee_asset) {
        json_object_set_new(info, "fee_asset", json_string(order->fee_asset));
    } else {
        json_object_set_new(info, "fee_asset", json_null());
    }

    return info;
}

json_t *get_stop_info(stop_t *stop)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(stop->id));
    json_object_set_new(info, "type", json_integer(stop->type));
    json_object_set_new(info, "side", json_integer(stop->side));
    json_object_set_new(info, "user", json_integer(stop->user_id));
    json_object_set_new(info, "ctime", json_real(stop->create_time));
    json_object_set_new(info, "mtime", json_real(stop->update_time));
    json_object_set_new(info, "market", json_string(stop->market));
    json_object_set_new(info, "source", json_string(stop->source));
    json_object_set_new(info, "fee_asset", json_string(stop->fee_asset));

    json_object_set_new_mpd(info, "stop_price", stop->stop_price);
    json_object_set_new_mpd(info, "price", stop->price);
    json_object_set_new_mpd(info, "amount", stop->amount);
    json_object_set_new_mpd(info, "taker_fee", stop->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", stop->maker_fee);
    json_object_set_new_mpd(info, "fee_discount", stop->fee_discount);

    return info;
}

static int append_fini_order(order_t *order)
{
    json_t *order_info = get_order_info(order);
    json_object_set_new(order_info, "finished", json_true());
    struct dict_order_key order_key = { .order_id = order->id };
    dict_add(dict_fini_orders, &order_key, order_info);
    return 0;
}

static int put_order(market_t *m, order_t *order)
{
    if (order->type != MARKET_ORDER_TYPE_LIMIT)
        return -__LINE__;

    struct dict_order_key order_key = { .order_id = order->id };
    if (dict_add(m->orders, &order_key, order) == NULL)
        return -__LINE__;

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->user_orders, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(m->user_orders, &user_key, order_list) == NULL)
            return -__LINE__;
    }

    entry = dict_find(dict_user_orders, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(dict_user_orders, &user_key, order_list) == NULL)
            return -__LINE__;
    }

    if (order->side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_insert(m->asks, order) == NULL)
            return -__LINE__;
        mpd_copy(order->frozen, order->left, &mpd_ctx);
        if (balance_freeze(order->user_id, BALANCE_TYPE_FROZEN, m->stock, order->left) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->bids, order) == NULL)
            return -__LINE__;
        mpd_t *result = mpd_new(&mpd_ctx);
        mpd_mul(result, order->price, order->left, &mpd_ctx);
        mpd_copy(order->frozen, result, &mpd_ctx);
        if (balance_freeze(order->user_id, BALANCE_TYPE_FROZEN, m->money, result) == NULL) {
            mpd_del(result);
            return -__LINE__;
        }
        mpd_del(result);
    }

    return 0;
}

static int finish_order(bool real, market_t *m, order_t *order)
{
    if (order->side == MARKET_ORDER_SIDE_ASK) {
        skiplist_node *node = skiplist_find(m->asks, order);
        if (node) {
            skiplist_delete(m->asks, node);
        }
        if (mpd_cmp(order->frozen, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, BALANCE_TYPE_FROZEN, m->stock, order->frozen) == NULL) {
                return -__LINE__;
            }
            balance_reset(order->user_id, m->stock);
        }
    } else {
        skiplist_node *node = skiplist_find(m->bids, order);
        if (node) {
            skiplist_delete(m->bids, node);
        }
        if (mpd_cmp(order->frozen, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, BALANCE_TYPE_FROZEN, m->money, order->frozen) == NULL) {
                return -__LINE__;
            }
            balance_reset(order->user_id, m->money);
        }
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->orders, &order_key);

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->user_orders, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
        if (skiplist_len(order_list) == 0) {
            dict_delete(m->user_orders, &user_key);
        }
    }

    entry = dict_find(dict_user_orders, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
        if (skiplist_len(order_list) == 0) {
            dict_delete(dict_user_orders, &user_key);
        }
    }

    if (mpd_cmp(order->deal_stock, mpd_zero, &mpd_ctx) > 0) {
        if (real) {
            int ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
        } else if (is_reader) {
            append_fini_order(order);
        }
    }

    order_free(order);
    if (real) {
        profile_inc("finish_order", 1);
    }

    return 0;
}

static int put_stop(market_t *m, stop_t *stop)
{
    struct dict_order_key order_key = { .order_id = stop->id };
    if (dict_add(m->stops, &order_key, stop) == NULL)
        return -__LINE__;

    struct dict_user_key user_key = { .user_id = stop->user_id };
    dict_entry *entry = dict_find(m->user_stops, &user_key);
    if (entry) {
        skiplist_t *stop_list = entry->val;
        if (skiplist_insert(stop_list, stop) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = stop_id_compare;
        skiplist_t *stop_list = skiplist_create(&type);
        if (stop_list == NULL)
            return -__LINE__;
        if (skiplist_insert(stop_list, stop) == NULL)
            return -__LINE__;
        if (dict_add(m->user_stops, &user_key, stop_list) == NULL)
            return -__LINE__;
    }

    entry = dict_find(dict_user_stops, &user_key);
    if (entry) {
        skiplist_t *stop_list = entry->val;
        if (skiplist_insert(stop_list, stop) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = stop_id_compare;
        skiplist_t *stop_list = skiplist_create(&type);
        if (stop_list == NULL)
            return -__LINE__;
        if (skiplist_insert(stop_list, stop) == NULL)
            return -__LINE__;
        if (dict_add(dict_user_stops, &user_key, stop_list) == NULL)
            return -__LINE__;
    }

    if (stop->side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_insert(m->stop_asks, stop) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->stop_bids, stop) == NULL)
            return -__LINE__;
    }

    return 0;
}

static int finish_stop(bool real, market_t *m, stop_t *stop, int status)
{
    if (stop->side == MARKET_ORDER_SIDE_ASK) {
        skiplist_node *node = skiplist_find(m->stop_asks, stop);
        if (node) {
            skiplist_delete(m->stop_asks, node);
        }
    } else {
        skiplist_node *node = skiplist_find(m->stop_bids, stop);
        if (node) {
            skiplist_delete(m->stop_bids, node);
        }
    }

    struct dict_order_key order_key = { .order_id = stop->id };
    dict_delete(m->stops, &order_key);

    struct dict_user_key user_key = { .user_id = stop->user_id };
    dict_entry *entry = dict_find(m->user_stops, &user_key);
    if (entry) {
        skiplist_t *stop_list = entry->val;
        skiplist_node *node = skiplist_find(stop_list, stop);
        if (node) {
            skiplist_delete(stop_list, node);
        }
        if (skiplist_len(stop_list) == 0) {
            dict_delete(m->user_stops, &user_key);
        }
    }

    entry = dict_find(dict_user_stops, &user_key);
    if (entry) {
        skiplist_t *stop_list = entry->val;
        skiplist_node *node = skiplist_find(stop_list, stop);
        if (node) {
            skiplist_delete(stop_list, node);
        }
        if (skiplist_len(stop_list) == 0) {
            dict_delete(dict_user_stops, &user_key);
        }
    }

    if (real) {
        stop->update_time = current_timestamp();
        if (MARKET_STOP_STATUS_CANCEL != status) {
            int ret = append_stop_history(stop, status);
            if (ret < 0) {
                log_fatal("append_stop_history fail: %d, order: %"PRIu64"", ret, stop->id);
            }
        }
    }

    stop_free(stop);
    if (real) {
        profile_inc("finish_stop", 1);
    }

    return 0;
}

static void status_report(void)
{
    profile_set("pending_users", dict_size(dict_user_orders));
}

static void on_timer_status(nw_timer *timer, void *privdata)
{
    status_report();
}

static void on_timer_fini_order(nw_timer *timer, void *privdata)
{
    double now = current_timestamp();
    dict_iterator *iter = dict_get_iterator(dict_fini_orders);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        json_t *order = entry->val;
        double update_time = json_real_value(json_object_get(order, "mtime"));
        if (now - update_time > settings.order_fini_keeptime) {
            dict_delete(dict_fini_orders, entry->key);
        }
    }
    dict_release_iterator(iter);
}

int init_market(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_user_hash_function;
    dt.key_compare      = dict_user_key_compare;
    dt.key_dup          = dict_user_key_dup;
    dt.key_destructor   = dict_user_key_free;
    dt.val_destructor   = dict_user_val_free;

    dict_user_orders = dict_create(&dt, 1024);
    if (dict_user_orders == NULL)
        return -__LINE__;

    dict_user_stops = dict_create(&dt, 1024);
    if (dict_user_stops == NULL)
        return -__LINE__;

    is_reader = false;
    nw_timer_set(&timer_status, 60, true, on_timer_status, NULL);
    nw_timer_start(&timer_status);

    return 0;
}

market_t *market_create(struct market *conf)
{
    if (!asset_exist(conf->stock) || !asset_exist(conf->money))
        return NULL;
    if (conf->stock_prec + conf->money_prec > asset_prec(conf->money))
        return NULL;
    if (conf->stock_prec + conf->fee_prec > asset_prec(conf->stock))
        return NULL;
    if (conf->money_prec + conf->fee_prec > asset_prec(conf->money))
        return NULL;

    market_t *m = malloc(sizeof(market_t));
    memset(m, 0, sizeof(market_t));
    m->name             = strdup(conf->name);
    m->stock            = strdup(conf->stock);
    m->money            = strdup(conf->money);
    m->stock_prec       = conf->stock_prec;
    m->money_prec       = conf->money_prec;
    m->fee_prec         = conf->fee_prec;
    m->min_amount       = mpd_qncopy(conf->min_amount);
    m->last             = mpd_qncopy(mpd_zero);

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_user_hash_function;
    dt.key_compare      = dict_user_key_compare;
    dt.key_dup          = dict_user_key_dup;
    dt.key_destructor   = dict_user_key_free;
    dt.val_destructor   = dict_user_val_free;

    m->user_orders = dict_create(&dt, 1024);
    if (m->user_orders == NULL)
        return NULL;
    m->user_stops = dict_create(&dt, 1024);
    if (m->user_stops == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_order_hash_function;
    dt.key_compare      = dict_order_key_compare;
    dt.key_dup          = dict_order_key_dup;
    dt.key_destructor   = dict_order_key_free;

    m->orders = dict_create(&dt, 1024);
    if (m->orders == NULL)
        return NULL;
    m->stops = dict_create(&dt, 1024);
    if (m->stops == NULL)
        return NULL;

    skiplist_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare = order_match_compare;
    m->asks = skiplist_create(&lt);
    m->bids = skiplist_create(&lt);
    if (m->asks == NULL || m->bids == NULL)
        return NULL;

    memset(&lt, 0, sizeof(lt));
    lt.compare = stop_match_compare;
    m->stop_asks = skiplist_create(&lt);
    m->stop_bids = skiplist_create(&lt);
    if (m->stop_asks == NULL || m->stop_bids == NULL)
        return NULL;

    return m;
}

int market_update(market_t *m, struct market *conf)
{
    mpd_copy(m->min_amount, conf->min_amount, &mpd_ctx); 
    return 0;
}

static int append_balance_trade_add(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", change, detail_str);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int append_balance_trade_sub(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int append_balance_trade_fee(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount, mpd_t *fee_rate)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    json_object_set_new_mpd(detail, "f", fee_rate);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int active_stop_limit(bool real, market_t *m, stop_t *stop)
{
    int status = MARKET_STOP_STATUS_ACTIVE;
    int ret = market_put_limit_order(real, NULL, m, stop->user_id, stop->side, stop->amount, stop->price,
                stop->taker_fee, stop->maker_fee, stop->source, stop->fee_asset, stop->fee_discount);
    if (ret < 0) {
        status = MARKET_STOP_STATUS_FAIL;
    }

    if (real) {
        push_stop_message(STOP_EVENT_ACTIVE, stop, m, status);
    }

    return finish_stop(real, m, stop, status);
}

static int active_stop_market(bool real, market_t *m, stop_t *stop)
{
    int status = MARKET_STOP_STATUS_ACTIVE;
    int ret = market_put_market_order(real, NULL, m, stop->user_id, stop->side, stop->amount,
                stop->taker_fee, stop->source, stop->fee_asset, stop->fee_discount);
    if (ret < 0) {
        status = MARKET_STOP_STATUS_FAIL;
    }

    if (real) {
        push_stop_message(STOP_EVENT_ACTIVE, stop, m, status);
    }

    return finish_stop(real, m, stop, status);
}

static int active_stop(bool real, market_t *m, stop_t *stop)
{
    if (stop->type == MARKET_ORDER_TYPE_LIMIT) {
        return active_stop_limit(real, m, stop);
    } else {
        return active_stop_market(real, m, stop);
    }
}

static int check_stop_asks(bool real, market_t *m)
{
    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->stop_asks);
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;    
        if (mpd_cmp(stop->stop_price, m->last, &mpd_ctx) >= 0) {
            skiplist_delete(m->stop_asks, node);
            active_stop(real, m, stop);
            
            skiplist_reset_iterator(m->stop_asks, iter);
        } else {
            break;
        }
    }
    skiplist_release_iterator(iter);

    return 0;
}

static int check_stop_bids(bool real, market_t *m)
{
    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->stop_bids);
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;
        if (mpd_cmp(stop->stop_price, m->last, &mpd_ctx) <= 0) {
            skiplist_delete(m->stop_bids, node);
            active_stop(real, m, stop);

            skiplist_reset_iterator(m->stop_bids, iter);
        } else {
            break;
        }
    }
    skiplist_release_iterator(iter);

    return 0;
}

static int execute_limit_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        ask_fee_asset = NULL;
        bid_fee_asset = NULL;

        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        // calculate deal price
        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) > 0) {
            break;
        }
        mpd_copy(price, maker->price, &mpd_ctx);

        // calculate deal amount
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (strcmp(taker->fee_asset, m->stock) == 0) {
                mpd_add(require, require, taker->left, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                ask_fee_asset = taker->fee_asset;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }
        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        }

        // calculate bid fee
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                bid_fee_asset = maker->fee_asset;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
        }
        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);
        }

        mpd_rescale(ask_fee, ask_fee, -asset_prec(ask_fee_asset), &mpd_ctx);
        mpd_rescale(bid_fee, bid_fee, -asset_prec(bid_fee_asset), &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER,
                    price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
            push_deal_message(taker->update_time, deal_id, m, MARKET_TRADE_SIDE_SELL,
                    taker, maker, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        }

        // update taker
        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == taker->fee_asset) {
            mpd_add(taker->asset_fee, taker->asset_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, ask_fee_asset, ask_fee, price, amount, taker->taker_fee);
            }
        }

        // update maker
        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (bid_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, BALANCE_TYPE_FROZEN, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, bid_fee_asset, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            finish_order(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }

        mpd_copy(m->last, price, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

static int execute_limit_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        ask_fee_asset = NULL;
        bid_fee_asset = NULL;

        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        // calculate deal price
        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) < 0) {
            break;
        }
        mpd_copy(price, maker->price, &mpd_ctx);

        // calculate deal amount
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                ask_fee_asset = maker->fee_asset;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
        }
        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        }

        // calculate bid fee
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (strcmp(taker->fee_asset, m->money) == 0) {
                mpd_mul(require, taker->left, taker->price, &mpd_ctx);
                mpd_add(require, require, result, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                bid_fee_asset = taker->fee_asset;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }
        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);
        }

        mpd_rescale(ask_fee, ask_fee, -asset_prec(ask_fee_asset), &mpd_ctx);
        mpd_rescale(bid_fee, bid_fee, -asset_prec(bid_fee_asset), &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER,
                    price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
            push_deal_message(taker->update_time, deal_id, m, MARKET_TRADE_SIDE_BUY,
                    maker, taker, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        }

        // update taker
        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        if (bid_fee_asset == taker->fee_asset) {
            mpd_add(taker->asset_fee, taker->asset_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, bid_fee_asset, bid_fee, price, amount, taker->taker_fee);
            }
        }

        // update maker
        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, BALANCE_TYPE_FROZEN, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, ask_fee_asset, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            finish_order(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }

        mpd_copy(m->last, price, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

static bool check_fee_asset(mpd_t *trade_amount, mpd_t *balance, mpd_t *taker_fee, const char *fee_asset, mpd_t *fee_discount)
{
    mpd_t *fee_amount = mpd_new(&mpd_ctx);
    mpd_t *multiplier = mpd_new(&mpd_ctx);
    
    mpd_set_string(multiplier, "1.1", &mpd_ctx);

    mpd_mul(fee_amount, trade_amount, taker_fee, &mpd_ctx);
    if (fee_discount != NULL) {
        mpd_mul(fee_amount, fee_amount, fee_discount, &mpd_ctx);
    }
    mpd_mul(fee_amount, fee_amount, multiplier, &mpd_ctx);

    mpd_t *total_amount = mpd_new(&mpd_ctx);
    mpd_add(total_amount, trade_amount, fee_amount, &mpd_ctx);
    
    int ret = mpd_cmp(balance, total_amount, &mpd_ctx);
    
    mpd_del(fee_amount);
    mpd_del(multiplier);
    mpd_del(total_amount);

    return ret > 0;
}

int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount,
        mpd_t *price, mpd_t *taker_fee, mpd_t *maker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, amount, price, &mpd_ctx);
        if (!balance || mpd_cmp(balance, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -1;
        }

        if ((fee_asset != NULL) && (strcmp(m->money, fee_asset) == 0) ) {
            if (!check_fee_asset(require, balance, taker_fee, fee_asset, fee_discount)) {
                fee_asset = NULL;
            }
        }

        mpd_del(require);
    }

    if (real && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
        return -2;
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL)
        return -__LINE__;
    memset(order, 0, sizeof(order_t));

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->frozen       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);
    order->asset_fee    = mpd_new(&mpd_ctx);
    order->fee_discount = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, maker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->frozen, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->asset_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee_discount, mpd_zero, &mpd_ctx);

    if (fee_asset && strlen(fee_asset) > 0) {
        order->fee_asset = strdup(fee_asset);
        order->fee_price = get_fee_price(m, fee_asset);
        if (order->fee_price == NULL)
            return -__LINE__;
        if (fee_discount) {
            mpd_copy(order->fee_discount, fee_discount, &mpd_ctx);
        } else {
            mpd_copy(order->fee_discount, mpd_one, &mpd_ctx);
        }
    }

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_limit_ask_order(real, m, order);
        balance_reset(user_id, m->stock);
    } else {
        ret = execute_limit_bid_order(real, m, order);
        balance_reset(user_id, m->money);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (mpd_cmp(order->left, mpd_zero, &mpd_ctx) == 0) {
        if (real) {
            ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
            push_order_message(ORDER_EVENT_FILL, order, m);
            push_order_message(ORDER_EVENT_FINISH, order, m);
            if (result) {
                *result = get_order_info(order);
            }
        } else if (is_reader) {
            append_fini_order(order);
        }
        order_free(order);
    } else {
        if (real) {
            push_order_message(ORDER_EVENT_PUT, order, m);
            if (result) {
                *result = get_order_info(order);
            }
        }
        ret = put_order(m, order);
        if (ret < 0) {
            log_fatal("put_order fail: %d, order: %"PRIu64"", ret, order->id);
        } else if (real) {
            profile_inc("put_order", 1);
        }
    }

    if (side == MARKET_ORDER_SIDE_ASK) {
        check_stop_asks(real, m);
    } else {
        check_stop_bids(real, m);
    }

    return 0;
}

static int execute_market_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        ask_fee_asset = NULL;
        bid_fee_asset = NULL;

        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        // calculate deal price
        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);

        // calculate deal amount
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (strcmp(taker->fee_asset, m->stock) == 0) {
                mpd_add(require, require, taker->left, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                ask_fee_asset = taker->fee_asset;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }
        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        }

        // calculate bid fee
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                bid_fee_asset = maker->fee_asset;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
        }
        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);
        }

        mpd_rescale(ask_fee, ask_fee, -asset_prec(ask_fee_asset), &mpd_ctx);
        mpd_rescale(bid_fee, bid_fee, -asset_prec(bid_fee_asset), &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER,
                    price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
            push_deal_message(taker->update_time, deal_id, m, MARKET_TRADE_SIDE_SELL,
                    taker, maker, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        }

        // update taker
        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == taker->fee_asset) {
            mpd_add(taker->asset_fee, taker->asset_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, ask_fee_asset, ask_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (bid_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, BALANCE_TYPE_FROZEN, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, bid_fee_asset, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            finish_order(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }

        mpd_copy(m->last, price, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

static int execute_market_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        ask_fee_asset = NULL;
        bid_fee_asset = NULL;

        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        // calculate deal price
        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);

        // calculate deal amount
        mpd_div(amount, taker->left, price, &mpd_ctx);
        mpd_rescale(amount, amount, -m->stock_prec, &mpd_ctx);
        while (true) {
            mpd_mul(result, amount, price, &mpd_ctx);
            if (mpd_cmp(result, taker->left, &mpd_ctx) > 0) {
                mpd_set_i32(result, -m->stock_prec, &mpd_ctx);
                mpd_pow(result, mpd_ten, result, &mpd_ctx);
                mpd_sub(amount, amount, result, &mpd_ctx);
            } else {
                break;
            }
        }
        if (mpd_cmp(amount, maker->left, &mpd_ctx) > 0) {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }
        if (mpd_cmp(amount, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                ask_fee_asset = maker->fee_asset;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
        }
        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        }

        // calculate bid fee
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec(taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (strcmp(taker->fee_asset, m->money) == 0) {
                mpd_add(require, require, taker->left, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                bid_fee_asset = taker->fee_asset;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }
        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);
        }

        mpd_rescale(ask_fee, ask_fee, -asset_prec(ask_fee_asset), &mpd_ctx);
        mpd_rescale(bid_fee, bid_fee, -asset_prec(bid_fee_asset), &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER,
                    price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
            push_deal_message(taker->update_time, deal_id, m, MARKET_TRADE_SIDE_BUY,
                    maker, taker, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        }

        // update taker
        mpd_sub(taker->left, taker->left, deal, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        if (bid_fee_asset == taker->fee_asset) {
            mpd_add(taker->asset_fee, taker->asset_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, bid_fee_asset, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, BALANCE_TYPE_FROZEN, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, ask_fee_asset, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            finish_order(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }

        mpd_copy(m->last, price, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount,
        mpd_t *taker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
        if (real && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
            return -2;
        }
        if (skiplist_len(m->bids) == 0) {
            return -3;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        if ((fee_asset != NULL) && (strcmp(m->money, fee_asset) == 0) ) {
            if (!check_fee_asset(amount, balance, taker_fee, fee_asset, fee_discount)) {
                fee_asset = NULL;
            }
        }

        skiplist_iter *iter = skiplist_get_iterator(m->asks);
        skiplist_node *node = skiplist_next(iter);
        if (node == NULL) {
            skiplist_release_iterator(iter);
            return -3;
        }
        skiplist_release_iterator(iter);

        if (real) {
            order_t *order = node->value;
            mpd_t *require = mpd_new(&mpd_ctx);
            mpd_mul(require, order->price, m->min_amount, &mpd_ctx);
            if (mpd_cmp(amount, require, &mpd_ctx) < 0) {
                mpd_del(require);
                return -2;
            }

            mpd_del(require);
        }
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL)
        return -__LINE__;
    memset(order, 0, sizeof(order_t));

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_MARKET;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->frozen       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);
    order->asset_fee    = mpd_new(&mpd_ctx);
    order->fee_discount = mpd_new(&mpd_ctx);

    mpd_copy(order->price, mpd_zero, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->frozen, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->asset_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee_discount, mpd_zero, &mpd_ctx);

    if (fee_asset && strlen(fee_asset) > 0) {
        order->fee_asset = strdup(fee_asset);
        order->fee_price = get_fee_price(m, fee_asset);
        if (order->fee_price == NULL)
            return -__LINE__;
        if (fee_discount) {
            mpd_copy(order->fee_discount, fee_discount, &mpd_ctx);
        } else {
            mpd_copy(order->fee_discount, mpd_one, &mpd_ctx);
        }
    }

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
        balance_reset(user_id, m->stock);
    } else {
        ret = execute_market_bid_order(real, m, order);
        balance_reset(user_id, m->money);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (real) {
        int ret = append_order_history(order);
        if (ret < 0) {
            log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message(ORDER_EVENT_FILL, order, m);
        push_order_message(ORDER_EVENT_FINISH, order, m);
        if (result) {
            *result = get_order_info(order);
        }
    } else if (is_reader) {
        append_fini_order(order);
    }

    order_free(order);

    if (side == MARKET_ORDER_SIDE_ASK) {
        check_stop_asks(real, m);
    } else {
        check_stop_bids(real, m);
    }

    return 0;
}

int market_put_stop_limit(bool real, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *stop_price, mpd_t *price,
        mpd_t *taker_fee, mpd_t *maker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
        if (mpd_cmp(stop_price, m->last, &mpd_ctx) >= 0) {
            return -2;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, amount, price, &mpd_ctx);
        if (!balance || mpd_cmp(balance, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -1;
        }
        mpd_del(require);
        if (mpd_cmp(stop_price, m->last, &mpd_ctx) <= 0) {
            return -2;
        }
    }

    if (real && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
        return -3;
    }

    stop_t *stop = malloc(sizeof(stop_t));
    if (stop == NULL)
        return -__LINE__;
    memset(stop, 0, sizeof(stop_t));

    stop->id            = ++order_id_start;
    stop->type          = MARKET_ORDER_TYPE_LIMIT;
    stop->side          = side;
    stop->create_time   = current_timestamp();
    stop->update_time   = stop->create_time;
    stop->user_id       = user_id;
    stop->market        = strdup(m->name);
    stop->source        = strdup(source);
    stop->fee_asset     = strdup(fee_asset);
    stop->fee_discount  = mpd_new(&mpd_ctx);
    stop->stop_price    = mpd_new(&mpd_ctx);
    stop->price         = mpd_new(&mpd_ctx);
    stop->amount        = mpd_new(&mpd_ctx);
    stop->taker_fee     = mpd_new(&mpd_ctx);
    stop->maker_fee     = mpd_new(&mpd_ctx);

    mpd_copy(stop->stop_price, stop_price, &mpd_ctx);
    mpd_copy(stop->price, price, &mpd_ctx);
    mpd_copy(stop->amount, amount, &mpd_ctx);
    mpd_copy(stop->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(stop->maker_fee, maker_fee, &mpd_ctx);
    if (fee_discount) {
        mpd_copy(stop->fee_discount, fee_discount, &mpd_ctx);
    } else {
        mpd_copy(stop->fee_discount, mpd_one, &mpd_ctx);
    }

    int ret = put_stop(m, stop);
    if (ret < 0) {
        log_fatal("put_stop fail: %d", ret);
    } else if (real) {
        profile_inc("put_stop", 1);
    }

    if (real) {
        push_stop_message(STOP_EVENT_PUT, stop, m, 0);  
    }

    return 0;
}

int market_put_stop_market(bool real, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *stop_price,
        mpd_t *taker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
        if (mpd_cmp(stop_price, m->last, &mpd_ctx) >= 0) {
            return -2;
        }
        if (real && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
            return -2;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
        if (mpd_cmp(stop_price, m->last, &mpd_ctx) <= 0) {
            return -2;
        }
    }

    stop_t *stop = malloc(sizeof(stop_t));
    if (stop == NULL)
        return -__LINE__;
    memset(stop, 0, sizeof(stop_t));

    stop->id            = ++order_id_start;
    stop->type          = MARKET_ORDER_TYPE_MARKET;
    stop->side          = side;
    stop->create_time   = current_timestamp();
    stop->update_time   = stop->create_time;
    stop->user_id       = user_id;
    stop->market        = strdup(m->name);
    stop->source        = strdup(source);
    stop->fee_asset     = strdup(fee_asset);
    stop->fee_discount  = mpd_new(&mpd_ctx);
    stop->stop_price    = mpd_new(&mpd_ctx);
    stop->price         = mpd_new(&mpd_ctx);
    stop->amount        = mpd_new(&mpd_ctx);
    stop->taker_fee     = mpd_new(&mpd_ctx);
    stop->maker_fee     = mpd_new(&mpd_ctx);

    mpd_copy(stop->stop_price, stop_price, &mpd_ctx);
    mpd_copy(stop->price, mpd_zero, &mpd_ctx);
    mpd_copy(stop->amount, amount, &mpd_ctx);
    mpd_copy(stop->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(stop->maker_fee, mpd_zero, &mpd_ctx);
    if (fee_discount) {
        mpd_copy(stop->fee_discount, fee_discount, &mpd_ctx);
    } else {
        mpd_copy(stop->fee_discount, mpd_one, &mpd_ctx);
    }

    int ret = put_stop(m, stop);
    if (ret < 0) {
        log_fatal("put_stop fail: %d", ret);
    } else if (real) {
        profile_inc("put_stop", 1);
    }

    if (real) {
        push_stop_message(STOP_EVENT_PUT, stop, m, 0);
    }

    return 0;
}

int market_self_deal(bool real, market_t *market, mpd_t *amount, mpd_t *price, uint32_t side)
{
    // get ask_price_1
    mpd_t *ask_price_1 = NULL;
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    if (iter != NULL) {
        skiplist_node *node = skiplist_next(iter);
        if (node != NULL) {
            order_t *order = node->value;
            ask_price_1 = mpd_new(&mpd_ctx);
            mpd_copy(ask_price_1, order->price, &mpd_ctx);
        }
        skiplist_release_iterator(iter);
    } else {
        return -__LINE__;
    }

    // get bid_price_1
    mpd_t *bid_price_1 = NULL;
    iter = skiplist_get_iterator(market->bids);
    if (iter != NULL) {
        skiplist_node *node = skiplist_next(iter);
        if (node != NULL) {
            order_t *order = node->value;
            bid_price_1 = mpd_new(&mpd_ctx);
            mpd_copy(bid_price_1, order->price, &mpd_ctx);
        }
        skiplist_release_iterator(iter);
    } else {
        if (ask_price_1 != NULL)
            mpd_del(ask_price_1);
        return -__LINE__;
    }

    mpd_t *deal_min_gear = mpd_new(&mpd_ctx);
    mpd_set_i32(deal_min_gear, -market->money_prec, &mpd_ctx);
    mpd_pow(deal_min_gear, mpd_ten, deal_min_gear, &mpd_ctx);

    if (ask_price_1 != NULL && bid_price_1 != NULL) {
        mpd_t *ask_bid_sub = mpd_new(&mpd_ctx);
        mpd_sub(ask_bid_sub, ask_price_1, bid_price_1, &mpd_ctx);
        if (mpd_cmp(deal_min_gear, ask_bid_sub, &mpd_ctx) == 0) {
            mpd_del(ask_bid_sub);
            mpd_del(deal_min_gear);

            if (ask_price_1 != NULL)
                mpd_del(ask_price_1);
            if (bid_price_1 != NULL)
                mpd_del(bid_price_1);
            return -1;
        }
        mpd_del(ask_bid_sub);
    }

    mpd_t *real_price = mpd_qncopy(price);
    if (ask_price_1 != NULL && mpd_cmp(price, ask_price_1, &mpd_ctx) >= 0) {
        mpd_sub(real_price, ask_price_1, deal_min_gear, &mpd_ctx);
    } else if (bid_price_1 != NULL && mpd_cmp(price, bid_price_1, &mpd_ctx) <= 0){
        mpd_add(real_price, bid_price_1, deal_min_gear, &mpd_ctx);
    }

    mpd_t *deal = mpd_new(&mpd_ctx);
    mpd_mul(deal, real_price, amount, &mpd_ctx);

    uint64_t deal_id = ++deals_id_start;
    double update_time = current_timestamp();

    if (real) {
        order_t *order = malloc(sizeof(order_t));
        memset(order, 0, sizeof(order_t));
        order->id        = 0;
        order->user_id   = 0;
        push_deal_message(update_time, deal_id, market, side, order, order, real_price, amount, deal, market->money, mpd_zero, market->stock, mpd_zero);
        free(order);
    }
    
    mpd_del(deal);
    mpd_del(real_price);
    mpd_del(deal_min_gear);
    if (bid_price_1 != NULL)
        mpd_del(bid_price_1);
    if (ask_price_1 != NULL)
        mpd_del(ask_price_1);

    return 0;
}

int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)
{
    if (real) {
        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info(order);
    }
    finish_order(real, m, order);
    return 0;
}

int market_cancel_stop(bool real, json_t **result, market_t *m, stop_t *stop)
{
    if (real) {
        push_stop_message(STOP_EVENT_CANCEL, stop, m, 0);
        *result = get_stop_info(stop);
    }
    finish_stop(real, m, stop, MARKET_STOP_STATUS_CANCEL);
    return 0;
}

int market_put_order(market_t *m, order_t *order)
{
    return put_order(m, order);
}

int market_put_stop(market_t *m, stop_t *stop)
{
    return put_stop(m, stop);
}

order_t *market_get_order(market_t *m, uint64_t order_id)
{
    struct dict_order_key key = { .order_id = order_id };
    dict_entry *entry = dict_find(m->orders, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

stop_t *market_get_stop(market_t *m, uint64_t order_id)
{
    struct dict_order_key key = { .order_id = order_id };
    dict_entry *entry = dict_find(m->stops, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

skiplist_t *market_get_order_list(market_t *m, uint32_t user_id)
{
    struct dict_user_key key = { .user_id = user_id };
    if (m) {
        dict_entry *entry = dict_find(m->user_orders, &key);
        if (entry) {
            return entry->val;
        }
    } else {
        dict_entry *entry = dict_find(dict_user_orders, &key);
        if (entry) {
            return entry->val;
        }
    }

    return NULL;
}

skiplist_t *market_get_stop_list(market_t *m, uint32_t user_id)
{
    struct dict_user_key key = { .user_id = user_id };
    if (m) {
        dict_entry *entry = dict_find(m->user_stops, &key);
        if (entry) {
            return entry->val;
        }
    } else {
        dict_entry *entry = dict_find(dict_user_stops, &key);
        if (entry) {
            return entry->val;
        }
    }

    return NULL;
}

sds market_status(sds reply)
{
    reply = sdscatprintf(reply, "total user: %u\n", dict_size(dict_user_orders));
    reply = sdscatprintf(reply, "order last ID: %"PRIu64"\n", order_id_start);
    reply = sdscatprintf(reply, "deals last ID: %"PRIu64"\n", deals_id_start);
    return reply;
}

json_t *market_get_fini_order(uint64_t order_id)
{
    if (!is_reader)
        return NULL;

    struct dict_order_key order_key = { .order_id = order_id };
    dict_entry *entry = dict_find(dict_fini_orders, &order_key);
    if (entry) {
        json_t *order = entry->val;
        json_incref(order);
        return order;
    }
    return NULL;
}

int market_set_reader()
{
    is_reader = true;
    dict_types types_order;
    memset(&types_order, 0, sizeof(types_order));
    types_order.hash_function  = dict_order_hash_function;
    types_order.key_compare    = dict_order_key_compare;
    types_order.key_dup        = dict_order_key_dup;
    types_order.key_destructor = dict_order_key_free;
    types_order.val_destructor = dict_order_value_free;

    dict_fini_orders = dict_create(&types_order, 1024);
    if (dict_fini_orders == NULL) {
        return -__LINE__;
    }

    nw_timer_set(&timer_fini_order, 0.5, true, on_timer_fini_order, NULL);
    nw_timer_start(&timer_fini_order);

    return 0;
}
