/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_asset.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"

uint64_t order_id_start;
uint64_t deals_id_start;

static dict_t *dict_user_orders;
static dict_t *dict_user_stops;
static dict_t *dict_fini_orders;

static nw_timer timer_status;
static nw_timer timer_fini_order;
static bool is_reader;

struct dict_user_val {
    skiplist_t *full_list;
    dict_t *accounts;
};

static void *dict_user_val_dup(const void *val)
{
    struct dict_user_val *obj = malloc(sizeof(struct dict_user_val));
    memcpy(obj, val, sizeof(struct dict_user_val));
    return obj;
}

static void dict_user_val_free(void *val)
{
    struct dict_user_val *obj = val;
    skiplist_release(obj->full_list);
    dict_release(obj->accounts);
    free(obj);
}

static void dict_skiplist_val_free(void *val)
{
    skiplist_release(val);
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
    if (stop1->state == STOP_STATE_LOW) {
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
    mpd_del(order->money_fee);
    mpd_del(order->stock_fee);
    mpd_del(order->asset_fee);
    mpd_del(order->fee_price);
    mpd_del(order->fee_discount);
    mpd_del(order->last_deal_amount);
    mpd_del(order->last_deal_price);

    free(order->market);
    free(order->source);
    if (order->fee_asset)
        free(order->fee_asset);
    
    if (order->client_id)
        free(order->client_id);

    free(order);
}

static void stop_free(stop_t *stop)
{
    mpd_del(stop->stop_price);
    mpd_del(stop->price);
    mpd_del(stop->amount);
    mpd_del(stop->taker_fee);
    mpd_del(stop->maker_fee);
    mpd_del(stop->fee_discount);

    free(stop->market);
    free(stop->source);
    if (stop->fee_asset)
        free(stop->fee_asset);

    if (stop->client_id)
        free(stop->client_id);

    free(stop);
}

json_t *get_order_balance(order_t *order, market_t *m)
{    
    uint32_t user_id = order->user_id;
    uint32_t account = order->account;

    struct asset_type *stock_type = get_asset_type(order->account, m->stock);
    struct asset_type *money_type = get_asset_type(order->account, m->money);

    mpd_t *stock_available = balance_available(user_id, account, m->stock);
    mpd_t *stock_frozen = balance_frozen_lock(user_id, account, m->stock);
    mpd_rescale(stock_available, stock_available, -stock_type->prec_show, &mpd_ctx);
    mpd_rescale(stock_frozen, stock_frozen, -stock_type->prec_show, &mpd_ctx);

    mpd_t *money_available = balance_available(user_id, account, m->money);
    mpd_t *money_frozen = balance_frozen_lock(user_id, account, m->money);
    mpd_rescale(money_available, money_available, -money_type->prec_show, &mpd_ctx);
    mpd_rescale(money_frozen, money_frozen, -money_type->prec_show, &mpd_ctx);

    json_t *info = json_object();
    json_object_set_new_mpd(info, "stock_available", stock_available);
    json_object_set_new_mpd(info, "stock_frozen", stock_frozen);
    json_object_set_new_mpd(info, "money_available", money_available);
    json_object_set_new_mpd(info, "money_frozen", money_frozen);

    const char *fee_asset = order->fee_asset;
    if (fee_asset && mpd_cmp(order->asset_fee, mpd_zero, &mpd_ctx) > 0 && strcmp(fee_asset, m->stock) != 0 && strcmp(fee_asset, m->money) != 0) {
        uint32_t fee_account = order->account;
        if (strcmp(fee_asset, SYSTEM_FEE_TOKEN) == 0) {
            fee_account = 0;
        }

        struct asset_type *fee_type = get_asset_type(fee_account, fee_asset);
        if (fee_type) {
            json_t *fee_info = json_object();
            mpd_t *fee_available = balance_available(user_id, fee_account, fee_asset);
            mpd_t *fee_frozen = balance_frozen_lock(user_id, fee_account, fee_asset);
            mpd_rescale(fee_available, fee_available, -fee_type->prec_show, &mpd_ctx);
            mpd_rescale(fee_frozen, fee_frozen, -fee_type->prec_show, &mpd_ctx);

            json_object_set_new(fee_info, "account", json_integer(fee_account));
            json_object_set_new(fee_info, "asset", json_string(fee_asset));
            json_object_set_new_mpd(fee_info, "available", fee_available);
            json_object_set_new_mpd(fee_info, "frozen", fee_frozen);
            json_object_set_new(info, "fee", fee_info);

            mpd_del(fee_available);
            mpd_del(fee_frozen);
        }
    }

    mpd_del(stock_available);
    mpd_del(stock_frozen);
    mpd_del(money_available);
    mpd_del(money_frozen);

    return info;
}

json_t *get_order_info(order_t *order, bool with_last_deal)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "side", json_integer(order->side));
    json_object_set_new(info, "user", json_integer(order->user_id));
    json_object_set_new(info, "account", json_integer(order->account));
    json_object_set_new(info, "option", json_integer(order->option));
    json_object_set_new(info, "ctime", json_real(order->create_time));
    json_object_set_new(info, "mtime", json_real(order->update_time));
    json_object_set_new(info, "market", json_string(order->market));
    json_object_set_new(info, "source", json_string(order->source));
    if (order->client_id) {
        json_object_set_new(info, "client_id", json_string(order->client_id));
    } else {
        json_object_set_new(info, "client_id", json_string(""));
    }

    json_object_set_new_mpd(info, "price", order->price);
    json_object_set_new_mpd(info, "amount", order->amount);
    json_object_set_new_mpd(info, "taker_fee", order->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", order->maker_fee);
    json_object_set_new_mpd(info, "left", order->left);
    json_object_set_new_mpd(info, "deal_stock", order->deal_stock);
    json_object_set_new_mpd(info, "deal_money", order->deal_money);
    json_object_set_new_mpd(info, "money_fee", order->money_fee);
    json_object_set_new_mpd(info, "stock_fee", order->stock_fee);
    json_object_set_new_mpd(info, "asset_fee", order->asset_fee);
    json_object_set_new_mpd(info, "fee_discount", order->fee_discount);
    if (with_last_deal) {
        json_object_set_new_mpd(info, "last_deal_amount", order->last_deal_amount);
        json_object_set_new_mpd(info, "last_deal_price", order->last_deal_price);
        json_object_set_new(info, "last_deal_time", json_real(order->last_deal_time));
        json_object_set_new(info, "last_deal_id", json_integer(order->last_deal_id));
        json_object_set_new(info, "last_role", json_integer(order->last_role));
    }

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
    json_object_set_new(info, "account", json_integer(stop->account));
    json_object_set_new(info, "option", json_integer(stop->option));
    json_object_set_new(info, "state", json_integer(stop->state));
    json_object_set_new(info, "ctime", json_real(stop->create_time));
    json_object_set_new(info, "mtime", json_real(stop->update_time));
    json_object_set_new(info, "market", json_string(stop->market));
    json_object_set_new(info, "source", json_string(stop->source));
    if (stop->client_id) {
        json_object_set_new(info, "client_id", json_string(stop->client_id));
    } else {
        json_object_set_new(info, "client_id", json_string(""));
    }

    json_object_set_new_mpd(info, "stop_price", stop->stop_price);
    json_object_set_new_mpd(info, "price", stop->price);
    json_object_set_new_mpd(info, "amount", stop->amount);
    json_object_set_new_mpd(info, "taker_fee", stop->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", stop->maker_fee);
    json_object_set_new_mpd(info, "fee_discount", stop->fee_discount);

    if (stop->fee_asset) {
        json_object_set_new(info, "fee_asset", json_string(stop->fee_asset));
    } else {
        json_object_set_new(info, "fee_asset", json_null());
    }

    return info;
}

static int record_fini_order(order_t *order)
{
    json_t *order_info = get_order_info(order, false);
    json_object_set_new(order_info, "finished", json_true());
    uint64_t order_key = order->id;
    dict_add(dict_fini_orders, &order_key, order_info);

    return 0;
}

static skiplist_t *user_order_list_query(dict_t *dict, uint32_t user_id, int account)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)user_id);
    if (entry == NULL)
        return NULL;

    struct dict_user_val *obj = entry->val;
    if (account < 0) {
        return obj->full_list;
    }

    entry = dict_find(obj->accounts, (void *)(uintptr_t)account);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

static int user_order_list_insert(dict_t *dict, uint32_t user_id, uint32_t account, skiplist_type *st, void *order)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)user_id);
    if (entry == NULL) {
        struct dict_user_val val;
        val.full_list = skiplist_create(st);
        if (val.full_list == NULL)
            return -__LINE__;

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function  = uint32_dict_hash_func;
        dt.key_compare    = uint32_dict_key_compare;
        dt.val_destructor = dict_skiplist_val_free;
        val.accounts = dict_create(&dt, 16);
        if (val.accounts == NULL)
            return -__LINE__;

        entry = dict_add(dict, (void *)(uintptr_t)user_id, &val);
        if (entry == NULL)
            return -__LINE__;
    }

    struct dict_user_val *obj = entry->val;
    entry = dict_find(obj->accounts, (void *)(uintptr_t)account);
    if (entry == NULL) {
        skiplist_t *list = skiplist_create(st);
        if (list == NULL)
            return -__LINE__;

        entry = dict_add(obj->accounts, (void *)(uintptr_t)account, list);
        if (entry == NULL)
            return -__LINE__;
    }

    if (skiplist_insert(entry->val, order) == NULL)
        return -__LINE__;
    if (skiplist_insert(obj->full_list, order) == NULL)
        return -__LINE__;

    return 0;
}

static void user_order_list_delete(dict_t *dict, uint32_t user_id, uint32_t account, void *order)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)user_id);
    if (entry == NULL)
        return;

    struct dict_user_val *obj = entry->val;
    entry = dict_find(obj->accounts, (void *)(uintptr_t)account);
    if (entry == NULL)
        return;
    skiplist_t *account_list = entry->val;

    skiplist_node *node = skiplist_find(obj->full_list, order);
    if (node)
        skiplist_delete(obj->full_list, node);
    node = skiplist_find(account_list, order);
    if (node)
        skiplist_delete(account_list, node);

    if (skiplist_len(account_list) == 0)
        dict_delete(obj->accounts, (void *)(uintptr_t)account);
    if (dict_size(obj->accounts) == 0)
        dict_delete(dict, (void *)(uintptr_t)user_id);
}

static int frozen_order(market_t *m, order_t *order)
{
    if (order->side == MARKET_ORDER_SIDE_ASK) {
        if (balance_freeze(order->user_id, order->account, BALANCE_TYPE_FROZEN, m->stock, order->left) == NULL) {
            return -__LINE__;
        }
        mpd_copy(order->frozen, order->left, &mpd_ctx);
    } else {
        mpd_t *result = mpd_new(&mpd_ctx);
        mpd_mul(result, order->price, order->left, &mpd_ctx);
        if (balance_freeze(order->user_id, order->account, BALANCE_TYPE_FROZEN, m->money, result) == NULL) {
            mpd_del(result);
            return -__LINE__;
        }
        mpd_copy(order->frozen, result, &mpd_ctx);
        mpd_del(result);
    }
    return 0;
}

static int put_order(market_t *m, order_t *order)
{
    if (order->type != MARKET_ORDER_TYPE_LIMIT)
        return -__LINE__;

    uint64_t order_key = order->id;
    if (dict_add(m->orders, &order_key, order) == NULL)
        return -__LINE__;

    skiplist_type st;
    memset(&st, 0, sizeof(st));
    st.compare = order_id_compare;

    if (user_order_list_insert(dict_user_orders, order->user_id, order->account, &st, order) < 0)
        return -__LINE__;
    if (user_order_list_insert(m->user_orders, order->user_id, order->account, &st, order) < 0)
        return -__LINE__;

    if (order->side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_insert(m->asks, order) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->bids, order) == NULL)
            return -__LINE__;
    }

    return 0;
}

static int finish_order(bool real, market_t *m, order_t *order)
{
    order->update_time = current_timestamp();

    uint64_t order_key = order->id;
    dict_delete(m->orders, &order_key);

    if (order->side == MARKET_ORDER_SIDE_ASK) {
        skiplist_node *node = skiplist_find(m->asks, order);
        if (node) {
            skiplist_delete(m->asks, node);
        }
        if (mpd_cmp(order->frozen, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, order->account, BALANCE_TYPE_FROZEN, m->stock, order->frozen) == NULL)
                return -__LINE__;
        }
        balance_reset(order->user_id, order->account, m->stock);
    } else {
        skiplist_node *node = skiplist_find(m->bids, order);
        if (node) {
            skiplist_delete(m->bids, node);
        }
        if (mpd_cmp(order->frozen, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, order->account, BALANCE_TYPE_FROZEN, m->money, order->frozen) == NULL)
                return -__LINE__;
        }
        balance_reset(order->user_id, order->account, m->money);
    }

    user_order_list_delete(dict_user_orders, order->user_id, order->account, order);
    user_order_list_delete(m->user_orders, order->user_id, order->account, order);

    if (mpd_cmp(order->deal_stock, mpd_zero, &mpd_ctx) > 0) {
        if (real) {
            append_order_history(order);
        } else if (is_reader) {
            record_fini_order(order);
        }
    } else if (is_reader) {
        record_fini_order(order);
    }

    if (real) {
        push_order_message(ORDER_EVENT_FINISH, order, m);
    }

    order_free(order);
    if (real) {
        profile_inc("finish_order", 1);
    }

    return 0;
}

static int put_stop(market_t *m, stop_t *stop)
{
    uint64_t order_key = stop->id;
    if (dict_add(m->stops, &order_key, stop) == NULL)
        return -__LINE__;

    skiplist_type st;
    memset(&st, 0, sizeof(st));
    st.compare = stop_id_compare;

    if (user_order_list_insert(dict_user_stops, stop->user_id, stop->account, &st, stop) < 0)
        return -__LINE__;
    if (user_order_list_insert(m->user_stops, stop->user_id, stop->account, &st, stop) < 0)
        return -__LINE__;

    if (stop->state == STOP_STATE_HIGH) {
        if (skiplist_insert(m->stop_high, stop) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->stop_low, stop) == NULL)
            return -__LINE__;
    }

    return 0;
}

static int finish_stop(bool real, market_t *m, stop_t *stop, int status)
{
    stop->update_time = current_timestamp();

    uint64_t order_key = stop->id;
    dict_delete(m->stops, &order_key);

    if (stop->state == STOP_STATE_LOW) {
        skiplist_node *node = skiplist_find(m->stop_low, stop);
        if (node) {
            skiplist_delete(m->stop_low, node);
        }
    } else {
        skiplist_node *node = skiplist_find(m->stop_high, stop);
        if (node) {
            skiplist_delete(m->stop_high, node);
        }
    }

    user_order_list_delete(dict_user_stops, stop->user_id, stop->account, stop);
    user_order_list_delete(m->user_stops, stop->user_id, stop->account, stop);

    if (real) {
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
    profile_set("pending_stop_users", dict_size(dict_user_stops));
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
    dt.hash_function    = uint32_dict_hash_func;
    dt.key_compare      = uint32_dict_key_compare;
    dt.val_dup          = dict_user_val_dup;
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

market_t *market_create(json_t *conf)
{
    const char *name = json_string_value(json_object_get(conf, "name"));
    if (name == NULL)
        return NULL;

    mpd_t *min_amount = NULL;
    int fee_prec;
    int account;

    if (read_cfg_mpd(conf, "min_amount", &min_amount, NULL) < 0)
        return NULL;
    if (read_cfg_int(conf, "fee_prec", &fee_prec, false, 4) < 0)
        return NULL;
    if (read_cfg_int(conf, "account", &account, true, 0) < 0)
        return NULL;

    json_t *stock = json_object_get(conf, "stock");
    json_t *money = json_object_get(conf, "money");
    const char *stock_name = json_string_value(json_object_get(stock, "name"));
    const char *money_name = json_string_value(json_object_get(money, "name"));
    if (stock_name == NULL || money_name == NULL)
        return NULL;

    int stock_prec, money_prec;
    if (read_cfg_int(stock, "prec", &stock_prec, true, 0) < 0)
        return NULL;
    if (read_cfg_int(money, "prec", &money_prec, true, 0) < 0)
        return NULL;

    int check_account = (account == -1) ? 0 : account;
    if (!asset_exist(check_account, stock_name) || !asset_exist(check_account, money_name))
        return NULL;
    if (stock_prec + fee_prec + settings.discount_prec > asset_prec_save(check_account, stock_name))
        return NULL;
    if (stock_prec + money_prec + fee_prec + settings.discount_prec > asset_prec_save(check_account, money_name))
        return NULL;

    market_t *m = malloc(sizeof(market_t));
    memset(m, 0, sizeof(market_t));
    m->name             = strdup(name);
    m->stock            = strdup(stock_name);
    m->money            = strdup(money_name);
    m->stock_prec       = stock_prec;
    m->money_prec       = money_prec;
    m->fee_prec         = fee_prec;
    m->account          = account;
    m->min_amount       = min_amount;
    m->last             = mpd_qncopy(mpd_zero);
    m->call_auction     = false;
    m->last_calc_time   = 0;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = uint32_dict_hash_func;
    dt.key_compare      = uint32_dict_key_compare;
    dt.val_dup          = dict_user_val_dup;
    dt.val_destructor   = dict_user_val_free;

    m->user_orders = dict_create(&dt, 1024);
    if (m->user_orders == NULL)
        return NULL;
    m->user_stops = dict_create(&dt, 1024);
    if (m->user_stops == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = uint64_dict_key_hash_func;
    dt.key_compare      = uint64_dict_key_compare;
    dt.key_dup          = uint64_dict_key_dup;
    dt.key_destructor   = uint64_dict_key_free;

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
    m->stop_high = skiplist_create(&lt);
    m->stop_low = skiplist_create(&lt);
    if (m->stop_high == NULL || m->stop_low == NULL)
        return NULL;

    return m;
}

int market_update(market_t *m, json_t *conf)
{
    mpd_t *min_amount = NULL;
    ERR_RET_LN(read_cfg_mpd(conf, "min_amount", &min_amount, NULL));
    mpd_copy(m->min_amount, min_amount, &mpd_ctx); 
    mpd_del(min_amount);

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
    int ret = append_user_balance_history(order->update_time, order->user_id, order->account, asset, "trade", change, detail_str);
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
    int ret = append_user_balance_history(order->update_time, order->user_id, order->account, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int append_balance_trade_fee(order_t *order, uint32_t fee_account, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount, mpd_t *fee_rate)
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
    int ret = append_user_balance_history(order->update_time, order->user_id, fee_account, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int active_stop_limit(bool real, market_t *m, stop_t *stop)
{
    int status = MARKET_STOP_STATUS_ACTIVE;
    mpd_t *fee_price = mpd_qncopy(mpd_zero);
    if (stop->fee_asset) {
        get_fee_price(m, stop->fee_asset, fee_price);
    }

    int ret = market_put_limit_order(real, NULL, m, stop->user_id, stop->account, stop->side, stop->amount, stop->price,
                stop->taker_fee, stop->maker_fee, stop->source, stop->fee_asset, fee_price, stop->fee_discount, stop->option, stop->client_id);
    if (ret < 0) {
        status = MARKET_STOP_STATUS_FAIL;
    }

    if (real) {
        push_stop_message(STOP_EVENT_ACTIVE, stop, m, status);
    }

    mpd_del(fee_price);
    return finish_stop(real, m, stop, status);
}

static int active_stop_market(bool real, market_t *m, stop_t *stop)
{
    int status = MARKET_STOP_STATUS_ACTIVE;
    mpd_t *fee_price = mpd_qncopy(mpd_zero);
    if (stop->fee_asset) {
        get_fee_price(m, stop->fee_asset, fee_price);
    }

    int ret = market_put_market_order(real, NULL, m, stop->user_id, stop->account, stop->side, stop->amount,
                stop->taker_fee, stop->source, stop->fee_asset, fee_price, stop->fee_discount, stop->option, stop->client_id);
    if (ret < 0) {
        status = MARKET_STOP_STATUS_FAIL;
    }

    if (real) {
        push_stop_message(STOP_EVENT_ACTIVE, stop, m, status);
    }

    mpd_del(fee_price);
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

static int check_stop_low(bool real, market_t *m)
{
    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->stop_low);
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;    
        if (mpd_cmp(stop->stop_price, m->last, &mpd_ctx) >= 0) {
            skiplist_delete(m->stop_low, node);
            active_stop(real, m, stop);
            skiplist_reset_iterator(m->stop_low, iter);
        } else {
            break;
        }
    }
    skiplist_release_iterator(iter);

    return 0;
}

static int check_stop_high(bool real, market_t *m)
{
    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->stop_high);
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;
        if (mpd_cmp(stop->stop_price, m->last, &mpd_ctx) <= 0) {
            skiplist_delete(m->stop_high, node);
            active_stop(real, m, stop);
            skiplist_reset_iterator(m->stop_high, iter);
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

    uint32_t ask_fee_account = 0;
    uint32_t bid_fee_account = 0;

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    bool ask_use_stock_fee = (taker->option & OPTION_SUGGEST_STOCK_FEE) ? true : false;

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

        bool bid_use_money_fee = (maker->option & OPTION_SUGGEST_MONEY_FEE) ? true : false;

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = taker->account;
            if (strcmp(taker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (taker->account == fee_account && strcmp(taker->fee_asset, m->stock) == 0) {
                mpd_add(require, require, taker->left, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                ask_fee_asset = taker->fee_asset;
                ask_fee_account = fee_account;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }

        if (ask_fee_asset == NULL && ask_use_stock_fee) {
            mpd_mul(result, amount, taker->taker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(taker->account, m->stock), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            mpd_add(require, require, taker->left, &mpd_ctx);
            mpd_t *fee_balance = balance_get(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->stock);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                ask_fee_asset = m->stock;
                ask_fee_account = taker->account;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }

        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            ask_fee_account = taker->account;
            mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
            mpd_rescale(ask_fee, ask_fee, -asset_prec_save(ask_fee_account, ask_fee_asset), &mpd_ctx);
        }

        // update taker
        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == taker->fee_asset) {
            mpd_add(taker->asset_fee, taker->asset_fee, ask_fee, &mpd_ctx);
        } else if (ask_fee_asset == m->money) {
            mpd_add(taker->money_fee, taker->money_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(taker->stock_fee, taker->stock_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(taker->user_id, ask_fee_account, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(taker, ask_fee_account, ask_fee_asset, ask_fee, price, amount, taker->taker_fee);
            }
        }

        // calculate bid fee
        get_fee_price(m, maker->fee_asset, maker->fee_price);
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = maker->account;
            if (strcmp(maker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                bid_fee_asset = maker->fee_asset;
                bid_fee_account = fee_account;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
        }

        if (bid_fee_asset == NULL && bid_use_money_fee) {
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(maker->account, m->money), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->money);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                bid_fee_asset = m->money;
                bid_fee_account = maker->account;
                mpd_mul(bid_fee, deal, maker->maker_fee, &mpd_ctx);
            }
        }

        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            bid_fee_account = maker->account;
            mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);
            mpd_rescale(bid_fee, bid_fee, -asset_prec_save(bid_fee_account, bid_fee_asset), &mpd_ctx);
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER,
                    price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
            push_deal_message(taker->update_time, deal_id, m, MARKET_TRADE_SIDE_SELL,
                    taker, maker, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        }

        // update maker
        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (bid_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, bid_fee, &mpd_ctx);
        } else if (bid_fee_asset == m->money) {
            mpd_add(maker->money_fee, maker->money_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(maker->stock_fee, maker->stock_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, maker->account, BALANCE_TYPE_FROZEN, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(maker->user_id, bid_fee_account, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(maker, bid_fee_account, bid_fee_asset, bid_fee, price, amount, maker->maker_fee);
            }
        }

        mpd_copy(taker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(taker->last_deal_price, price, &mpd_ctx);
        taker->last_deal_time = taker->update_time;
        taker->last_deal_id = deal_id;
        taker->last_role = MARKET_ROLE_TAKER;

        mpd_copy(maker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(maker->last_deal_price, price, &mpd_ctx);
        maker->last_deal_time = maker->update_time;
        maker->last_deal_id = deal_id;
        maker->last_role = MARKET_ROLE_MAKER;

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
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

    uint32_t ask_fee_account = 0;
    uint32_t bid_fee_account = 0;

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    bool bid_use_money_fee = (taker->option & OPTION_SUGGEST_MONEY_FEE) ? true : false;

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

        bool ask_use_stock_fee = (maker->option & OPTION_SUGGEST_STOCK_FEE) ? true : false;

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        get_fee_price(m, maker->fee_asset, maker->fee_price);
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = maker->account;
            if (strcmp(maker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                ask_fee_asset = maker->fee_asset;
                ask_fee_account = fee_account;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
        }

        if (ask_fee_asset == NULL && ask_use_stock_fee) {
            mpd_mul(result, amount, maker->maker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(maker->account, m->stock), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->stock);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                ask_fee_asset = m->stock;
                ask_fee_account = maker->account;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
        }

        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            ask_fee_account = maker->account;
            mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
            mpd_rescale(ask_fee, ask_fee, -asset_prec_save(ask_fee_account, ask_fee_asset), &mpd_ctx);
        }

        // update maker
        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, ask_fee, &mpd_ctx);
        } else if (ask_fee_asset == m->money) {
            mpd_add(maker->money_fee, maker->money_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(maker->stock_fee, maker->stock_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, maker->account, BALANCE_TYPE_FROZEN, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(maker->user_id, ask_fee_account, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(maker, ask_fee_account, ask_fee_asset, ask_fee, price, amount, maker->maker_fee);
            }
        }

        // calculate bid fee
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = taker->account;
            if (strcmp(taker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (taker->account == fee_account && strcmp(taker->fee_asset, m->money) == 0) {
                mpd_mul(require, taker->left, taker->price, &mpd_ctx);
                mpd_add(require, require, result, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                bid_fee_asset = taker->fee_asset;
                bid_fee_account = fee_account;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }
        if (bid_fee_asset == NULL && bid_use_money_fee) {
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(taker->account, m->money), &mpd_ctx);

            mpd_t *require = mpd_new(&mpd_ctx);
            mpd_mul(require, taker->left, taker->price, &mpd_ctx);
            mpd_rescale(require, require, -asset_prec_save(taker->account, m->money), &mpd_ctx);

            mpd_add(require, require, result, &mpd_ctx);
            mpd_t *fee_balance = balance_get(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->money);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                bid_fee_asset = m->money;
                bid_fee_account = taker->account;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }

        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            bid_fee_account = taker->account;
            mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);
            mpd_rescale(bid_fee, bid_fee, -asset_prec_save(bid_fee_account, bid_fee_asset), &mpd_ctx);
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER,
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
        } else if (bid_fee_asset == m->money) {
            mpd_add(taker->money_fee, taker->money_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(taker->stock_fee, taker->stock_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(taker->user_id, bid_fee_account, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(taker, bid_fee_account, bid_fee_asset, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_copy(taker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(taker->last_deal_price, price, &mpd_ctx);
        taker->last_deal_time = taker->update_time;
        taker->last_deal_id = deal_id;
        taker->last_role = MARKET_ROLE_TAKER;

        mpd_copy(maker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(maker->last_deal_price, price, &mpd_ctx);
        maker->last_deal_time = maker->update_time;
        maker->last_deal_id = deal_id;
        maker->last_role = MARKET_ROLE_MAKER;

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
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

static int calc_call_auction_basic_price(market_t *m, bool force_calc)
{
    double now = current_timestamp();
    if (!force_calc && m->last_calc_time != 0 && m->last_calc_time > now) {
        return 0;
    }
    m->last_calc_time = now + settings.call_auction_calc_interval;

    if (skiplist_len(m->asks) == 0 && skiplist_len(m->bids) == 0) {
        mpd_copy(m->last, mpd_zero, &mpd_ctx);
        return 1;
    }

    if (skiplist_len(m->asks) == 0) {
        skiplist_node *node = skiplist_header(m->bids);
        order_t *order = node->value;
        mpd_copy(m->last, order->price, &mpd_ctx);
        return 1;
    }

    if (skiplist_len(m->bids) == 0) {
        skiplist_node *node = skiplist_header(m->asks);
        order_t *order = node->value;
        mpd_copy(m->last, order->price, &mpd_ctx);
        return 1;
    }

    mpd_t *basic_price = mpd_qncopy(mpd_zero);
    skiplist_iter *ask_iter = skiplist_get_iterator(m->asks);
    skiplist_iter *bid_iter = skiplist_get_iterator(m->bids);
    skiplist_node *ask_node = skiplist_next(ask_iter);
    skiplist_node *bid_node = skiplist_next(bid_iter);

    mpd_t *ask_amount = mpd_qncopy(mpd_zero);
    mpd_t *bid_amount = mpd_qncopy(mpd_zero);
    mpd_t *mpd_two = mpd_new(&mpd_ctx);
    mpd_set_string(mpd_two, "2", &mpd_ctx);

    while (ask_node != NULL && bid_node != NULL) {
        order_t *ask_order = ask_node->value;
        order_t *bid_order = bid_node->value;
        if (mpd_cmp(ask_order->price, bid_order->price, &mpd_ctx) > 0)
            break;

        if (mpd_cmp(ask_amount, mpd_zero, &mpd_ctx) == 0)
            mpd_copy(ask_amount, ask_order->amount, &mpd_ctx);

        if (mpd_cmp(bid_amount, mpd_zero, &mpd_ctx) == 0)
            mpd_copy(bid_amount, bid_order->amount, &mpd_ctx);

        if (mpd_cmp(ask_amount, bid_amount, &mpd_ctx) > 0) {
            mpd_sub(ask_amount, ask_amount, bid_amount, &mpd_ctx);
            mpd_copy(bid_amount, mpd_zero, &mpd_ctx);
            bid_node = skiplist_next(bid_iter);
        } else if (mpd_cmp(bid_amount, ask_amount, &mpd_ctx) > 0) {
            mpd_sub(bid_amount, bid_amount, ask_amount, &mpd_ctx);
            mpd_copy(ask_amount, mpd_zero, &mpd_ctx);
            ask_node = skiplist_next(ask_iter);
        } else {
            bid_node = skiplist_next(bid_iter);
            ask_node = skiplist_next(ask_iter);
            mpd_copy(bid_amount, mpd_zero, &mpd_ctx);
            mpd_copy(ask_amount, mpd_zero, &mpd_ctx);
        }

        if (mpd_cmp(ask_order->price, bid_order->price, &mpd_ctx) == 0) {
            mpd_copy(basic_price, bid_order->price, &mpd_ctx);
        } else {
            mpd_add(basic_price, bid_order->price, ask_order->price, &mpd_ctx);
            mpd_div(basic_price, basic_price, mpd_two, &mpd_ctx);
            mpd_rescale(basic_price, basic_price, -m->money_prec, &mpd_ctx);
        }
    }
    skiplist_release_iterator(ask_iter);
    skiplist_release_iterator(bid_iter);
    mpd_del(ask_amount);
    mpd_del(bid_amount);

    int ret = 0;
    if (mpd_cmp(basic_price, mpd_zero, &mpd_ctx) == 0) {
        skiplist_node *node = skiplist_header(m->asks);
        order_t *order = node->value;
        mpd_add(basic_price, basic_price, order->price, &mpd_ctx);

        node = skiplist_header(m->bids);
        order = node->value;
        mpd_add(basic_price, basic_price, order->price, &mpd_ctx);
        mpd_div(basic_price, basic_price, mpd_two, &mpd_ctx);
        mpd_rescale(basic_price, basic_price, -m->money_prec, &mpd_ctx);
        ret = 1;
    }
    mpd_copy(m->last, basic_price, &mpd_ctx);
    mpd_del(basic_price);
    mpd_del(mpd_two);

    return ret;
}

static bool check_limit_fill_or_kill(market_t *m, uint32_t side, mpd_t *price, mpd_t *amount)
{
    mpd_t *fill_amount = mpd_qncopy(mpd_zero);
    skiplist_node *node;
    skiplist_iter *iter;
    if (side == MARKET_ORDER_SIDE_ASK) {
        iter = skiplist_get_iterator(m->bids);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) > 0) {
                break;
            }

            mpd_add(fill_amount, fill_amount, order->left, &mpd_ctx);
            if (mpd_cmp(fill_amount, amount, &mpd_ctx) >= 0) {
                break;
            }
        }
    } else {
        iter = skiplist_get_iterator(m->asks);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) < 0) {
                break;
            }

            mpd_add(fill_amount, fill_amount, order->left, &mpd_ctx);
            if (mpd_cmp(fill_amount, amount, &mpd_ctx) >= 0) {
                break;
            }
        }
    }
    skiplist_release_iterator(iter);

    bool ret = false;
    if (mpd_cmp(fill_amount, amount, &mpd_ctx) >= 0) {
        ret = true;
    }
    mpd_del(fill_amount);

    return ret;
}

static bool check_limit_maker_only(market_t *m, uint32_t side, mpd_t *price)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_len(m->bids) > 0) {
            skiplist_node *node = skiplist_header(m->bids);
            order_t *order = node->value;
            if (mpd_cmp(order->price, price, &mpd_ctx) >= 0 ){
                return false;
            }
        }
    } else {
        if (skiplist_len(m->asks) > 0) {
            skiplist_node *node = skiplist_header(m->asks);
            order_t *order = node->value;
            if (mpd_cmp(order->price, price, &mpd_ctx) <= 0) {
                return false;
            }
        }
    }

    return true;
}

int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t account, uint32_t side, mpd_t *amount,
        mpd_t *price, mpd_t *taker_fee, mpd_t *maker_fee, const char *source, const char *fee_asset, mpd_t *fee_price, mpd_t *fee_discount, uint32_t option, const char *client_id)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, account, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
    } else {
        mpd_t *balance = balance_get(user_id, account, BALANCE_TYPE_AVAILABLE, m->money);
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, amount, price, &mpd_ctx);
        if (!balance || mpd_cmp(balance, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -1;
        }
        mpd_del(require);
    }

    bool unlimited_min = (option & OPTION_UNLIMITED_MIN_AMOUNT) ? true : false;
    if (real && !unlimited_min && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
        return -2;
    }

    bool fill_or_kill = (option & OPTION_FILL_OR_KILL) ? true : false;
    if (real && fill_or_kill && !check_limit_fill_or_kill(m, side, price, amount)) {
        return -3;
    }

    bool only_maker = (option & OPTION_MAKER_ONLY) ? true : false;
    if (real && only_maker && !check_limit_maker_only(m, side, price)) {
        return -4;
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL)
        return -__LINE__;
    memset(order, 0, sizeof(order_t));

    order->id           = order_id_start + 1;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->account      = account;
    order->option       = option;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->frozen       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->money_fee    = mpd_new(&mpd_ctx);
    order->stock_fee    = mpd_new(&mpd_ctx);
    order->asset_fee    = mpd_new(&mpd_ctx);
    order->fee_price    = mpd_new(&mpd_ctx);
    order->fee_discount = mpd_new(&mpd_ctx);
    order->last_deal_amount = mpd_new(&mpd_ctx);
    order->last_deal_price  = mpd_new(&mpd_ctx);
    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, maker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->frozen, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->money_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->stock_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->asset_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee_price, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee_discount, mpd_one, &mpd_ctx);
    mpd_copy(order->last_deal_amount, mpd_zero, &mpd_ctx);
    mpd_copy(order->last_deal_price, mpd_zero, &mpd_ctx);

    if (fee_asset && fee_asset[0] != 0) {
        order->fee_asset = strdup(fee_asset);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0) {
            order_free(order);
            return -__LINE__;
        }

        mpd_copy(order->fee_price, fee_price, &mpd_ctx);
        if (fee_discount) {
            mpd_copy(order->fee_discount, fee_discount, &mpd_ctx);
        }
    }

    if (client_id && strlen(client_id) > 0) {
        order->client_id = strdup(client_id);
    }

    int ret;
    mpd_t *pre_last = mpd_qncopy(m->last);
    if (!m->call_auction) {
        if (side == MARKET_ORDER_SIDE_ASK) {
            ret = execute_limit_ask_order(real, m, order);
        } else {
            ret = execute_limit_bid_order(real, m, order);
        }
        if (ret < 0) {
            log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
            order_free(order);
            mpd_del(pre_last);
            return -__LINE__;
        }
    }

    ++order_id_start;
    bool immediate_or_cancel = (option & OPTION_IMMEDIATED_OR_CANCEL) ? true : false;
    if (mpd_cmp(order->left, mpd_zero, &mpd_ctx) == 0 || (immediate_or_cancel && mpd_cmp(order->amount, order->left, &mpd_ctx) > 0)) {
        if (real) {
            ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
            push_order_message(ORDER_EVENT_FINISH, order, m);
            if (result) {
                *result = get_order_info(order, false);
            }
        } else if (is_reader) {
            record_fini_order(order);
        }
        if (side == MARKET_ORDER_SIDE_ASK) {
            balance_reset(user_id, account, m->stock);
        } else {
            balance_reset(user_id, account, m->money);
        }
        order_free(order);
    } else if (immediate_or_cancel) {
        if (result) {
            *result = get_order_info(order, false);
        }
        if (is_reader) {
            record_fini_order(order);
        }
        order_free(order);
    } else {
        ret = frozen_order(m, order);
        if (ret < 0) {
            log_fatal("frozen_order fail: %d", ret);
            finish_order(real, m, order);
        } else {
            ret = put_order(m, order);
            if (ret < 0) {
                log_fatal("put_order fail: %d, order: %"PRIu64"", ret, order->id);
            } else if (real) {
                profile_inc("put_order", 1);
                push_order_message(ORDER_EVENT_PUT, order, m);
                if (result) {
                    *result = get_order_info(order, false);
                }
            }
        }
    }

    if (m->call_auction && ret == 0) {
        calc_call_auction_basic_price(m, false);
    }

    if (mpd_cmp(m->last, pre_last, &mpd_ctx) < 0) {
        check_stop_low(real, m);
    } else if (mpd_cmp(m->last, pre_last, &mpd_ctx) > 0) {
        check_stop_high(real, m);
    }
    mpd_del(pre_last);
    m->update_id++;
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

    uint32_t ask_fee_account = 0;
    uint32_t bid_fee_account = 0;

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    bool ask_use_stock_fee = (taker->option & OPTION_SUGGEST_STOCK_FEE) ? true : false;

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

        bool bid_use_money_fee = (maker->option & OPTION_SUGGEST_MONEY_FEE) ? true : false;

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = taker->account;
            if (strcmp(taker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (taker->account == fee_account && strcmp(taker->fee_asset, m->stock) == 0) {
                mpd_add(require, require, taker->left, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                ask_fee_asset = taker->fee_asset;
                ask_fee_account = fee_account;
                mpd_copy(ask_fee, result, &mpd_ctx);
                mpd_rescale(ask_fee, ask_fee, -asset_prec_save(ask_fee_account, ask_fee_asset), &mpd_ctx);
            }
            mpd_del(require);
        }

        if (ask_fee_asset == NULL && ask_use_stock_fee) {
            mpd_mul(result, amount, taker->taker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(taker->account, m->stock), &mpd_ctx);

            mpd_t *require = mpd_qncopy(result);
            mpd_add(require, require, taker->left, &mpd_ctx);
            mpd_t *fee_balance = balance_get(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->stock);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                ask_fee_asset = m->stock;
                ask_fee_account = taker->account;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }

        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            ask_fee_account = taker->account;
            mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
            mpd_rescale(ask_fee, ask_fee, -asset_prec_save(ask_fee_account, ask_fee_asset), &mpd_ctx);
        }

        // update taker
        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == taker->fee_asset) {
            mpd_add(taker->asset_fee, taker->asset_fee, ask_fee, &mpd_ctx);
        } else if (ask_fee_asset == m->money) {
            mpd_add(taker->money_fee, taker->money_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(taker->stock_fee, taker->stock_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(taker->user_id, ask_fee_account, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(taker, ask_fee_account, ask_fee_asset, ask_fee, price, amount, taker->taker_fee);
            }
        }

        // calculate bid fee
        get_fee_price(m, maker->fee_asset, maker->fee_price);
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = maker->account;
            if (strcmp(maker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                bid_fee_asset = maker->fee_asset;
                bid_fee_account = fee_account;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
        }

        if (bid_fee_asset == NULL && bid_use_money_fee) {
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(maker->account, m->money), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->money);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                bid_fee_asset = m->money;
                bid_fee_account = maker->account;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
            
        }

        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            bid_fee_account = maker->account;
            mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);
            mpd_rescale(bid_fee, bid_fee, -asset_prec_save(bid_fee_account, bid_fee_asset), &mpd_ctx);
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER,
                    price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
            push_deal_message(taker->update_time, deal_id, m, MARKET_TRADE_SIDE_SELL,
                    taker, maker, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        }

        // update maker
        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (bid_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, bid_fee, &mpd_ctx);
        } else if (bid_fee_asset == m->money) {
            mpd_add(maker->money_fee, maker->money_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(maker->stock_fee, maker->stock_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, maker->account, BALANCE_TYPE_FROZEN, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(maker->user_id, bid_fee_account, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(maker, bid_fee_account, bid_fee_asset, bid_fee, price, amount, maker->maker_fee);
            }
        }

        mpd_copy(taker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(taker->last_deal_price, price, &mpd_ctx);
        taker->last_deal_time = taker->update_time;
        taker->last_deal_id = deal_id;
        taker->last_role = MARKET_ROLE_TAKER;

        mpd_copy(maker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(maker->last_deal_price, price, &mpd_ctx);
        maker->last_deal_time = maker->update_time;
        maker->last_deal_id = deal_id;
        maker->last_role = MARKET_ROLE_MAKER;

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
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

    uint32_t ask_fee_account = 0;
    uint32_t bid_fee_account = 0;

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    bool bid_use_money_fee = (taker->option & OPTION_SUGGEST_MONEY_FEE) ? true : false;

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

        if (mpd_cmp(amount, maker->left, &mpd_ctx) > 0) {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }
        if (mpd_cmp(amount, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        bool ask_use_stock_fee = (maker->option & OPTION_SUGGEST_STOCK_FEE) ? true : false;

        // calculate ask fee
        mpd_mul(deal, price, amount, &mpd_ctx);
        get_fee_price(m, maker->fee_asset, maker->fee_price);
        if (maker->fee_asset != NULL && mpd_cmp(maker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = maker->account;
            if (strcmp(maker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, maker->maker_fee, &mpd_ctx);
            mpd_div(result, result, maker->fee_price, &mpd_ctx);
            mpd_mul(result, result, maker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, maker->fee_asset), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, maker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                ask_fee_asset = maker->fee_asset;
                ask_fee_account = fee_account;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
        }

        if (ask_fee_asset == NULL && ask_use_stock_fee) {
            mpd_mul(result, amount, maker->maker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(maker->account, m->stock), &mpd_ctx);
            mpd_t *fee_balance = balance_get(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->stock);
            if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
                ask_fee_asset = m->stock;
                ask_fee_account = maker->account;
                mpd_copy(ask_fee, result, &mpd_ctx);
            }
        }

        if (ask_fee_asset == NULL) {
            ask_fee_asset = m->money;
            ask_fee_account = maker->account;
            mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
            mpd_rescale(ask_fee, ask_fee, -asset_prec_save(ask_fee_account, ask_fee_asset), &mpd_ctx);
        }

        // update maker
        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->frozen, maker->frozen, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        if (ask_fee_asset == maker->fee_asset) {
            mpd_add(maker->asset_fee, maker->asset_fee, ask_fee, &mpd_ctx);
        } else if (ask_fee_asset == m->money) {
            mpd_add(maker->money_fee, maker->money_fee, ask_fee, &mpd_ctx);
        } else {
            mpd_add(maker->stock_fee, maker->stock_fee, ask_fee, &mpd_ctx);
        }

        balance_sub(maker->user_id, maker->account, BALANCE_TYPE_FROZEN, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, maker->account, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(maker->user_id, ask_fee_account, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(maker, ask_fee_account, ask_fee_asset, ask_fee, price, amount, maker->maker_fee);
            }
        }

        // calculate bid fee
        if (taker->fee_asset != NULL && mpd_cmp(taker->fee_price, mpd_zero, &mpd_ctx) > 0) {
            uint32_t fee_account = taker->account;
            if (strcmp(taker->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
                fee_account = 0;
            }
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_div(result, result, taker->fee_price, &mpd_ctx);
            mpd_mul(result, result, taker->fee_discount, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(fee_account, taker->fee_asset), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            if (taker->account == fee_account && strcmp(taker->fee_asset, m->money) == 0) {
                mpd_add(require, require, taker->left, &mpd_ctx);
            }
            mpd_t *fee_balance = balance_get(taker->user_id, fee_account, BALANCE_TYPE_AVAILABLE, taker->fee_asset);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                bid_fee_asset = taker->fee_asset;
                bid_fee_account = fee_account;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }

        if (bid_fee_asset == NULL && bid_use_money_fee) {
            mpd_mul(result, deal, taker->taker_fee, &mpd_ctx);
            mpd_rescale(result, result, -asset_prec_save(taker->account, m->money), &mpd_ctx);
            mpd_t *require = mpd_qncopy(result);
            mpd_add(require, require, taker->left, &mpd_ctx);
            mpd_t *fee_balance = balance_get(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->money);
            if (fee_balance && mpd_cmp(fee_balance, require, &mpd_ctx) >= 0) {
                bid_fee_asset = m->money;
                bid_fee_account = taker->account;
                mpd_copy(bid_fee, result, &mpd_ctx);
            }
            mpd_del(require);
        }

        if (bid_fee_asset == NULL) {
            bid_fee_asset = m->stock;
            bid_fee_account = taker->account;
            mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);
            mpd_rescale(bid_fee, bid_fee, -asset_prec_save(bid_fee_account, bid_fee_asset), &mpd_ctx);
        }

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER,
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
        } else if (bid_fee_asset == m->money) {
            mpd_add(taker->money_fee, taker->money_fee, bid_fee, &mpd_ctx);
        } else {
            mpd_add(taker->stock_fee, taker->stock_fee, bid_fee, &mpd_ctx);
        }

        balance_sub(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, taker->account, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            mpd_t *sub_result = balance_sub(taker->user_id, bid_fee_account, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
            if (real && sub_result != NULL) {
                append_balance_trade_fee(taker, bid_fee_account, bid_fee_asset, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_copy(taker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(taker->last_deal_price, price, &mpd_ctx);
        taker->last_deal_time = taker->update_time;
        taker->last_deal_id = deal_id;
        taker->last_role = MARKET_ROLE_TAKER;

        mpd_copy(maker->last_deal_amount, amount, &mpd_ctx);
        mpd_copy(maker->last_deal_price, price, &mpd_ctx);
        maker->last_deal_time = maker->update_time;
        maker->last_deal_id = deal_id;
        maker->last_role = MARKET_ROLE_MAKER;

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
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

static bool check_market_fill_or_kill(market_t *m, uint32_t side, mpd_t *amount)
{
    mpd_t *fill_amount = mpd_qncopy(mpd_zero);
    skiplist_node *node;
    skiplist_iter *iter;
    if (side == MARKET_ORDER_SIDE_ASK) {
        iter = skiplist_get_iterator(m->bids);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            mpd_add(fill_amount, fill_amount, order->left, &mpd_ctx);
            if (mpd_cmp(fill_amount, amount, &mpd_ctx) >= 0) {
                break;
            }
        }
    } else {
        mpd_t *deal_amount = mpd_qncopy(mpd_zero);
        iter = skiplist_get_iterator(m->asks);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            mpd_mul(deal_amount, order->price, order->left, &mpd_ctx);
            mpd_add(fill_amount, fill_amount, deal_amount, &mpd_ctx);
            if (mpd_cmp(fill_amount, amount, &mpd_ctx) >= 0) {
                break;
            }
        }
        mpd_del(deal_amount);
    }
    skiplist_release_iterator(iter);

    bool ret = false;
    if (mpd_cmp(fill_amount, amount, &mpd_ctx) >= 0) {
        ret = true;
    }
    mpd_del(fill_amount);

    return ret;
}

int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t account, uint32_t side, mpd_t *amount,
        mpd_t *taker_fee, const char *source, const char *fee_asset, mpd_t *fee_price, mpd_t *fee_discount, uint32_t option, const char *client_id)
{
    bool unlimited_min = (option & OPTION_UNLIMITED_MIN_AMOUNT) ? true: false;
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, account, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
        if (real && !unlimited_min && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
            return -2;
        }
        if (skiplist_len(m->bids) == 0) {
            return -3;
        }
    } else {
        mpd_t *balance = balance_get(user_id, account, BALANCE_TYPE_AVAILABLE, m->money);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        if (skiplist_len(m->asks) == 0) {
            return -3;
        }

        if (real && !unlimited_min) {
            skiplist_node *node = skiplist_header(m->asks);
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

    bool fill_or_kill = (option & OPTION_FILL_OR_KILL) ? true : false;
    if (real && fill_or_kill && !check_market_fill_or_kill(m, side, amount)) {
        return -4;
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL)
        return -__LINE__;
    memset(order, 0, sizeof(order_t));

    order->id           = order_id_start + 1;
    order->type         = MARKET_ORDER_TYPE_MARKET;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->account      = account;
    order->option       = option;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->frozen       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->money_fee     = mpd_new(&mpd_ctx);
    order->stock_fee     = mpd_new(&mpd_ctx);
    order->asset_fee    = mpd_new(&mpd_ctx);
    order->fee_price    = mpd_new(&mpd_ctx);
    order->fee_discount = mpd_new(&mpd_ctx);
    order->last_deal_amount = mpd_new(&mpd_ctx);
    order->last_deal_price  = mpd_new(&mpd_ctx);

    mpd_copy(order->price, mpd_zero, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->frozen, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->money_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->stock_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->asset_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee_price, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee_discount, mpd_one, &mpd_ctx);
    mpd_copy(order->last_deal_amount, mpd_zero, &mpd_ctx);
    mpd_copy(order->last_deal_price, mpd_zero, &mpd_ctx);

    if (fee_asset && fee_asset[0] != 0) {
        order->fee_asset = strdup(fee_asset);
        mpd_copy(order->fee_price, fee_price, &mpd_ctx);
        if (fee_discount) {
            mpd_copy(order->fee_discount, fee_discount, &mpd_ctx);
        }
    }

    if (client_id && strlen(client_id) > 0) {
        order->client_id = strdup(client_id);
    }

    int ret;
    mpd_t *pre_last = mpd_qncopy(m->last);
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
        balance_reset(user_id, account, m->stock);
    } else {
        ret = execute_market_bid_order(real, m, order);
        balance_reset(user_id, account, m->money);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        mpd_del(pre_last);
        return -__LINE__;
    }

    ++order_id_start;

    if (real) {
        if (mpd_cmp(order->amount, order->left, &mpd_ctx) > 0) {
            int ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
        }
        push_order_message(ORDER_EVENT_FINISH, order, m);
        if (result) {
            *result = get_order_info(order, false);
        }
    } else if (is_reader) {
        record_fini_order(order);
    }

    order_free(order);

    if (mpd_cmp(m->last, pre_last, &mpd_ctx) < 0) {
        check_stop_low(real, m);
    } else if (mpd_cmp(m->last, pre_last, &mpd_ctx) > 0) {
        check_stop_high(real, m);
    }
    mpd_del(pre_last);
    m->update_id++;
    return 0;
}

int market_put_stop_limit(bool real, market_t *m, uint32_t user_id, uint32_t account, uint32_t side, mpd_t *amount, mpd_t *stop_price, mpd_t *price,
        mpd_t *taker_fee, mpd_t *maker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount, uint32_t option, const char *client_id)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        if (mpd_cmp(m->last, mpd_zero, &mpd_ctx) == 0 || mpd_cmp(stop_price, m->last, &mpd_ctx) == 0) {
            return -1;
        }
    } else {
        if (mpd_cmp(m->last, mpd_zero, &mpd_ctx) == 0 || mpd_cmp(stop_price, m->last, &mpd_ctx) == 0) {
            return -1;
        }
    }

    bool unlimited_min = (option & OPTION_UNLIMITED_MIN_AMOUNT) ? true : false;
    if (real && !unlimited_min && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
        return -2;
    }

    option |= (OPTION_STOP_ORDER | OPTION_UNLIMITED_MIN_AMOUNT);
    stop_t *stop = malloc(sizeof(stop_t));
    if (stop == NULL)
        return -__LINE__;
    memset(stop, 0, sizeof(stop_t));

    stop->id            = order_id_start + 1;
    stop->type          = MARKET_ORDER_TYPE_LIMIT;
    stop->side          = side;
    stop->create_time   = current_timestamp();
    stop->update_time   = stop->create_time;
    stop->user_id       = user_id;
    stop->account       = account;
    stop->option        = option;
    stop->market        = strdup(m->name);
    stop->source        = strdup(source);
    stop->fee_discount  = mpd_new(&mpd_ctx);
    stop->stop_price    = mpd_new(&mpd_ctx);
    stop->price         = mpd_new(&mpd_ctx);
    stop->amount        = mpd_new(&mpd_ctx);
    stop->taker_fee     = mpd_new(&mpd_ctx);
    stop->maker_fee     = mpd_new(&mpd_ctx);

    if (mpd_cmp(stop_price, m->last, &mpd_ctx) > 0) {
        stop->state = STOP_STATE_HIGH;
    } else {
        stop->state = STOP_STATE_LOW;
    }

    mpd_copy(stop->stop_price, stop_price, &mpd_ctx);
    mpd_copy(stop->price, price, &mpd_ctx);
    mpd_copy(stop->amount, amount, &mpd_ctx);
    mpd_copy(stop->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(stop->maker_fee, maker_fee, &mpd_ctx);
    mpd_copy(stop->fee_discount, mpd_one, &mpd_ctx);
    if (fee_asset && strlen(fee_asset) > 0) {
        stop->fee_asset = strdup(fee_asset);
        if (fee_discount) {
            mpd_copy(stop->fee_discount, fee_discount, &mpd_ctx);
        }
    }

    if (client_id && strlen(client_id) > 0) {
        stop->client_id = strdup(client_id);
    }

    ++order_id_start;

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

int market_put_stop_market(bool real, market_t *m, uint32_t user_id, uint32_t account, uint32_t side, mpd_t *amount, mpd_t *stop_price,
        mpd_t *taker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount, uint32_t option, const char *client_id)
{
    bool unlimited_min = (option & OPTION_UNLIMITED_MIN_AMOUNT) ? true : false;
    if (side == MARKET_ORDER_SIDE_ASK) {
        if (mpd_cmp(m->last, mpd_zero, &mpd_ctx) == 0 || mpd_cmp(stop_price, m->last, &mpd_ctx) == 0) {
            return -1;
        }
        if (real && !unlimited_min && mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
            return -2;
        }
    } else {
        if (mpd_cmp(m->last, mpd_zero, &mpd_ctx) == 0 || mpd_cmp(stop_price, m->last, &mpd_ctx) == 0) {
            return -1;
        }

        if (real && !unlimited_min) {
            mpd_t *require = mpd_new(&mpd_ctx);
            mpd_mul(require, stop_price, m->min_amount, &mpd_ctx);
            if (mpd_cmp(amount, require, &mpd_ctx) < 0) {
                mpd_del(require);
                return -2;
            }
            mpd_del(require);
        }
    }

    stop_t *stop = malloc(sizeof(stop_t));
    if (stop == NULL)
        return -__LINE__;
    memset(stop, 0, sizeof(stop_t));
    option |= (OPTION_STOP_ORDER | OPTION_UNLIMITED_MIN_AMOUNT);
    stop->id            = order_id_start + 1;
    stop->type          = MARKET_ORDER_TYPE_MARKET;
    stop->side          = side;
    stop->create_time   = current_timestamp();
    stop->update_time   = stop->create_time;
    stop->user_id       = user_id;
    stop->account       = account;
    stop->option        = option;
    stop->market        = strdup(m->name);
    stop->source        = strdup(source);
    stop->fee_discount  = mpd_new(&mpd_ctx);
    stop->stop_price    = mpd_new(&mpd_ctx);
    stop->price         = mpd_new(&mpd_ctx);
    stop->amount        = mpd_new(&mpd_ctx);
    stop->taker_fee     = mpd_new(&mpd_ctx);
    stop->maker_fee     = mpd_new(&mpd_ctx);

    if (mpd_cmp(stop_price, m->last, &mpd_ctx) > 0) {
        stop->state = STOP_STATE_HIGH;
    } else {
        stop->state = STOP_STATE_LOW;
    }

    mpd_copy(stop->stop_price, stop_price, &mpd_ctx);
    mpd_copy(stop->price, mpd_zero, &mpd_ctx);
    mpd_copy(stop->amount, amount, &mpd_ctx);
    mpd_copy(stop->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(stop->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(stop->fee_discount, mpd_one, &mpd_ctx);
    if (fee_asset && strlen(fee_asset) > 0) {
        stop->fee_asset = strdup(fee_asset);
        if (fee_discount) {
            mpd_copy(stop->fee_discount, fee_discount, &mpd_ctx);
        }
    }

    if (client_id && strlen(client_id) > 0) {
        stop->client_id = strdup(client_id);
    }

    ++order_id_start;

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

static mpd_t *get_best_price(skiplist_t *list)
{
    skiplist_iter *iter = skiplist_get_iterator(list);
    if (iter == NULL)
        return NULL;
    skiplist_node *node = skiplist_next(iter);
    skiplist_release_iterator(iter);
    if (node == NULL)
        return NULL;
    order_t *order = node->value;
    return order->price;
}

int market_self_deal(bool real, market_t *market, mpd_t *amount, mpd_t *price, uint32_t side)
{
    mpd_t *ask_price_1 = get_best_price(market->asks);
    mpd_t *bid_price_1 = get_best_price(market->bids);

    mpd_t *deal_min_gear = mpd_new(&mpd_ctx);
    mpd_set_i32(deal_min_gear, -market->money_prec, &mpd_ctx);
    mpd_pow(deal_min_gear, mpd_ten, deal_min_gear, &mpd_ctx);

    if (ask_price_1 != NULL && bid_price_1 != NULL) {
        mpd_t *ask_bid_sub = mpd_new(&mpd_ctx);
        mpd_sub(ask_bid_sub, ask_price_1, bid_price_1, &mpd_ctx);
        if (mpd_cmp(deal_min_gear, ask_bid_sub, &mpd_ctx) == 0) {
            mpd_del(ask_bid_sub);
            mpd_del(deal_min_gear);
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
        push_deal_message(update_time, deal_id, market, side, order, order, real_price, amount, deal, market->money, mpd_zero, market->stock, mpd_zero);
        free(order);
    }

    mpd_t *pre_last = mpd_qncopy(market->last);
    mpd_copy(market->last, real_price, &mpd_ctx);
    if (mpd_cmp(market->last, pre_last, &mpd_ctx) < 0) {
        check_stop_low(real, market);
    } else if (mpd_cmp(market->last, pre_last, &mpd_ctx) > 0) {
        check_stop_high(real, market);
    }
    
    mpd_del(pre_last);
    mpd_del(deal);
    mpd_del(real_price);
    mpd_del(deal_min_gear);

    return 0;
}

int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)
{
    if (real) {
        *result = get_order_info(order, false);
    }
    int ret = finish_order(real, m, order);
    if (ret == 0 && m->call_auction) {
        calc_call_auction_basic_price(m, false);
    }
    m->update_id++;
    return ret;
}

int market_cancel_order_all(bool real, uint32_t user_id, int32_t account, market_t *m, uint32_t side)
{
    int ret = 0;
    skiplist_t *order_list = get_user_order_list(m, user_id, account);
    if (order_list == NULL)
        return ret;

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(order_list);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        if (side == 0 || side == order->side) {
            ret = finish_order(real, m, order);
            if (ret < 0) {
                log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
                skiplist_release_iterator(iter);
                return ret;
            }
        }
    }
    skiplist_release_iterator(iter);
    if (m->call_auction) {
        calc_call_auction_basic_price(m, false);
    }
    m->update_id++;
    return ret;
}

int market_cancel_stop(bool real, json_t **result, market_t *m, stop_t *stop)
{
    if (real) {
        push_stop_message(STOP_EVENT_CANCEL, stop, m, 0);
        *result = get_stop_info(stop);
    }
    return finish_stop(real, m, stop, MARKET_STOP_STATUS_CANCEL);
}

int market_cancel_stop_all(bool real, uint32_t user_id, int32_t account, market_t *m, uint32_t side)
{
    int ret = 0;
    skiplist_t *stop_list = get_user_stop_list(m, user_id, account);
    if( stop_list == NULL)
        return ret;

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(stop_list);
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;
        if (side == 0 || side == stop->side) {
            if (real) {
                push_stop_message(STOP_EVENT_CANCEL, stop, m, 0);
            }

            ret = finish_stop(real, m, stop, MARKET_STOP_STATUS_CANCEL);
            if (ret < 0) {
                log_fatal("cancel stop: %"PRIu64" fail: %d", stop->id, ret);
                skiplist_release_iterator(iter);
                return ret;
            }
        }
    }
    skiplist_release_iterator(iter);

    return ret;
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
    uint64_t key = order_id;
    dict_entry *entry = dict_find(m->orders, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

stop_t *market_get_stop(market_t *m, uint64_t order_id)
{
    uint64_t key = order_id;
    dict_entry *entry = dict_find(m->stops, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

skiplist_t *get_user_order_list(market_t *m, uint32_t user_id, int account)
{
    if (m == NULL) {
        return user_order_list_query(dict_user_orders, user_id, account);
    } else {
        return user_order_list_query(m->user_orders, user_id, account);
    }
}

skiplist_t *get_user_stop_list(market_t *m, uint32_t user_id, int account)
{
    if (m == NULL) {
        return user_order_list_query(dict_user_stops, user_id, account);
    } else {
        return user_order_list_query(m->user_stops, user_id, account);
    }
}

sds market_status(sds reply)
{
    reply = sdscatprintf(reply, "total order user: %u\n", dict_size(dict_user_orders));
    reply = sdscatprintf(reply, "total stop user: %u\n", dict_size(dict_user_stops));
    reply = sdscatprintf(reply, "order last ID: %"PRIu64"\n", order_id_start);
    reply = sdscatprintf(reply, "deals last ID: %"PRIu64"\n", deals_id_start);
    return reply;
}

json_t *market_get_fini_order(uint64_t order_id)
{
    if (!is_reader)
        return NULL;

    uint64_t order_key = order_id;
    dict_entry *entry = dict_find(dict_fini_orders, &order_key);
    if (entry) {
        json_t *order = entry->val;
        json_incref(order);
        return order;
    }

    return NULL;
}

json_t *market_get_summary(market_t *m)
{
    int order_ask_users, order_bid_users, stop_ask_users, stop_bid_users;
    mpd_t *order_ask_amount = mpd_qncopy(mpd_zero);
    mpd_t *order_bid_amount = mpd_qncopy(mpd_zero);
    mpd_t *order_ask_left = mpd_qncopy(mpd_zero);
    mpd_t *order_bid_left = mpd_qncopy(mpd_zero);
    mpd_t *stop_ask_amount = mpd_qncopy(mpd_zero);
    mpd_t *stop_bid_amount = mpd_qncopy(mpd_zero);

    //ask orders
    dict_t *distinct_dict = uint32_set_create();
    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(order_ask_amount, order_ask_amount, order->amount, &mpd_ctx);
        mpd_add(order_ask_left, order_ask_left, order->left, &mpd_ctx);
        uint32_set_add(distinct_dict, order->user_id);
    }
    order_ask_users = uint32_set_num(distinct_dict);
    uint32_set_clear(distinct_dict);
    skiplist_release_iterator(iter);

    //bid orders
    iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(order_bid_amount, order_bid_amount, order->amount, &mpd_ctx);
        mpd_add(order_bid_left, order_bid_left, order->left, &mpd_ctx);
        uint32_set_add(distinct_dict, order->user_id);
    }
    order_bid_users = uint32_set_num(distinct_dict);
    skiplist_release_iterator(iter);
    uint32_set_release(distinct_dict);

    uint64_t stop_asks_num = 0, stop_bids_num = 0;
    dict_t *distinct_stop_ask_dict = uint32_set_create();
    dict_t *distinct_stop_bid_dict = uint32_set_create();

    //high stops
    iter = skiplist_get_iterator(m->stop_high);
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;
        if (stop->side == MARKET_ORDER_SIDE_ASK) {
            stop_asks_num++;
            mpd_add(stop_ask_amount, stop_ask_amount, stop->amount, &mpd_ctx);
            uint32_set_add(distinct_stop_ask_dict, stop->user_id);
        } else {
            stop_bids_num++;
            mpd_add(stop_bid_amount, stop_bid_amount, stop->amount, &mpd_ctx);
            uint32_set_add(distinct_stop_bid_dict, stop->user_id);
        }
    }
    skiplist_release_iterator(iter);

    //low stops
    iter = skiplist_get_iterator(m->stop_low);
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;
        if (stop->side == MARKET_ORDER_SIDE_ASK) {
            stop_asks_num++;
            mpd_add(stop_ask_amount, stop_ask_amount, stop->amount, &mpd_ctx);
            uint32_set_add(distinct_stop_ask_dict, stop->user_id);
        } else {
            stop_bids_num++;
            mpd_add(stop_bid_amount, stop_bid_amount, stop->amount, &mpd_ctx);
            uint32_set_add(distinct_stop_bid_dict, stop->user_id);
        }
    }
    skiplist_release_iterator(iter);

    stop_ask_users = uint32_set_num(distinct_stop_ask_dict);
    stop_bid_users = uint32_set_num(distinct_stop_bid_dict);
    uint32_set_release(distinct_stop_ask_dict);
    uint32_set_release(distinct_stop_bid_dict);

    json_t *result = json_object();
    json_object_set_new(result, "order_users", json_integer(dict_size(m->user_orders)));
    json_object_set_new(result, "order_ask_users", json_integer(order_ask_users));
    json_object_set_new(result, "order_bid_users", json_integer(order_bid_users));

    json_object_set_new(result, "stop_users", json_integer(dict_size(m->user_stops)));
    json_object_set_new(result, "stop_ask_users", json_integer(stop_ask_users));
    json_object_set_new(result, "stop_bid_users", json_integer(stop_bid_users));

    json_object_set_new(result, "orders", json_integer(dict_size(m->orders)));
    json_object_set_new(result, "stops", json_integer(dict_size(m->stops)));

    json_object_set_new(result, "order_asks", json_integer(skiplist_len(m->asks)));
    json_object_set_new_mpd(result, "order_ask_amount", order_ask_amount);
    json_object_set_new_mpd(result, "order_ask_left", order_ask_left);

    json_object_set_new(result, "order_bids", json_integer(skiplist_len(m->bids)));
    json_object_set_new_mpd(result, "order_bid_amount", order_bid_amount);
    json_object_set_new_mpd(result, "order_bid_left", order_bid_left);

    json_object_set_new(result, "stop_asks", json_integer(stop_asks_num));
    json_object_set_new_mpd(result, "stop_ask_amount", stop_ask_amount);

    json_object_set_new(result, "stop_bids", json_integer(stop_bids_num));
    json_object_set_new_mpd(result, "stop_bid_amount", stop_bid_amount);

    json_object_set_new(result, "stop_high", json_integer(skiplist_len(m->stop_high)));
    json_object_set_new(result, "stop_low", json_integer(skiplist_len(m->stop_low)));

    mpd_del(order_ask_amount);
    mpd_del(order_bid_amount);
    mpd_del(order_ask_left);
    mpd_del(order_bid_left);
    mpd_del(stop_ask_amount);
    mpd_del(stop_bid_amount);
    return result;
}

int market_set_reader()
{
    is_reader = true;

    dict_types types_order;
    memset(&types_order, 0, sizeof(types_order));
    types_order.hash_function  = uint64_dict_key_hash_func;
    types_order.key_compare    = uint64_dict_key_compare;
    types_order.key_dup        = uint64_dict_key_dup;
    types_order.key_destructor = uint64_dict_key_free;
    types_order.val_destructor = dict_order_value_free;

    dict_fini_orders = dict_create(&types_order, 1024);
    if (dict_fini_orders == NULL) {
        return -__LINE__;
    }

    nw_timer_set(&timer_fini_order, 0.5, true, on_timer_fini_order, NULL);
    nw_timer_start(&timer_fini_order);

    return 0;
}

int execute_ask_bid_order_with_price(bool real, market_t *m, order_t *ask, order_t *bid, mpd_t* price, mpd_t* amount, uint64_t taker_id)
{
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    mpd_t *ask_role_trade_fee = mpd_new(&mpd_ctx);
    mpd_t *bid_role_trade_fee = mpd_new(&mpd_ctx);

    int ask_role = 0;
    int bid_role = 0;
    int market_side = 0;
    if (ask->id == taker_id) {
        ask_role = MARKET_ROLE_TAKER;
        bid_role = MARKET_ROLE_MAKER;
        market_side = MARKET_TRADE_SIDE_SELL;
        mpd_copy(ask_role_trade_fee, ask->taker_fee, &mpd_ctx);
        mpd_copy(bid_role_trade_fee, bid->maker_fee, &mpd_ctx);
    } else {
        ask_role = MARKET_ROLE_MAKER;
        bid_role = MARKET_ROLE_TAKER;
        market_side = MARKET_TRADE_SIDE_BUY;
        mpd_copy(ask_role_trade_fee, ask->maker_fee, &mpd_ctx);
        mpd_copy(bid_role_trade_fee, bid->taker_fee, &mpd_ctx);
    }

    uint32_t ask_fee_account = 0;
    uint32_t bid_fee_account = 0;

    const char *ask_fee_asset = NULL;
    const char *bid_fee_asset = NULL;

    bool ask_use_stock_fee = (ask->option & OPTION_SUGGEST_STOCK_FEE) ? true : false;
    bool bid_use_money_fee = (bid->option & OPTION_SUGGEST_MONEY_FEE) ? true : false;

    mpd_mul(deal, price, amount, &mpd_ctx);
    get_fee_price(m, ask->fee_asset, ask->fee_price);
    if (ask->fee_asset != NULL && mpd_cmp(ask->fee_price, mpd_zero, &mpd_ctx) > 0) {
        uint32_t fee_account = ask->account;
        if (strcmp(ask->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
            fee_account = 0;
        }
        mpd_mul(result, deal, ask_role_trade_fee, &mpd_ctx);
        mpd_div(result, result, ask->fee_price, &mpd_ctx);
        mpd_mul(result, result, ask->fee_discount, &mpd_ctx);
        mpd_rescale(result, result, -asset_prec_save(fee_account, ask->fee_asset), &mpd_ctx);
        mpd_t *fee_balance = balance_get(ask->user_id, fee_account, BALANCE_TYPE_AVAILABLE, ask->fee_asset);
        if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
            ask_fee_asset = ask->fee_asset;
            ask_fee_account = fee_account;
            mpd_copy(ask_fee, result, &mpd_ctx);
        }
    }

    if (ask_fee_asset == NULL && ask_use_stock_fee) {
        mpd_mul(result, amount, ask_role_trade_fee, &mpd_ctx);
        mpd_rescale(result, result, -asset_prec_save(ask->account, m->stock), &mpd_ctx);
        mpd_t *fee_balance = balance_get(ask->user_id, ask->account, BALANCE_TYPE_AVAILABLE, m->stock);
        if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
            ask_fee_asset = m->stock;
            ask_fee_account = ask->account;
            mpd_copy(ask_fee, result, &mpd_ctx);
        }
    }

    if (ask_fee_asset == NULL) {
        ask_fee_asset = m->money;
        ask_fee_account = ask->account;
        mpd_mul(ask_fee, deal, ask_role_trade_fee, &mpd_ctx);
        mpd_rescale(ask_fee, ask_fee, -asset_prec_save(ask_fee_account, ask_fee_asset), &mpd_ctx);
    }

    uint64_t deal_id = ++deals_id_start;
    mpd_sub(ask->left, ask->left, amount, &mpd_ctx);
    mpd_sub(ask->frozen, ask->frozen, amount, &mpd_ctx);
    mpd_add(ask->deal_stock, ask->deal_stock, amount, &mpd_ctx);
    mpd_add(ask->deal_money, ask->deal_money, deal, &mpd_ctx);
    ask->last_deal_time = ask->update_time;
    ask->last_deal_id = deal_id;
    ask->last_role = ask_role;
    if (ask_fee_asset == ask->fee_asset) {
        mpd_add(ask->asset_fee, ask->asset_fee, ask_fee, &mpd_ctx);
    } else if (ask_fee_asset == m->money) {
        mpd_add(ask->money_fee, ask->money_fee, ask_fee, &mpd_ctx);
    } else {
        mpd_add(ask->stock_fee, ask->stock_fee, ask_fee, &mpd_ctx);
    }

    balance_sub(ask->user_id, ask->account, BALANCE_TYPE_FROZEN, m->stock, amount);
    if (real) {
        append_balance_trade_sub(ask, m->stock, amount, price, amount);
    }

    balance_add(ask->user_id, ask->account, BALANCE_TYPE_AVAILABLE, m->money, deal);
    if (real) {
        append_balance_trade_add(ask, m->money, deal, price, amount);
    }

    if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
        mpd_t *sub_result = balance_sub(ask->user_id, ask_fee_account, BALANCE_TYPE_AVAILABLE, ask_fee_asset, ask_fee);
        if (real && sub_result != NULL) {
            append_balance_trade_fee(ask, ask_fee_account, ask_fee_asset, ask_fee, price, amount, ask_role_trade_fee);
        }
    }

    get_fee_price(m, bid->fee_asset, bid->fee_price);
    if (bid->fee_asset != NULL && mpd_cmp(bid->fee_price, mpd_zero, &mpd_ctx) > 0) {
        uint32_t fee_account = bid->account;
        if (strcmp(bid->fee_asset, SYSTEM_FEE_TOKEN) == 0) {
            fee_account = 0;
        }
        mpd_mul(result, deal, bid_role_trade_fee, &mpd_ctx);
        mpd_div(result, result, bid->fee_price, &mpd_ctx);
        mpd_mul(result, result, bid->fee_discount, &mpd_ctx);
        mpd_rescale(result, result, -asset_prec_save(fee_account, bid->fee_asset), &mpd_ctx);
        mpd_t *fee_balance = balance_get(bid->user_id, fee_account, BALANCE_TYPE_AVAILABLE, bid->fee_asset);
        if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
            bid_fee_asset = bid->fee_asset;
            bid_fee_account = fee_account;
            mpd_copy(bid_fee, result, &mpd_ctx);
        }
    }

    if (bid_fee_asset == NULL && bid_use_money_fee) {
        mpd_mul(result, deal, bid_role_trade_fee, &mpd_ctx);
        mpd_rescale(result, result, -asset_prec_save(bid->account, m->money), &mpd_ctx);
        mpd_t *fee_balance = balance_get(bid->user_id, bid->account, BALANCE_TYPE_AVAILABLE, m->money);
        if (fee_balance && mpd_cmp(fee_balance, result, &mpd_ctx) >= 0) {
            bid_fee_asset = m->money;
            bid_fee_account = bid->account;
            mpd_copy(bid_fee, result, &mpd_ctx);
        }
    }

    if (bid_fee_asset == NULL) {
        bid_fee_asset = m->stock;
        bid_fee_account = bid->account;
        mpd_mul(bid_fee, amount, bid_role_trade_fee, &mpd_ctx);
        mpd_rescale(bid_fee, bid_fee, -asset_prec_save(bid_fee_account, bid_fee_asset), &mpd_ctx);
    }

    ask->update_time = bid->update_time = current_timestamp();
    if (real) {
        append_deal_history(ask->update_time, deal_id, ask, ask_role, bid, bid_role,
            price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        push_deal_message(ask->update_time, deal_id, m, market_side,
            ask, bid, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
    }

    mpd_sub(bid->left, bid->left, amount, &mpd_ctx);
    mpd_sub(bid->frozen, bid->frozen, deal, &mpd_ctx);
    mpd_add(bid->deal_stock, bid->deal_stock, amount, &mpd_ctx);
    mpd_add(bid->deal_money, bid->deal_money, deal, &mpd_ctx);
    bid->last_deal_time = bid->update_time;
    bid->last_deal_id = deal_id;
    bid->last_role = bid_role;
    if (bid_fee_asset == bid->fee_asset) {
        mpd_add(bid->asset_fee, bid->asset_fee, bid_fee, &mpd_ctx);
    } else if (bid_fee_asset == m->money) {
        mpd_add(bid->money_fee, bid->money_fee, bid_fee, &mpd_ctx);
    } else {
        mpd_add(bid->stock_fee, bid->stock_fee, bid_fee, &mpd_ctx);
    }

    balance_sub(bid->user_id, bid->account, BALANCE_TYPE_FROZEN, m->money, deal);
    if (real) {
        append_balance_trade_sub(bid, m->money, deal, price, amount);
    }
    balance_add(bid->user_id, bid->account, BALANCE_TYPE_AVAILABLE, m->stock, amount);
    if (real) {
        append_balance_trade_add(bid, m->stock, amount, price, amount);
    }

    if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
        mpd_t *sub_result = balance_sub(bid->user_id, bid_fee_account, BALANCE_TYPE_AVAILABLE, bid_fee_asset, bid_fee);
        if (real && sub_result != NULL) {
            append_balance_trade_fee(bid, bid_fee_account, bid_fee_asset, bid_fee, price, amount, bid_role_trade_fee);
        }
    }

    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);
    mpd_del(ask_role_trade_fee);
    mpd_del(bid_role_trade_fee);

    return 0;
}

int market_start_call_auction(market_t *m)
{
    m->call_auction = true;
    return 0;
}

int market_execute_call_auction(bool real, market_t *m, mpd_t *volume)
{
    m->call_auction = false;
    int ret = calc_call_auction_basic_price(m, true);
    if (ret != 0) {
        return ret;
    }

    skiplist_iter *ask_iter = skiplist_get_iterator(m->asks);
    skiplist_iter *bid_iter = skiplist_get_iterator(m->bids);
    skiplist_node *ask_node = skiplist_next(ask_iter);
    skiplist_node *bid_node = skiplist_next(bid_iter);
    mpd_t *deal_amount = mpd_qncopy(mpd_zero);
    mpd_t *deal_volume = mpd_qncopy(mpd_zero);
    order_t *ask_order;
    order_t *bid_order;

    while (ask_node != NULL && bid_node != NULL) {
        ask_order = ask_node->value;
        bid_order = bid_node->value;
        if (mpd_cmp(ask_order->price, m->last, &mpd_ctx) > 0 || mpd_cmp(bid_order->price, m->last, &mpd_ctx) < 0) {
            break;
        }

        if (mpd_cmp(ask_order->left, bid_order->left, &mpd_ctx) > 0) {
            mpd_copy(deal_amount, bid_order->left, &mpd_ctx);
            bid_node = skiplist_next(bid_iter);
        } else if (mpd_cmp(ask_order->left, bid_order->left, &mpd_ctx) < 0) {
            mpd_copy(deal_amount, ask_order->left, &mpd_ctx);
            ask_node = skiplist_next(ask_iter);
        } else {
            mpd_copy(deal_amount, ask_order->left, &mpd_ctx);
            bid_node = skiplist_next(bid_iter);
            ask_node = skiplist_next(ask_iter);
        }

        uint64_t taker_id = 0;
        if (ask_order->create_time >= bid_order->create_time) {
            taker_id = ask_order->id;
        } else {
            taker_id = bid_order->id;
        }
        execute_ask_bid_order_with_price(real, m, ask_order, bid_order, m->last, deal_amount, taker_id);
        if (mpd_cmp(ask_order->left, mpd_zero, &mpd_ctx) == 0) {
            mpd_copy(ask_order->last_deal_amount, ask_order->amount, &mpd_ctx);
            mpd_copy(ask_order->last_deal_price, m->last, &mpd_ctx);
            finish_order(real, m, ask_order);
        }

        if (mpd_cmp(bid_order->left, mpd_zero, &mpd_ctx) == 0) {
            mpd_copy(bid_order->last_deal_amount, bid_order->amount, &mpd_ctx);
            mpd_copy(bid_order->last_deal_price, m->last, &mpd_ctx);
            finish_order(real, m, bid_order);
        }
        mpd_add(deal_volume, deal_volume, deal_amount, &mpd_ctx);
    }

    if (real) {
        mpd_copy(volume, deal_volume, &mpd_ctx);
    }
    mpd_del(deal_amount);
    mpd_del(deal_volume);
    skiplist_release_iterator(ask_iter);
    skiplist_release_iterator(bid_iter);
    m->update_id++;
    return ret;
}

bool check_fee_rate(const mpd_t *fee)
{
    if (fee == NULL)
        return false;
    if (mpd_cmp(fee, settings.min_fee, &mpd_ctx) < 0)
        return false;
    if (mpd_cmp(fee, settings.max_fee, &mpd_ctx) > 0)
        return false;
    return true;
}
