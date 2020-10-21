/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "ts_config.h"
# include "ts_market.h"
# include "ts_message.h"

static nw_timer dump_timer;
static nw_timer clear_timer;
static nw_timer report_timer;

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_orders;

static int64_t kafka_deals_offset = 0;
static int64_t kafka_orders_offset = 0;

static time_t  last_dump_date = 0;
static time_t  last_dump_time = 0;

static dict_t *dict_market_info;

struct market_info_val {
    dict_t *daily_trade;
    dict_t *users_detail;
};

struct fee_key {
    uint32_t user_id;
    char asset[ASSET_NAME_MAX_LEN + 1];
};

struct client_fee_key {
    uint32_t user_id;
    char client_id[CLIENT_ID_MAX_LEN + 1];
    char asset[ASSET_NAME_MAX_LEN + 1];
};

struct fee_val {
    mpd_t *value;
    mpd_t *taker_fee;
    mpd_t *maker_fee;
};

struct daily_trade_val {
    dict_t  *users_trade;
    dict_t  *fees_detail;

    dict_t  *client_trade;
    dict_t  *client_fees_detail;

    mpd_t   *deal_amount;
    mpd_t   *deal_volume;
    mpd_t   *taker_buy_amount;
    mpd_t   *taker_sell_amount;

    int     deal_count;
    int     taker_buy_count;
    int     taker_sell_count;
    int     limit_buy_order;
    int     limit_sell_order;
    int     market_buy_order;
    int     market_sell_order;
};

struct client_user_trade_key {
    uint32_t user_id;
    char client_id[CLIENT_ID_MAX_LEN + 1];
};

struct users_trade_val {
    mpd_t   *deal_amount;
    mpd_t   *deal_volume;
    mpd_t   *buy_amount;
    mpd_t   *buy_volume;
    mpd_t   *sell_amount;
    mpd_t   *sell_volume;
    mpd_t   *taker_amount;
    mpd_t   *taker_volume;
    mpd_t   *maker_amount;
    mpd_t   *maker_volume;

    int     deal_count;
    int     deal_buy_count;
    int     deal_sell_count;
    int     limit_buy_order;
    int     limit_sell_order;
    int     market_buy_order;
    int     market_sell_order;
};

struct user_detail_val {
    mpd_t   *buy_amount;
    mpd_t   *sell_amount;
    mpd_t   *buy_volume;
    mpd_t   *sell_volume;
};

struct trade_net_rank_val {
    uint32_t user_id;
    mpd_t    *amount;
    mpd_t    *amount_net;
};

struct trade_amount_rank_val {
    uint32_t user_id;
    mpd_t    *amount;
    mpd_t    *amount_total;
};

// fee key
static uint32_t dict_fee_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct fee_key));
}

static int dict_fee_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct fee_key));
}

static void *dict_fee_key_dup(const void *key)
{
    struct fee_key *obj = malloc(sizeof(struct fee_key));
    memcpy(obj, key, sizeof(struct fee_key));
    return obj;
}

static void dict_fee_key_free(void *key)
{
    free(key);
}

// client fee key
static uint32_t dict_client_fee_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct client_fee_key));
}

static int dict_client_fee_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct client_fee_key));
}

static void *dict_client_fee_key_dup(const void *key)
{
    struct client_fee_key *obj = malloc(sizeof(struct client_fee_key));
    memcpy(obj, key, sizeof(struct client_fee_key));
    return obj;
}

static void dict_client_fee_key_free(void *key)
{
    free(key);
}

// market info val
static void dict_market_info_val_free(void *val)
{
    struct market_info_val *obj = val;
    dict_release(obj->daily_trade);
    dict_release(obj->users_detail);
    free(obj);
}

// fee val
static void dict_fee_val_free(void *val)
{
    struct fee_val *obj = val;
    mpd_del(obj->value);
    mpd_del(obj->taker_fee);
    mpd_del(obj->maker_fee);
    free(obj);
}

// daily trade val
static void dict_daily_trade_val_free(void *val)
{
    struct daily_trade_val *obj = val;
    dict_release(obj->users_trade);
    dict_release(obj->fees_detail);
    mpd_del(obj->deal_amount);
    mpd_del(obj->deal_volume);
    mpd_del(obj->taker_buy_amount);
    mpd_del(obj->taker_sell_amount);
    free(obj);
}

static uint32_t dict_client_trade_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct client_user_trade_key));
}

static int dict_client_trade_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct client_user_trade_key));
}

static void *dict_client_trade_key_dup(const void *key)
{
    struct client_user_trade_key *obj = malloc(sizeof(struct client_user_trade_key));
    memcpy(obj, key, sizeof(struct client_user_trade_key));
    return obj;
}

static void dict_client_trade_key_free(void *key)
{
    free(key);
}

// user trade val
static void dict_users_trade_val_free(void *val)
{
    struct users_trade_val *obj = val;
    mpd_del(obj->deal_amount);
    mpd_del(obj->deal_volume);
    mpd_del(obj->buy_amount);
    mpd_del(obj->sell_amount);
    mpd_del(obj->buy_volume);
    mpd_del(obj->sell_volume);
    mpd_del(obj->taker_amount);
    mpd_del(obj->taker_volume);
    mpd_del(obj->maker_amount);
    mpd_del(obj->maker_volume);

    free(obj);
}

// user dict val
static void dict_user_detail_dict_free(void *val)
{
    dict_release(val);
}

// user detail val
static void dict_user_detail_val_free(void *val)
{
    struct user_detail_val *obj = val;
    mpd_del(obj->buy_amount);
    mpd_del(obj->sell_amount);
    mpd_del(obj->buy_volume);
    mpd_del(obj->sell_volume);
    free(obj);
}

// trade net rank val
static void trade_net_rank_val_free(void *val)
{
    struct trade_net_rank_val *obj = val;
    if (obj->amount)
        mpd_del(obj->amount);
    if (obj->amount_net)
        mpd_del(obj->amount_net);
    free(obj);
}

// trade amount rank  val
static void trade_amount_rank_val_free(void *val)
{
    struct trade_amount_rank_val *obj = val;
    if (obj->amount)
        mpd_del(obj->amount);
    if (obj->amount_total)
        mpd_del(obj->amount_total);
    free(obj);
}

// trade net rank compare
static int trade_net_rank_val_compare(const void *val1, const void *val2)
{
    const struct trade_net_rank_val *obj1 = val1;
    const struct trade_net_rank_val *obj2 = val2;
    if (mpd_cmp(obj1->amount_net, obj2->amount_net, &mpd_ctx) > 0)
        return -1;
    return 1;
}

// trade amount rank compare
static int trade_amount_rank_val_compare(const void *val1, const void *val2)
{
    const struct trade_amount_rank_val *obj1 = val1;
    const struct trade_amount_rank_val *obj2 = val2;
    if (mpd_cmp(obj1->amount, obj2->amount, &mpd_ctx) > 0)
        return -1;
    return 1;
}

static struct market_info_val *get_market_info(char *market)
{
    dict_entry *entry = dict_find(dict_market_info, market);
    if (entry)
        return entry->val;

    struct market_info_val *market_info = malloc(sizeof(struct market_info_val));
    if (market_info == NULL)
        return NULL;
    memset(market_info, 0, sizeof(struct market_info_val));

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = time_dict_key_hash_func;
    dt.key_compare      = time_dict_key_compare;
    dt.val_destructor   = dict_daily_trade_val_free;
    market_info->daily_trade = dict_create(&dt, 64);
    if (market_info->daily_trade == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = time_dict_key_hash_func;
    dt.key_compare      = time_dict_key_compare;
    dt.val_destructor   = dict_user_detail_dict_free;
    market_info->users_detail = dict_create(&dt, 64);
    if (market_info->users_detail == NULL)
        return NULL;

    dict_add(dict_market_info, market, market_info);
    return market_info;
}

struct daily_trade_val *get_daily_trade_info(dict_t *dict, time_t timestamp)
{
    time_t day_start = timestamp / 86400 * 86400;
    void *key = (void *)(uintptr_t)day_start;
    dict_entry *entry = dict_find(dict, key);
    if (entry)
        return entry->val;

    struct daily_trade_val *trade_info = malloc(sizeof(struct daily_trade_val));
    if (trade_info == NULL)
        return NULL;
    memset(trade_info, 0, sizeof(struct daily_trade_val));

    trade_info->deal_amount         = mpd_qncopy(mpd_zero);
    trade_info->deal_volume         = mpd_qncopy(mpd_zero);
    trade_info->taker_buy_amount    = mpd_qncopy(mpd_zero);
    trade_info->taker_sell_amount   = mpd_qncopy(mpd_zero);

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func; 
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_destructor = dict_users_trade_val_free;
    trade_info->users_trade = dict_create(&dt, 1024);
    if (trade_info->users_trade == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_client_trade_key_hash_func; 
    dt.key_compare    = dict_client_trade_key_compare;
    dt.key_dup        = dict_client_trade_key_dup;
    dt.key_destructor = dict_client_trade_key_free;
    dt.val_destructor = dict_users_trade_val_free;
    trade_info->client_trade = dict_create(&dt, 1024);
    if (trade_info->client_trade == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_fee_key_hash_func;
    dt.key_compare      = dict_fee_key_compare;
    dt.key_dup          = dict_fee_key_dup;
    dt.key_destructor   = dict_fee_key_free;
    dt.val_destructor   = dict_fee_val_free;
    trade_info->fees_detail = dict_create(&dt, 1024);
    if (trade_info->fees_detail == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_client_fee_key_hash_func;
    dt.key_compare      = dict_client_fee_key_compare;
    dt.key_dup          = dict_client_fee_key_dup;
    dt.key_destructor   = dict_client_fee_key_free;
    dt.val_destructor   = dict_fee_val_free;
    trade_info->client_fees_detail = dict_create(&dt, 1024);
    if (trade_info->client_fees_detail == NULL)
        return NULL;

    dict_add(dict, key, trade_info);
    return trade_info;
}

struct users_trade_val *get_user_trade_info(dict_t *dict, uint32_t user_id, const char *client_id)
{
    void *key = (void *)(uintptr_t)user_id;
    struct client_fee_key client_key;
    if (client_id != NULL) {
        memset(&client_key, 0, sizeof(client_key));
        client_key.user_id = user_id;
        sstrncpy(client_key.client_id, client_id, sizeof(client_key.client_id));
        key = &client_key;
    }

    dict_entry *entry = dict_find(dict, key);
    if (entry != NULL) {
        return entry->val;
    }

    struct users_trade_val *user_info = malloc(sizeof(struct users_trade_val));
    memset(user_info, 0, sizeof(struct users_trade_val));

    user_info->deal_amount = mpd_qncopy(mpd_zero);
    user_info->deal_volume = mpd_qncopy(mpd_zero);
    user_info->buy_amount  = mpd_qncopy(mpd_zero);
    user_info->buy_volume  = mpd_qncopy(mpd_zero);
    user_info->sell_amount = mpd_qncopy(mpd_zero);
    user_info->sell_volume = mpd_qncopy(mpd_zero);
    user_info->taker_amount = mpd_qncopy(mpd_zero);
    user_info->taker_volume = mpd_qncopy(mpd_zero);
    user_info->maker_amount = mpd_qncopy(mpd_zero);
    user_info->maker_volume = mpd_qncopy(mpd_zero);

    dict_add(dict, key, user_info);

    return user_info;
}

struct user_detail_val *get_user_detail_info(dict_t *dict, uint32_t user_id, time_t timestamp)
{
    dict_t *user_dict = NULL;
    void *tkey = (void *)(uintptr_t)(timestamp / 60 * 60);
    dict_entry *entry = dict_find(dict, tkey);
    if (entry != NULL) {
        user_dict = entry->val;
    } else {
        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function   = uint32_dict_hash_func; 
        dt.key_compare     = uint32_dict_key_compare;
        dt.val_destructor  = dict_user_detail_val_free;
        user_dict = dict_create(&dt, 1024);
        if (user_dict == NULL) {
            return NULL;
        }
        dict_add(dict, tkey, user_dict);
    }

    void *ukey = (void *)(uintptr_t)user_id;
    entry = dict_find(user_dict, ukey);
    if (entry != NULL) {
        return entry->val;
    }

    struct user_detail_val *user_detail = malloc(sizeof(struct user_detail_val));
    if (user_detail == NULL)
        return NULL;
    memset(user_detail, 0, sizeof(struct user_detail_val));
    user_detail->buy_amount  = mpd_qncopy(mpd_zero);
    user_detail->sell_amount = mpd_qncopy(mpd_zero);
    user_detail->buy_volume  = mpd_qncopy(mpd_zero);
    user_detail->sell_volume = mpd_qncopy(mpd_zero);
    dict_add(user_dict, ukey, user_detail);

    return user_detail;
}

static int update_market_volume(struct daily_trade_val *trade_info, int side, mpd_t *amount, mpd_t *volume)
{
    trade_info->deal_count += 1;
    mpd_add(trade_info->deal_amount, trade_info->deal_amount, amount, &mpd_ctx);
    mpd_add(trade_info->deal_volume, trade_info->deal_volume, volume, &mpd_ctx);

    if (side == MARKET_TRADE_SIDE_BUY) {
        trade_info->taker_buy_count += 1;
        mpd_add(trade_info->taker_buy_amount, trade_info->taker_buy_amount, amount, &mpd_ctx);
    } else {
        trade_info->taker_sell_count += 1;
        mpd_add(trade_info->taker_sell_amount, trade_info->taker_sell_amount, amount, &mpd_ctx);
    }

    return 0;
}

static void update_trade_info(struct users_trade_val *user_info, int side, bool is_taker, mpd_t *amount, mpd_t *volume)
{
    user_info->deal_count += 1;
    mpd_add(user_info->deal_amount, user_info->deal_amount, amount, &mpd_ctx);
    mpd_add(user_info->deal_volume, user_info->deal_volume, volume, &mpd_ctx);
    if (side == MARKET_TRADE_SIDE_BUY) {
        user_info->deal_buy_count += 1;
        mpd_add(user_info->buy_amount, user_info->buy_amount, amount, &mpd_ctx);
        mpd_add(user_info->buy_volume, user_info->buy_volume, volume, &mpd_ctx);
    } else {
        user_info->deal_sell_count += 1;
        mpd_add(user_info->sell_amount, user_info->sell_amount, amount, &mpd_ctx);
        mpd_add(user_info->sell_volume, user_info->sell_volume, volume, &mpd_ctx);
    }

    if (is_taker) {
        mpd_add(user_info->taker_amount, user_info->taker_amount, amount, &mpd_ctx);
        mpd_add(user_info->taker_volume, user_info->taker_volume, volume, &mpd_ctx);
    } else {
        mpd_add(user_info->maker_amount, user_info->maker_amount, amount, &mpd_ctx);
        mpd_add(user_info->maker_volume, user_info->maker_volume, volume, &mpd_ctx);
    }
}

static int update_user_volume(dict_t *users_trade, dict_t *users_detail, uint32_t user_id, time_t timestamp, int side, mpd_t *amount, mpd_t *volume, bool is_taker)
{
    struct users_trade_val *user_info = get_user_trade_info(users_trade, user_id, NULL);
    if (user_info == NULL)
        return -__LINE__;
    update_trade_info(user_info, side, is_taker, amount, volume);

    struct user_detail_val *user_detail = get_user_detail_info(users_detail, user_id, timestamp);
    if (user_detail == NULL)
        return -__LINE__;

    if (side == MARKET_TRADE_SIDE_BUY) {
        mpd_add(user_detail->buy_amount, user_detail->buy_amount, amount, &mpd_ctx);
        mpd_add(user_detail->buy_volume, user_detail->buy_volume, volume, &mpd_ctx);
    } else {
        mpd_add(user_detail->sell_amount, user_detail->sell_amount, amount, &mpd_ctx);
        mpd_add(user_detail->sell_volume, user_detail->sell_volume, volume, &mpd_ctx);
    }

    return 0;
}

static int update_client_volume(dict_t *users_trade, const char *client_id, uint32_t user_id, int side, mpd_t *amount, mpd_t *volume, bool is_taker)
{
    log_info("update client volume: %s", client_id);
    struct users_trade_val *user_info = get_user_trade_info(users_trade, user_id, client_id);
    if (user_info == NULL)
        return -__LINE__;
    update_trade_info(user_info, side, is_taker, amount, volume);
    return 0;
}

static int update_fee(dict_t *fees_detail, uint32_t user_id, const char *asset, mpd_t *fee, bool is_taker)
{
    struct fee_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    sstrncpy(key.asset, asset, sizeof(key.asset));

    dict_entry *entry = dict_find(fees_detail, &key);
    if (entry == NULL) {
        struct fee_val *val = malloc(sizeof(struct fee_val));
        val->value = mpd_qncopy(fee);
        if (is_taker){
            val->taker_fee = mpd_qncopy(fee);
            val->maker_fee = mpd_qncopy(mpd_zero);
        } else {
            val->taker_fee = mpd_qncopy(mpd_zero);
            val->maker_fee = mpd_qncopy(fee);
        }
        entry = dict_add(fees_detail, &key, val);
    } else {
        struct fee_val *val = entry->val;
        mpd_add(val->value, val->value, fee, &mpd_ctx);
        if (is_taker){
            mpd_add(val->taker_fee, val->taker_fee, fee, &mpd_ctx);
        } else {
            mpd_add(val->maker_fee, val->maker_fee, fee, &mpd_ctx);
        }
    }

    return 0;
}

static int update_client_fee(dict_t *client_fees_detail, const char *client_id, uint32_t user_id, const char *asset, mpd_t *fee)
{
    log_info("update client volume: %s", client_id);
    struct client_fee_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    sstrncpy(key.asset, asset, sizeof(key.asset));
    sstrncpy(key.client_id, client_id, sizeof(key.client_id));

    dict_entry *entry = dict_find(client_fees_detail, &key);
    if (entry == NULL) {
        struct fee_val *val = malloc(sizeof(struct fee_val));
        val->value = mpd_qncopy(fee);
        entry = dict_add(client_fees_detail, &key, val);
    } else {
        struct fee_val *val = entry->val;
        mpd_add(val->value, val->value, fee, &mpd_ctx);
    }

    return 0;
}

static int update_market_orders(struct daily_trade_val *trade_info, int order_type, int order_side)
{
    if (order_type == MARKET_ORDER_TYPE_LIMIT) {
        if (order_side == MARKET_TRADE_SIDE_BUY) {
            trade_info->limit_buy_order += 1;
        } else {
            trade_info->limit_sell_order += 1;
        }
    } else {
        if (order_side == MARKET_TRADE_SIDE_BUY) {
            trade_info->market_buy_order += 1;
        } else {
            trade_info->market_sell_order += 1;
        }
    }

    return 0;
}

static void update_order_info(struct users_trade_val *user_info, int order_type, int order_side)
{
    if (order_type == MARKET_ORDER_TYPE_LIMIT) {
        if (order_side == MARKET_TRADE_SIDE_BUY) {
            user_info->limit_buy_order += 1;
        } else {
            user_info->limit_sell_order += 1;
        }
    } else {
        if (order_side == MARKET_TRADE_SIDE_BUY) {
            user_info->market_buy_order += 1;
        } else {
            user_info->market_sell_order += 1;
        }
    }
}

static int update_user_orders(dict_t *users_trade, uint32_t user_id, int order_type, int order_side)
{
    struct users_trade_val *user_info = get_user_trade_info(users_trade, user_id, NULL);
    if (user_info == NULL)
        return -__LINE__;

    update_order_info(user_info, order_type, order_side);
    return 0;
}

static int update_client_orders(dict_t *users_trade, const char *client_id, uint32_t user_id, int order_type, int order_side)
{
    struct users_trade_val *user_info = get_user_trade_info(users_trade, user_id, client_id);
    if (user_info == NULL)
        return -__LINE__;

    update_order_info(user_info, order_type, order_side);
    return 0;
}

static bool check_client_id(const char *client_id)
{
    if (strlen(client_id) == 0)
        return false;

    for (size_t i = 0; i < settings.client_id_count; i++) {
        if (strcmp(client_id, settings.client_ids[i]) == 0)
            return true;
    }
    return false;
}

static void on_deals_message(sds message, int64_t offset)
{
    log_trace("deals message: %s, offset: %"PRIi64, message, offset);
    json_t *obj = json_loadb(message, sdslen(message), 0, NULL);
    if (obj == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        return;
    }

    kafka_deals_offset = offset;

    uint32_t ask_user_id = json_integer_value(json_object_get(obj, "ask_user_id"));
    uint32_t bid_user_id = json_integer_value(json_object_get(obj, "bid_user_id"));
    if (ask_user_id == 0 || bid_user_id == 0) {
        json_decref(obj);
        return;
    }

    mpd_t *amount = NULL;
    mpd_t *volume = NULL;
    mpd_t *ask_fee = NULL;
    mpd_t *bid_fee = NULL;

    double timestamp = json_real_value(json_object_get(obj, "timestamp"));
    const char *market = json_string_value(json_object_get(obj, "market"));
    int side = json_integer_value(json_object_get(obj, "side"));
    const char *amount_str = json_string_value(json_object_get(obj, "amount"));
    const char *volume_str = json_string_value(json_object_get(obj, "deal"));
    const char *ask_fee_asset = json_string_value(json_object_get(obj, "ask_fee_asset"));
    const char *bid_fee_asset = json_string_value(json_object_get(obj, "bid_fee_asset"));
    const char *ask_fee_str = json_string_value(json_object_get(obj, "ask_fee"));
    const char *bid_fee_str = json_string_value(json_object_get(obj, "bid_fee"));
    const char *ask_client_id = json_string_value(json_object_get(obj, "ask_client_id"));
    const char *bid_client_id = json_string_value(json_object_get(obj, "bid_client_id"));
    if (timestamp == 0 || market == NULL || side == 0 || amount_str == NULL || volume_str == NULL || \
            ask_fee_asset == NULL || bid_fee_asset == NULL || ask_fee_str == NULL || bid_fee_str == NULL || \
            ask_client_id == NULL || bid_client_id == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        goto cleanup;
    }

    amount = decimal(amount_str, 0);
    volume = decimal(volume_str, 0);
    ask_fee = decimal(ask_fee_str, 0);
    bid_fee = decimal(bid_fee_str, 0);
    if (amount == NULL || volume == NULL || ask_fee == NULL || bid_fee == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        goto cleanup;
    }

    struct market_info_val *market_info = get_market_info((char *)market);
    if (market_info == NULL) {
        log_error("get_market_info: %s fail", market);
        goto cleanup;
    }
    struct daily_trade_val *trade_info = get_daily_trade_info(market_info->daily_trade, (time_t)timestamp);
    if (trade_info == NULL) {
        log_error("get_daily_trade_info: %s fail", market);
        goto cleanup;
    }

    bool ask_is_taker = false;
    bool bid_is_taker = false;
    if (side == MARKET_TRADE_SIDE_SELL) {
        ask_is_taker = true;
    } else {
        bid_is_taker = true;
    }

    update_market_volume(trade_info, side, amount, volume);
    update_user_volume(trade_info->users_trade, market_info->users_detail, ask_user_id, (time_t)timestamp, MARKET_TRADE_SIDE_SELL, amount, volume, ask_is_taker);
    bool is_ask_client = false;
    if (check_client_id(ask_client_id)) {
        log_info("update client volume: %s", ask_client_id);
        is_ask_client = true;
        update_client_volume(trade_info->client_trade, ask_client_id, ask_user_id, MARKET_TRADE_SIDE_SELL, amount, volume, ask_is_taker);
    }

    update_user_volume(trade_info->users_trade, market_info->users_detail, bid_user_id, (time_t)timestamp, MARKET_TRADE_SIDE_BUY,  amount, volume, bid_is_taker);
    bool is_bid_client = false;
    if (check_client_id(bid_client_id)) {
        log_info("update client volume: %s", bid_client_id);
        is_bid_client = true;
        update_client_volume(trade_info->client_trade, bid_client_id, bid_user_id, MARKET_TRADE_SIDE_BUY, amount, volume, bid_is_taker);
    }

    if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
        update_fee(trade_info->fees_detail, ask_user_id, ask_fee_asset, ask_fee, ask_is_taker);
        if (is_ask_client) {
            update_client_fee(trade_info->client_fees_detail, ask_client_id, ask_user_id, ask_fee_asset, ask_fee);
        }
    }

    if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
        update_fee(trade_info->fees_detail, bid_user_id, bid_fee_asset, bid_fee, bid_is_taker);
        if (is_bid_client) {
            update_client_fee(trade_info->client_fees_detail, bid_client_id, bid_user_id, bid_fee_asset, bid_fee);
        }
    }

cleanup:
    if (amount)
        mpd_del(amount);
    if (volume)
        mpd_del(volume);
    if (ask_fee)
        mpd_del(ask_fee);
    if (bid_fee)
        mpd_del(bid_fee);
    json_decref(obj);
}

static void on_orders_message(sds message, int64_t offset)
{
    static int64_t max_order_id;

    log_trace("orders message: %s, offset: %"PRIi64, message, offset);
    json_t *obj = json_loadb(message, sdslen(message), 0, NULL);
    if (obj == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        return;
    }

    kafka_orders_offset = offset;

    json_t *order = json_object_get(obj, "order");
    if (order == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        goto cleanup;
    }
    int64_t order_id = json_integer_value(json_object_get(order, "id"));

    int event = json_integer_value(json_object_get(obj, "event"));
    if (event != ORDER_EVENT_PUT && event != ORDER_EVENT_FINISH) {
        json_decref(obj);
        return;
    }

    if (event == ORDER_EVENT_FINISH) {
        if (order_id <= max_order_id || max_order_id == 0) {
            json_decref(obj);
            return;
        }
    }
    max_order_id = order_id;

    const char *market = json_string_value(json_object_get(order, "market"));
    double timestamp = json_real_value(json_object_get(order, "ctime"));
    uint32_t user_id = json_integer_value(json_object_get(order, "user"));
    uint32_t order_type = json_integer_value(json_object_get(order, "type"));
    uint32_t order_side = json_integer_value(json_object_get(order, "side"));
    const char *client_id = json_string_value(json_object_get(order, "client_id"));
    if (market == NULL || timestamp == 0 || user_id == 0 || order_type == 0 || order_side == 0 || client_id == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        goto cleanup;
    }

    struct market_info_val *market_info = get_market_info((char *)market);
    if (market_info == NULL) {
        log_error("get_market_info: %s fail", market);
        goto cleanup;
    }
    struct daily_trade_val *trade_info = get_daily_trade_info(market_info->daily_trade, (time_t)timestamp);
    if (trade_info == NULL) {
        log_error("get_daily_trade_info: %s fail", market);
        goto cleanup;
    }

    update_market_orders(trade_info, order_type, order_side);
    update_user_orders(trade_info->users_trade, user_id, order_type, order_side);
    if (check_client_id(client_id)) {
        update_client_orders(trade_info->client_trade, client_id, user_id, order_type, order_side);
    }

cleanup:
    json_decref(obj);
}

static bool is_kafka_synced(void)
{
    int64_t deals_high_offset = 0;
    int64_t orders_high_offset = 0;
    if (kafka_query_offset(kafka_deals, NULL, &deals_high_offset) < 0)
        return false;
    if (kafka_query_offset(kafka_orders, NULL, &orders_high_offset) < 0)
        return false;
    if (deals_high_offset - kafka_deals_offset > 100)
        return false;
    if (orders_high_offset - kafka_orders_offset > 100)
        return false;
    return true;
}

static time_t get_utc_time_from_date(const char *date)
{
    time_t now = time(NULL);
    struct tm *timeinfo = localtime(&now);
    int year = 0, mon = 0, mday = 0;
    sscanf(date, "%d-%d-%d", &year, &mon, &mday);
    struct tm dt;
    memset(&dt, 0, sizeof(dt));
    dt.tm_year = year - 1900;
    dt.tm_mon  = mon - 1;
    dt.tm_mday = mday;
    time_t timestamp = mktime(&dt);
    return timestamp + timeinfo->tm_gmtoff;
}

static char *get_utc_date_from_time(time_t timestamp, const char *format)
{
    static char str[512];
    struct tm *timeinfo = gmtime(&timestamp);
    strftime(str, sizeof(str), format, timeinfo);
    return str;
}

static sds sql_append_mpd(sds sql, mpd_t *val, bool comma)
{
    char *str = mpd_to_sci(val, 0);
    sql = sdscatprintf(sql, "'%s'", str);
    if (comma) {
        sql = sdscatprintf(sql, ", ");
    }
    free(str);
    return sql;
}

static int dump_users_market_detail(MYSQL *conn, const char *market_name, time_t timestamp, dict_t *user_dict)
{
    char table[512];
    snprintf(table, sizeof(table), "user_detail_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `user_detail_example`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    size_t index = 0;
    size_t insert_limit = 1000;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(user_dict);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t user_id = (uintptr_t)entry->key;
        struct user_detail_val *user_detail = entry->val;

        if (index % insert_limit == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `user_id`, `market`, `time`, `buy_amount`, `sell_amount`, `buy_volume`, `sell_volume`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, %u, '%s', %ld, ", user_id, market_name, timestamp);
        sql = sql_append_mpd(sql, user_detail->buy_amount, true);
        sql = sql_append_mpd(sql, user_detail->sell_amount, true);
        sql = sql_append_mpd(sql, user_detail->buy_volume, true);
        sql = sql_append_mpd(sql, user_detail->sell_volume, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index % insert_limit == 0 || index == dict_size(user_dict)) {
            sql = sdscatprintf(sql, " on duplicate key update `buy_amount`=values(`buy_amount`), `sell_amount`=values(`sell_amount`), "
                    "`buy_volume`=values(`buy_volume`), `sell_volume`=values(`sell_volume`)");
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
        }
    }
    dict_release_iterator(iter);
    return 0;
}

static int dump_users_detail(MYSQL *conn)
{
    time_t max_timestamp = last_dump_time;
    dict_entry *market_entry;
    dict_iterator *market_iter = dict_get_iterator(dict_market_info);
    while ((market_entry = dict_next(market_iter)) != NULL) {
        const char *market_name = market_entry->key;
        struct market_info_val *market_info = market_entry->val;

        dict_entry *time_entry;
        dict_iterator *time_iter = dict_get_iterator(market_info->users_detail);
        while ((time_entry = dict_next(time_iter))) {
            time_t timestamp = (uintptr_t)time_entry->key;
            if (timestamp < last_dump_time)
                continue;

            dict_t *user_dict = time_entry->val;
            int ret = dump_users_market_detail(conn, market_name, timestamp, user_dict);
            if (ret < 0)
                return ret;

            if (timestamp > max_timestamp)
                max_timestamp = timestamp;
        }
        dict_release_iterator(time_iter);
    }
    dict_release_iterator(market_iter);
    last_dump_time = max_timestamp;
    return 0;
}

static int load_detail(MYSQL *conn, time_t timestamp)
{
    char table[512];
    snprintf(table, sizeof(table), "user_detail_%s", get_utc_date_from_time(timestamp, "%Y%m"));
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `market`, `user_id`, `buy_amount`, `sell_amount`, `buy_volume`, `sell_volume`"
                "FROM `%s` WHERE `timestamp`=%ld  and `id` > %"PRIu64" ORDER BY `id` ASC LIMIT %zu", table, timestamp, last_id, query_limit);
        log_trace("exec sql: %s", sql);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            char *market = row[1];

            struct market_info_val *market_info = get_market_info(market);
            if (market_info == NULL) {
                log_error("get_market_info: %s fail", market);
                return -__LINE__;
            }

            uint32_t user_id = strtoull(row[2], NULL, 0);
            struct user_detail_val *user_detail = get_user_detail_info(market_info->users_detail, user_id, timestamp);
            if (user_detail == NULL)
                return -__LINE__;

            mpd_t *buy_amount = decimal(row[3], 0);
            mpd_t *sell_amount = decimal(row[4], 0);
            mpd_t *buy_volume = decimal(row[5], 0);
            mpd_t *sell_volume = decimal(row[6], 0);
            if (!buy_amount || !sell_amount || !buy_volume || !sell_volume) {
                log_error("get detail fail, market: %s, timestamp: %ld", market, timestamp);
            }
            mpd_copy(user_detail->buy_amount, buy_amount, &mpd_ctx);
            mpd_copy(user_detail->sell_amount, sell_amount, &mpd_ctx);
            mpd_copy(user_detail->buy_volume, buy_volume, &mpd_ctx);
            mpd_copy(user_detail->sell_volume, sell_volume, &mpd_ctx);
            mpd_del(buy_amount);
            mpd_del(sell_amount);
            mpd_del(buy_volume);
            mpd_del(sell_volume);

            if (timestamp > last_dump_time) {
                last_dump_time = timestamp;
            }
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }
    return 0;
}

static int load_users_detail(MYSQL *conn)
{
    time_t now = time(NULL) / 60 * 60;
    for (time_t timestamp = now - settings.keep_days * 8600; timestamp <= now; timestamp += 60) {
        int ret = load_detail(conn, timestamp);
        if (ret < 0) {
            log_error("load detail fail, timestamp: %ld, ret: %d", timestamp, ret);
            return -__LINE__;
        }
    }
    return 0;
}

static int dump_market_info(MYSQL *conn, const char *market_name, const char *stock, const char *money, time_t timestamp, struct daily_trade_val *trade_info)
{
    json_t *user_list = json_array();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(trade_info->users_trade);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t user_id = (uintptr_t)entry->key;
        struct users_trade_val *val = entry->val;
        if (mpd_cmp(val->deal_volume, mpd_zero, &mpd_ctx) > 0) {
            json_array_append_new(user_list, json_integer(user_id));
        }
    }
    dict_release_iterator(iter);

    char *user_list_string = json_dumps(user_list, 0);
    size_t user_list_size = strlen(user_list_string);
    char _user_list_string[user_list_size * 2 + 1];
    mysql_real_escape_string(conn, _user_list_string, user_list_string, user_list_size);

    sds date_day = sdsnew(get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `coin_trade_summary` (`id`, `trade_date`, `market`, `stock_asset`, `money_asset`, `deal_amount`, `deal_volume`, "
            "`deal_count`, `deal_user_count`, `deal_user_list`, `taker_buy_amount`, `taker_sell_amount`, `taker_buy_count`, `taker_sell_count`, "
            "`limit_buy_order`, `limit_sell_order`, `market_buy_order`, `market_sell_order`) VALUES ");
    sql = sdscatprintf(sql, "(NULL, '%s', '%s', '%s', '%s', ", date_day, market_name, stock, money);
    sql = sql_append_mpd(sql, trade_info->deal_amount, true);
    sql = sql_append_mpd(sql, trade_info->deal_volume, true);
    sql = sdscatprintf(sql, "%d, %zu, '%s', ", trade_info->deal_count, json_array_size(user_list), _user_list_string);
    sql = sql_append_mpd(sql, trade_info->taker_buy_amount, true);
    sql = sql_append_mpd(sql, trade_info->taker_sell_amount, true);
    sql = sdscatprintf(sql, "%d, %d, %d, %d, %d, %d)", trade_info->taker_buy_count, trade_info->taker_sell_count, \
            trade_info->limit_buy_order, trade_info->limit_sell_order, trade_info->market_buy_order, trade_info->market_sell_order);
    sql = sdscatprintf(sql, "on duplicate key update `deal_count`=values(`deal_count`), `deal_user_count`=values(`deal_user_count`), "
            "`deal_user_list`=values(`deal_user_list`), `taker_buy_count`=values(`taker_buy_count`), `taker_sell_count`=values(`taker_sell_count`), "
            "`limit_buy_order`=values(`limit_buy_order`), `limit_sell_order`=values(`limit_sell_order`), `market_buy_order`=values(`market_buy_order`), "
            "`market_sell_order`=values(`market_sell_order`), `deal_amount`=values(`deal_amount`), `deal_volume`=values(`deal_volume`), "
            "`taker_buy_amount`=values(`taker_buy_amount`), `taker_sell_amount`=values(`taker_sell_amount`)");

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        ret = -__LINE__;
    }

    sdsfree(sql);
    sdsfree(date_day);
    free(user_list_string);
    json_decref(user_list);

    return ret;
}

static int load_market_info(MYSQL *conn, time_t timestamp)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    sds date_day = sdsnew(get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `market`, `deal_amount`, `deal_volume`, `taker_buy_amount`, `taker_sell_amount`, `deal_count`, "
                "`taker_buy_count`, `taker_sell_count`, `limit_buy_order`, `limit_sell_order`, `market_buy_order`, `market_sell_order` "
                "FROM `coin_trade_summary` WHERE `trade_date`= '%s' and `id` > %"PRIu64" ORDER BY `id` ASC LIMIT %zu", date_day, last_id, query_limit);
        log_trace("exec sql: %s", sql);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            sdsfree(date_day);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);

            char *market = row[1];
            struct market_info_val *market_info = get_market_info(market);
            if (market_info == NULL) {
                log_error("get_market_info: %s fail", market);
                return -__LINE__;
            }

            struct daily_trade_val *trade_info = get_daily_trade_info(market_info->daily_trade, timestamp);
            if (trade_info == NULL) {
                log_error("get_daily_trade_info: %s fail", market);
                return -__LINE__;
            }

            mpd_t *deal_amount = decimal(row[2], 0);
            mpd_t *deal_volume = decimal(row[3], 0);
            mpd_t *taker_buy_amount  = decimal(row[4], 0);
            mpd_t *taker_sell_amount = decimal(row[5], 0);
            if (!deal_amount || !deal_volume || !taker_buy_amount || !taker_sell_amount) {
                log_error("trade_info: %s fail", market);
                return -__LINE__;
            }

            mpd_copy(trade_info->deal_amount, deal_amount, &mpd_ctx);
            mpd_copy(trade_info->deal_volume, deal_volume, &mpd_ctx);
            mpd_copy(trade_info->taker_buy_amount, taker_buy_amount, &mpd_ctx);
            mpd_copy(trade_info->taker_sell_amount, taker_sell_amount, &mpd_ctx);
            trade_info->deal_count = strtoul(row[6], NULL, 0);
            trade_info->taker_buy_count = strtoul(row[7], NULL, 0);
            trade_info->taker_sell_count = strtoul(row[8], NULL, 0);
            trade_info->limit_buy_order = strtoul(row[9], NULL, 0);
            trade_info->limit_sell_order = strtoul(row[10], NULL, 0);
            trade_info->market_buy_order = strtoul(row[11], NULL, 0);
            trade_info->market_sell_order = strtoul(row[12], NULL, 0);
            mpd_del(deal_amount);
            mpd_del(deal_volume);
            mpd_del(taker_buy_amount);
            mpd_del(taker_sell_amount);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }
    sdsfree(date_day);
    return 0;
}

static int dump_client_dict_info(MYSQL *conn, const char *market_name, const char *stock, const char *money, time_t timestamp, dict_t *client_trade)
{
    char table[512];
    snprintf(table, sizeof(table), "client_trade_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `client_trade_summary_example`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    size_t insert_limit = 1000;
    size_t index = 0;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(client_trade);
    while ((entry = dict_next(iter)) != NULL) {
        struct client_user_trade_key *key = (struct client_user_trade_key *)entry->key;
        struct users_trade_val *user_info = entry->val;

        if (index % insert_limit == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `trade_date`, `client_id`, `user_id`, `market`, `stock_asset`, `money_asset`, `deal_amount`, `deal_volume`, "
                    "`buy_amount`, `buy_volume`, `sell_amount`, `sell_volume`, `taker_amount`, `taker_volume`, `maker_amount`, `maker_volume`, `deal_count`, "
                    "`deal_buy_count`, `deal_sell_count`, `limit_buy_order`, `limit_sell_order`, `market_buy_order`, `market_sell_order`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', '%s', %u, '%s', '%s', '%s', ", get_utc_date_from_time(timestamp, "%Y-%m-%d"), key->client_id, key->user_id, market_name, stock, money);
        sql = sql_append_mpd(sql, user_info->deal_amount, true);
        sql = sql_append_mpd(sql, user_info->deal_volume, true);
        sql = sql_append_mpd(sql, user_info->buy_amount, true);
        sql = sql_append_mpd(sql, user_info->buy_volume, true);
        sql = sql_append_mpd(sql, user_info->sell_amount, true);
        sql = sql_append_mpd(sql, user_info->sell_volume, true);
        sql = sql_append_mpd(sql, user_info->taker_amount, true);
        sql = sql_append_mpd(sql, user_info->taker_volume, true);
        sql = sql_append_mpd(sql, user_info->maker_amount, true);
        sql = sql_append_mpd(sql, user_info->maker_volume, true);
        sql = sdscatprintf(sql, "%d, %d, %d, %d, %d, %d, %d)", user_info->deal_count, user_info->deal_buy_count, user_info->deal_sell_count,
                user_info->limit_buy_order, user_info->limit_sell_order, user_info->market_buy_order, user_info->market_sell_order);

        index += 1;
        if (index % insert_limit == 0 || index == dict_size(client_trade)) {
            sql = sdscatprintf(sql, " on duplicate key update `deal_count`=values(`deal_count`), `deal_buy_count`=values(`deal_buy_count`), "
                "`deal_sell_count`=values(`deal_sell_count`), `limit_buy_order`=values(`limit_buy_order`), `limit_sell_order`=values(`limit_sell_order`), "
                "`market_buy_order`=values(`market_buy_order`), `market_sell_order`=values(`market_sell_order`), `deal_amount`=values(`deal_amount`), "
                "`deal_volume`=values(`deal_volume`), `buy_amount`=values(`buy_amount`), `buy_volume`=values(`buy_volume`), `sell_amount`=values(`sell_amount`), "
                "`sell_volume`=values(`sell_volume`), `taker_amount`=values(`taker_amount`), `taker_volume`=values(`taker_volume`), "
                "`maker_amount`=values(`maker_amount`), `maker_volume`=values(`maker_volume`)");

            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
        }
    }
    dict_release_iterator(iter);
    sdsfree(sql);
    return 0;
}

static int load_client_info(MYSQL *conn, time_t timestamp)
{
    char table[512];
    snprintf(table, sizeof(table), "client_trade_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    size_t query_limit = 1000;
    uint64_t last_id = 0;
    sds date_day = sdsnew(get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `market`, `client_id`， `user_id`, `deal_amount`, `deal_volume`, `buy_amount`, `buy_volume`, "
                "`sell_amount`, `sell_volume`, `maker_volume`, `maker_amount`, `taker_volume`, `taker_amount`, `deal_count`, `deal_buy_count`, "
                "`deal_sell_count`, `limit_buy_order`, `limit_sell_order`, `market_buy_order`, `market_sell_order` "
                "FROM `%s` WHERE `trade_date`= '%s' and `id` > %"PRIu64" ORDER BY `id` ASC LIMIT %zu", table, date_day, last_id, query_limit);
        log_trace("exec sql: %s", sql);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            sdsfree(date_day);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
    
            char *market = row[1];
            struct market_info_val *market_info = get_market_info(market);
            if (market_info == NULL) {
                log_error("get_market_info: %s fail", market);
                return -__LINE__;
            }

            struct daily_trade_val *trade_info = get_daily_trade_info(market_info->daily_trade, timestamp);
            if (trade_info == NULL) {
                log_error("get_daily_trade_info: %s fail", market);
                return -__LINE__;
            }

            char *client_id = row[2];
            uint32_t user_id = strtoull(row[3], NULL, 0);
            struct users_trade_val *user_info = get_user_trade_info(trade_info->users_trade, user_id, client_id);
            if (user_info == NULL) {
                log_error("get_user_trade_info: %s fail", market);
                return -__LINE__;
            }

            mpd_t *deal_amount = decimal(row[4], 0);
            mpd_t *deal_volume = decimal(row[5], 0);
            mpd_t *buy_amount  = decimal(row[6], 0);
            mpd_t *buy_volume  = decimal(row[7], 0);
            mpd_t *sell_amount = decimal(row[8], 0);
            mpd_t *sell_volume = decimal(row[9], 0);
            mpd_t *taker_amount = decimal(row[10], 0);
            mpd_t *taker_volume = decimal(row[11], 0);
            mpd_t *maker_amount = decimal(row[12], 0);
            mpd_t *maker_volume = decimal(row[13], 0);
            if (!deal_amount || !deal_volume || !buy_amount || !buy_volume || !sell_amount 
                || !sell_volume || !taker_amount || !taker_volume || !maker_amount || !maker_volume) {
                log_error("user_info: %s fail", market);
                return -__LINE__;
            }

            mpd_copy(user_info->deal_amount, deal_amount, &mpd_ctx);
            mpd_copy(user_info->deal_volume, deal_volume, &mpd_ctx);
            mpd_copy(user_info->buy_amount, buy_amount, &mpd_ctx);
            mpd_copy(user_info->buy_volume, buy_volume, &mpd_ctx);
            mpd_copy(user_info->sell_amount, sell_amount, &mpd_ctx);
            mpd_copy(user_info->sell_volume, sell_volume, &mpd_ctx);
            mpd_copy(user_info->taker_amount, taker_amount, &mpd_ctx);
            mpd_copy(user_info->taker_volume, taker_volume, &mpd_ctx);
            mpd_copy(user_info->maker_amount, maker_amount, &mpd_ctx);
            mpd_copy(user_info->maker_volume, maker_volume, &mpd_ctx);
            user_info->deal_count = strtoul(row[14], NULL, 0);
            user_info->deal_buy_count = strtoul(row[15], NULL, 0);
            user_info->deal_sell_count = strtoul(row[16], NULL, 0);
            user_info->limit_buy_order = strtoul(row[17], NULL, 0);
            user_info->limit_sell_order = strtoul(row[18], NULL, 0);
            user_info->market_buy_order = strtoul(row[19], NULL, 0);
            user_info->market_sell_order = strtoul(row[20], NULL, 0);
            mpd_del(deal_amount);
            mpd_del(deal_volume);
            mpd_del(buy_amount);
            mpd_del(buy_volume);
            mpd_del(sell_amount);
            mpd_del(sell_volume);
            mpd_del(taker_amount);
            mpd_del(taker_volume);
            mpd_del(maker_amount);
            mpd_del(maker_volume);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }
    sdsfree(date_day);
    return 0;
}

static int dump_user_dict_info(MYSQL *conn, const char *market_name, const char *stock, const char *money, time_t timestamp, dict_t *users_trade)
{
    char table[512];
    snprintf(table, sizeof(table), "user_trade_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `user_trade_summary_example`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    size_t insert_limit = 1000;
    size_t index = 0;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(users_trade);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t user_id = (uintptr_t)entry->key;
        struct users_trade_val *user_info = entry->val;

        if (index % insert_limit == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `trade_date`, `user_id`, `market`, `stock_asset`, `money_asset`, `deal_amount`, `deal_volume`, "
                    "`buy_amount`, `buy_volume`, `sell_amount`, `sell_volume`, `taker_amount`, `taker_volume`, `maker_amount`, `maker_volume`, `deal_count`, "
                    "`deal_buy_count`, `deal_sell_count`, `limit_buy_order`, `limit_sell_order`, `market_buy_order`, `market_sell_order`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', %u, '%s', '%s', '%s', ", get_utc_date_from_time(timestamp, "%Y-%m-%d"), user_id, market_name, stock, money);
        sql = sql_append_mpd(sql, user_info->deal_amount, true);
        sql = sql_append_mpd(sql, user_info->deal_volume, true);
        sql = sql_append_mpd(sql, user_info->buy_amount, true);
        sql = sql_append_mpd(sql, user_info->buy_volume, true);
        sql = sql_append_mpd(sql, user_info->sell_amount, true);
        sql = sql_append_mpd(sql, user_info->sell_volume, true);
        sql = sql_append_mpd(sql, user_info->taker_amount, true);
        sql = sql_append_mpd(sql, user_info->taker_volume, true);
        sql = sql_append_mpd(sql, user_info->maker_amount, true);
        sql = sql_append_mpd(sql, user_info->maker_volume, true);
        sql = sdscatprintf(sql, "%d, %d, %d, %d, %d, %d, %d)", user_info->deal_count, user_info->deal_buy_count, user_info->deal_sell_count,
                user_info->limit_buy_order, user_info->limit_sell_order, user_info->market_buy_order, user_info->market_sell_order);

        index += 1;
        if (index % insert_limit == 0 || index == dict_size(users_trade)) {
            sql = sdscatprintf(sql, " on duplicate key update `deal_count`=values(`deal_count`), `deal_buy_count`=values(`deal_buy_count`), "
                "`deal_sell_count`=values(`deal_sell_count`), `limit_buy_order`=values(`limit_buy_order`), `limit_sell_order`=values(`limit_sell_order`), "
                "`market_buy_order`=values(`market_buy_order`), `market_sell_order`=values(`market_sell_order`), `deal_amount`=values(`deal_amount`), "
                "`deal_volume`=values(`deal_volume`), `buy_amount`=values(`buy_amount`), `buy_volume`=values(`buy_volume`), `sell_amount`=values(`sell_amount`), "
                "`sell_volume`=values(`sell_volume`), `taker_amount`=values(`taker_amount`), `taker_volume`=values(`taker_volume`), "
                "`maker_amount`=values(`maker_amount`), `maker_volume`=values(`maker_volume`)");
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
        }
    }
    dict_release_iterator(iter);
    sdsfree(sql);
    return 0;
}

static int load_users_info(MYSQL *conn, time_t timestamp)
{
    char table[512];
    snprintf(table, sizeof(table), "user_trade_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    size_t query_limit = 1000;
    uint64_t last_id = 0;
    sds date_day = sdsnew(get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `market`, `user_id`, `deal_amount`, `deal_volume`, `buy_amount`, `buy_volume`, `sell_amount`, "
                "`sell_volume`, `maker_volume`, `maker_amount`, `taker_volume`, `taker_amount`, `deal_count`, `deal_buy_count`, "
                "`deal_sell_count`, `limit_buy_order`, `limit_sell_order`, `market_buy_order`, `market_sell_order` "
                "FROM `%s` WHERE `trade_date`= '%s' and `id` > %"PRIu64" ORDER BY `id` ASC LIMIT %zu", table, date_day, last_id, query_limit);
        log_trace("exec sql: %s", sql);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            sdsfree(date_day);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
    
            char *market = row[1];
            struct market_info_val *market_info = get_market_info(market);
            if (market_info == NULL) {
                log_error("get_market_info: %s fail", market);
                return -__LINE__;
            }

            struct daily_trade_val *trade_info = get_daily_trade_info(market_info->daily_trade, timestamp);
            if (trade_info == NULL) {
                log_error("get_daily_trade_info: %s fail", market);
                return -__LINE__;
            }

            uint32_t user_id = strtoull(row[2], NULL, 0);
            struct users_trade_val *user_info = get_user_trade_info(trade_info->users_trade, user_id, NULL);
            if (user_info == NULL) {
                log_error("get_user_trade_info: %s fail", market);
                return -__LINE__;
            }

            mpd_t *deal_amount = decimal(row[3], 0);
            mpd_t *deal_volume = decimal(row[4], 0);
            mpd_t *buy_amount  = decimal(row[5], 0);
            mpd_t *buy_volume  = decimal(row[6], 0);
            mpd_t *sell_amount = decimal(row[7], 0);
            mpd_t *sell_volume = decimal(row[8], 0);
            mpd_t *taker_amount = decimal(row[9], 0);
            mpd_t *taker_volume = decimal(row[10], 0);
            mpd_t *maker_amount = decimal(row[11], 0);
            mpd_t *maker_volume = decimal(row[12], 0);
            if (!deal_amount || !deal_volume || !buy_amount || !buy_volume || !sell_amount 
                || !sell_volume || !taker_amount || !taker_volume || !maker_amount || !maker_volume) {
                log_error("user_info: %s fail", market);
                return -__LINE__;
            }

            mpd_copy(user_info->deal_amount, deal_amount, &mpd_ctx);
            mpd_copy(user_info->deal_volume, deal_volume, &mpd_ctx);
            mpd_copy(user_info->buy_amount, buy_amount, &mpd_ctx);
            mpd_copy(user_info->buy_volume, buy_volume, &mpd_ctx);
            mpd_copy(user_info->sell_amount, sell_amount, &mpd_ctx);
            mpd_copy(user_info->sell_volume, sell_volume, &mpd_ctx);
            mpd_copy(user_info->taker_amount, taker_amount, &mpd_ctx);
            mpd_copy(user_info->taker_volume, taker_volume, &mpd_ctx);
            mpd_copy(user_info->maker_amount, maker_amount, &mpd_ctx);
            mpd_copy(user_info->maker_volume, maker_volume, &mpd_ctx);
            user_info->deal_count = strtoul(row[13], NULL, 0);
            user_info->deal_buy_count = strtoul(row[14], NULL, 0);
            user_info->deal_sell_count = strtoul(row[15], NULL, 0);
            user_info->limit_buy_order = strtoul(row[16], NULL, 0);
            user_info->limit_sell_order = strtoul(row[17], NULL, 0);
            user_info->market_buy_order = strtoul(row[18], NULL, 0);
            user_info->market_sell_order = strtoul(row[19], NULL, 0);
            mpd_del(deal_amount);
            mpd_del(deal_volume);
            mpd_del(buy_amount);
            mpd_del(buy_volume);
            mpd_del(sell_amount);
            mpd_del(sell_volume);
            mpd_del(taker_amount);
            mpd_del(taker_volume);
            mpd_del(maker_amount);
            mpd_del(maker_volume);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }
    sdsfree(date_day);
    return 0;
}

static int dump_client_fee_dict_info(MYSQL *conn, const char *market_name, time_t timestamp, dict_t *fees_detail)
{
    char table[512];
    snprintf(table, sizeof(table), "client_fee_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `client_fee_summary_example`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    size_t insert_limit = 1000;
    size_t index = 0;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(fees_detail);
    while ((entry = dict_next(iter)) != NULL) {
        struct client_fee_key *fkey = entry->key;
        struct fee_val *fval = entry->val;

        if (index % insert_limit == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `trade_date`, `client_id`, `user_id`, `market`, `asset`, `fee`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', '%s', %u, '%s', '%s', ", get_utc_date_from_time(timestamp, "%Y-%m-%d"), fkey->client_id, fkey->user_id, market_name, fkey->asset);
        sql = sql_append_mpd(sql, fval->value, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index % insert_limit == 0 || index == dict_size(fees_detail)) {
            sql = sdscatprintf(sql, " on duplicate key update `fee`=values(`fee`)");
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
        }
    }
    dict_release_iterator(iter);
    sdsfree(sql);
    return 0;
}

static int load_client_fee_info(MYSQL *conn, time_t timestamp)
{
    char table[512];
    snprintf(table, sizeof(table), "client_fee_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    size_t query_limit = 1000;
    uint64_t last_id = 0;
    sds date_day = sdsnew(get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `market`, `asset`, `client_id`, `user_id`, `fee`"
                "FROM `%s` WHERE `trade_date`= '%s' and `id` > %"PRIu64" ORDER BY `id` ASC LIMIT %zu", table, date_day, last_id, query_limit);
        log_trace("exec sql: %s", sql);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            sdsfree(date_day);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
    
            char *market = row[1];
            struct market_info_val *market_info = get_market_info(market);
            if (market_info == NULL) {
                log_error("get_market_info: %s fail", market);
                return -__LINE__;
            }

            struct daily_trade_val *trade_info = get_daily_trade_info(market_info->daily_trade, timestamp);
            if (trade_info == NULL) {
                log_error("get_daily_trade_info: %s fail", market);
                return -__LINE__;
            }

            char *fee_asset = row[2];
            char *client_id = row[3];
            uint32_t user_id = strtoul(row[4], NULL, 0);
            mpd_t *fee = decimal(row[5], 0);
            if (!fee) {
                log_error("get fee: %s fail", market);
                return -__LINE__;
            }
            update_client_fee(trade_info->client_fees_detail, client_id, user_id, fee_asset, fee);
            mpd_del(fee);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }
    sdsfree(date_day);
    return 0;
}

static int dump_fee_dict_info(MYSQL *conn, const char *market_name, time_t timestamp, dict_t *fees_detail)
{
    char table[512];
    snprintf(table, sizeof(table), "user_fee_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `user_fee_summary_example`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    size_t insert_limit = 1000;
    size_t index = 0;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(fees_detail);
    while ((entry = dict_next(iter)) != NULL) {
        struct fee_key *fkey = entry->key;
        struct fee_val *fval = entry->val;

        if (index % insert_limit == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `trade_date`, `user_id`, `market`, `asset`, `fee`, `taker_fee`, `maker_fee`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', %u, '%s', '%s', ", get_utc_date_from_time(timestamp, "%Y-%m-%d"), fkey->user_id, market_name, fkey->asset);
        sql = sql_append_mpd(sql, fval->value, true);
        sql = sql_append_mpd(sql, fval->taker_fee, true);
        sql = sql_append_mpd(sql, fval->maker_fee, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index % insert_limit == 0 || index == dict_size(fees_detail)) {
            sql = sdscatprintf(sql, " on duplicate key update `fee`=values(`fee`), `taker_fee`=values(`taker_fee`), `maker_fee`=values(`maker_fee`)");
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
        }
    }
    dict_release_iterator(iter);
    sdsfree(sql);
    return 0;
}

static int load_users_fee_info(MYSQL *conn, time_t timestamp)
{
    char table[512];
    snprintf(table, sizeof(table), "user_fee_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    size_t query_limit = 1000;
    uint64_t last_id = 0;
    sds date_day = sdsnew(get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `market`, `asset`, `user_id`, `fee`, `taker_fee`, `maker_fee`"
                "FROM `%s` WHERE `trade_date`= '%s' and `id` > %"PRIu64" ORDER BY `id` ASC LIMIT %zu", table, date_day, last_id, query_limit);
        log_trace("exec sql: %s", sql);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            sdsfree(date_day);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
    
            char *market = row[1];
            struct market_info_val *market_info = get_market_info(market);
            if (market_info == NULL) {
                log_error("get_market_info: %s fail", market);
                return -__LINE__;
            }

            struct daily_trade_val *trade_info = get_daily_trade_info(market_info->daily_trade, timestamp);
            if (trade_info == NULL) {
                log_error("get_daily_trade_info: %s fail", market);
                return -__LINE__;
            }

            char *fee_asset = row[2];
            uint32_t user_id = strtoul(row[3], NULL, 0);
            mpd_t *fee = decimal(row[4], 0);
            mpd_t *taker_fee = decimal(row[5], 0);
            mpd_t *maker_fee = decimal(row[6], 0);
            if (!fee || !taker_fee || !maker_fee) {
                log_error("get fee: %s fail", market);
                return -__LINE__;
            }

            struct fee_key key;
            memset(&key, 0, sizeof(key));
            key.user_id = user_id;
            sstrncpy(key.asset, fee_asset, sizeof(key.asset));

            struct fee_val *val = malloc(sizeof(struct fee_val));
            val->value = fee;
            val->taker_fee = taker_fee;
            val->maker_fee = maker_fee;
            dict_add(trade_info->fees_detail, &key, val);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }
    sdsfree(date_day);
    return 0;
}

static int dump_market(MYSQL *conn, json_t *markets, time_t timestamp)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market_info);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market_name = entry->key;
        json_t *attr = json_object_get(markets, market_name);
        if (attr == NULL)
            continue;
        const char *stock = json_string_value(json_object_get(attr, "stock"));
        const char *money = json_string_value(json_object_get(attr, "money"));

        struct market_info_val *market_info = entry->val;
        void *tkey = (void *)(uintptr_t)timestamp;
        dict_entry *result = dict_find(market_info->daily_trade, tkey);
        if (result == NULL)
            continue;

        int ret;
        struct daily_trade_val *trade_info = result->val;
        ret = dump_market_info(conn, market_name, stock, money, timestamp, trade_info);
        if (ret < 0) {
            log_error("dump_market_info: %s timestamp: %ld fail", market_name, timestamp);
            return -__LINE__;
        }
        ret = dump_user_dict_info(conn, market_name, stock, money, timestamp, trade_info->users_trade);
        if (ret < 0) {
            log_error("dump_users_info: %s timestamp: %ld fail", market_name, timestamp);
            return -__LINE__;
        }
        ret = dump_client_dict_info(conn, market_name, stock, money, timestamp, trade_info->client_trade);
        if (ret < 0) {
            log_error("dump_client_trade_info: %s timestamp: %ld fail", market_name, timestamp);
            return -__LINE__;
        }
        ret = dump_fee_dict_info(conn, market_name, timestamp, trade_info->fees_detail);
        if (ret < 0) {
            log_error("dump_fee_info: %s timestamp: %ld fail", market_name, timestamp);
            return -__LINE__;
        }
        ret = dump_client_fee_dict_info(conn, market_name, timestamp, trade_info->client_fees_detail);
        if (ret < 0) {
            log_error("dump_client_fee_info: %s timestamp: %ld fail", market_name, timestamp);
            return -__LINE__;
        }
    }
    dict_release_iterator(iter);

    return 0;
}

static int load_market(MYSQL *conn, time_t date)
{
    int ret = load_market_info(conn, date);
    if (ret < 0) {
        log_error("load_market_info fail: %d", ret);
        return -__LINE__;
    }

    ret = load_users_info(conn, date);
    if (ret < 0) {
        log_error("load_users_info fail: %d", ret);
        return -__LINE__;
    }

    ret = load_users_fee_info(conn, date);
    if (ret < 0) {
        log_error("load_users_fee_info fail: %d", ret);
        return -__LINE__;
    }

    ret = load_client_info(conn, date);
    if (ret < 0) {
        log_error("load_client_info fail: %d", ret);
        return -__LINE__;
    }

    ret = load_client_fee_info(conn, date);
    if (ret < 0) {
        log_error("load_client_info fail: %d", ret);
        return -__LINE__;
    }

    return 0;
}

static int update_dump_history(MYSQL *conn, time_t date, time_t timestamp)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `dump_history` (`id`, `time`, `trade_date`, `deals_offset`, `orders_offset`)"
          " VALUES (NULL, %ld, '%s', %"PRIi64", %"PRIi64")", timestamp, get_utc_date_from_time(date, "%Y-%m-%d"), kafka_deals_offset, kafka_orders_offset);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);
    log_info("update dump history to: %s", get_utc_date_from_time(date, "%Y-%m-%d"));

    return 0;
}

static int dump_to_db()
{
    json_t *markets = get_market_dict();
    if (markets == NULL) {
        log_error("get market list fail");
        return -__LINE__;
    }

    MYSQL *conn = mysql_connect(&settings.db_summary);
    if (conn == NULL) {
        log_error("connect mysql fail");
        json_decref(markets);
        return -__LINE__;
    }

    time_t now = time(NULL);
    time_t today_start = get_utc_day_start(now);
    if (last_dump_date == 0) {
        last_dump_date = today_start;
    }
    log_info("last_dump_date: %zd, today_start: %zd", last_dump_date, today_start);

    int ret;
    time_t date;
    for (date = last_dump_date; date <= today_start; date += 86400) {
        ret = dump_market(conn, markets, date);
        if (ret < 0) {
            mysql_close(conn);
            json_decref(markets);
            log_error("dump_market fail: %d", ret);
            return -__LINE__;
        }
    }
    json_decref(markets);

    ret = dump_users_detail(conn);
    if (ret < 0) {
        mysql_close(conn);
        log_error("dump_users_detail fail: %d", ret);
        return -__LINE__;
    }

    ret = update_dump_history(conn, date, now);
    if (ret < 0) {
        mysql_close(conn);
        log_error("update_dump_history fail: %d", ret);
        return -__LINE__;
    }

    last_dump_date = date;
    mysql_close(conn);
    return 0;
}

static int get_last_dump(MYSQL *conn, int64_t *orders_offset, int64_t *deals_offset)
{    
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `trade_date`, `orders_offset`, `deals_offset` from dump_history order by `id` desc limit 1");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    if (num_rows == 1) {
        MYSQL_ROW row = mysql_fetch_row(result);
        last_dump_date = get_utc_time_from_date(row[0]);
        *orders_offset = strtoull(row[1], NULL, 0);
        *deals_offset = strtoull(row[1], NULL, 0);
    }
    mysql_free_result(result);
    return 0;
}

static int load_from_db(int64_t *orders_offset, int64_t *deals_offset)
{
    MYSQL *conn = mysql_connect(&settings.db_summary);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    int ret = get_last_dump(conn, orders_offset, deals_offset);
    if (ret < 0) {
        log_error("get_last_dump fail: %d", ret);
        mysql_close(conn);
        return -__LINE__;
    }

    if (last_dump_date == 0) {
        log_info("there is no data");
        mysql_close(conn);
        return 0;
    }

    ret = load_market(conn, last_dump_date);
    if (ret < 0) {
        log_error("load_market fail: %d", ret);
        mysql_close(conn);
        return -__LINE__;
    }

    ret = load_users_detail(conn);
    if (ret < 0) {
        log_error("load_users_detail fail: %d", ret);
        mysql_close(conn);
        return -__LINE__;
    }

    mysql_close(conn);
    return 0;
}

static void on_dump_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    if (now % 3600 > 60 || !is_kafka_synced())
        return;

    dlog_flush_all();
    int pid = fork();
    if (pid < 0) {
        log_fatal("fork fail: %d", pid);
        return;
    } else if (pid > 0) {
        return;
    }

    int ret = dump_to_db();
    if (ret < 0) {
        log_fatal("dump_to_db fail, ret: %d", ret);
        exit(0);
    }
    profile_inc_real("dump_success", 1);
    exit(0);
}

static void clear_market(struct market_info_val *market_info, time_t end)
{
    dict_entry *entry;
    dict_iterator *iter;

    iter = dict_get_iterator(market_info->daily_trade);
    while ((entry = dict_next(iter)) != NULL) {
        time_t key_time = (uintptr_t)entry->key;
        if (key_time < end) {
            dict_delete(market_info->daily_trade, entry->key);
        }
    }
    dict_release_iterator(iter);

    iter = dict_get_iterator(market_info->users_detail);
    while ((entry = dict_next(iter)) != NULL) {
        time_t key_time = (uintptr_t)entry->key;
        if (key_time < end) {
            dict_delete(market_info->users_detail, entry->key);
        }
    }
    dict_release_iterator(iter);
}

static void on_clear_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    time_t end = now / 86400 * 86400 - settings.keep_days * 86400;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market_info);
    while ((entry = dict_next(iter)) != NULL) {
        clear_market(entry->val, end);
    }
    dict_release_iterator(iter);
}

static void report_kafka_offset(kafka_consumer_t *consumer, int64_t current_offset)
{
    int64_t high = 0;
    if (kafka_query_offset(consumer, NULL, &high) < 0) {
        log_error("kafka_query_offset %s fail", consumer->topic);
    } else {
        log_info("topic: %s hightest offset: %"PRIi64", currently: %"PRIi64", gap: %"PRIi64, \
                consumer->topic, high, current_offset, high - current_offset);
    }
}

static void on_report_timer(nw_timer *timer, void *privdata)
{
    report_kafka_offset(kafka_deals, kafka_deals_offset);
    report_kafka_offset(kafka_orders, kafka_orders_offset);
}

int init_message(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = str_dict_hash_function;
    dt.key_compare      = str_dict_key_compare;
    dt.key_dup          = str_dict_key_dup;
    dt.key_destructor   = str_dict_key_free;
    dt.val_destructor   = dict_market_info_val_free;

    dict_market_info = dict_create(&dt, 64);
    if (dict_market_info == NULL)
        return -__LINE__;

    int64_t offset = 0, orders_offset = 0, deals_offset = 0;
    int ret = load_from_db(&orders_offset, &deals_offset);
    if (ret < 0) {
        log_error("load_from_db fail: %d", ret);
        log_stderr("load_from_db fail: %d", ret);
        return -__LINE__;
    }

    offset = deals_offset == 0 ? RD_KAFKA_OFFSET_END : deals_offset + 1;
    kafka_deals = kafka_consumer_create(settings.brokers, TOPIC_DEAL, 0, offset, on_deals_message);
    if (kafka_deals == NULL)
        return -__LINE__;

    offset = orders_offset == 0 ? RD_KAFKA_OFFSET_END : orders_offset + 1;
    kafka_orders = kafka_consumer_create(settings.brokers, TOPIC_ORDER, 0, offset, on_orders_message);
    if (kafka_orders == NULL)
        return -__LINE__;

    nw_timer_set(&dump_timer, 1, true, on_dump_timer, NULL);
    nw_timer_start(&dump_timer);

    nw_timer_set(&clear_timer, 3600, true, on_clear_timer, NULL);
    nw_timer_start(&clear_timer);

    nw_timer_set(&report_timer, 10, true, on_report_timer, NULL);
    nw_timer_start(&report_timer);

    return 0;
}

static int update_trade_detail(dict_t *dict, time_t start_time, time_t end_time, const char *market_name)
{
    dict_entry *entry = dict_find(dict_market_info, market_name);
    if (entry == NULL)
        return 0;
    struct market_info_val *market_info = entry->val;

    for (time_t timestamp = start_time / 60 * 60; timestamp <= end_time; timestamp += 60) {
        void *tkey = (void *)(uintptr_t)timestamp;
        entry = dict_find(market_info->users_detail, tkey);
        if (entry == NULL)
            continue;

        dict_t *user_dict = entry->val;
        dict_iterator *iter = dict_get_iterator(user_dict);
        while ((entry = dict_next(iter)) != NULL) {
            void *ukey = entry->key;
            struct user_detail_val *user_detail = entry->val;

            dict_entry *result = dict_find(dict, ukey);
            if (result == NULL) {
                struct user_detail_val *detail = malloc(sizeof(struct user_detail_val));
                memset(detail, 0, sizeof(struct user_detail_val));
                detail->buy_amount  = mpd_qncopy(mpd_zero);
                detail->sell_amount = mpd_qncopy(mpd_zero);
                detail->buy_volume  = mpd_qncopy(mpd_zero);
                detail->sell_volume = mpd_qncopy(mpd_zero);
                result = dict_add(dict, ukey, detail);
            }

            struct user_detail_val *user_total = result->val;
            mpd_add(user_total->buy_amount, user_total->buy_amount, user_detail->buy_amount, &mpd_ctx);
            mpd_add(user_total->sell_amount, user_total->sell_amount, user_detail->sell_amount, &mpd_ctx);
        }
        dict_release_iterator(iter);
    }

    return 0;
}

static int get_trade_users_detail(dict_t *dict, json_t *user_list, const char *market_name, time_t start_time, time_t end_time)
{
    dict_entry *entry = dict_find(dict_market_info, market_name);
    if (entry == NULL)
        return 0;
    struct market_info_val *market_info = entry->val;

    for (time_t timestamp = start_time / 60 * 60; timestamp < end_time; timestamp += 60) {
        void *tkey = (void *)(uintptr_t)timestamp;
        dict_entry *entry_users = dict_find(market_info->users_detail, tkey);
        if (entry_users == NULL)
            continue;
        dict_t *user_dict = entry_users->val;
        
        for (size_t i = 0; i < json_array_size(user_list); i++) {
            uint32_t user_id = json_integer_value(json_array_get(user_list, i));
            void *key = (void *)(uintptr_t)user_id;
            dict_entry *entry_user = dict_find(user_dict, key);
            if (entry_user == NULL) {
                continue;
            }

            void *ukey = entry_user->key;
            struct user_detail_val *user_detail = entry_user->val;
            dict_entry *result = dict_find(dict, ukey);
            if (result == NULL) {
                struct user_detail_val *detail = malloc(sizeof(struct user_detail_val));
                memset(detail, 0, sizeof(struct user_detail_val));
                detail->buy_amount  = mpd_qncopy(mpd_zero);
                detail->sell_amount = mpd_qncopy(mpd_zero);
                detail->buy_volume  = mpd_qncopy(mpd_zero);
                detail->sell_volume = mpd_qncopy(mpd_zero);
                result = dict_add(dict, ukey, detail);
            }

            struct user_detail_val *user_total = result->val;
            mpd_add(user_total->buy_volume, user_total->buy_volume, user_detail->buy_volume, &mpd_ctx);
            mpd_add(user_total->sell_volume, user_total->sell_volume, user_detail->sell_volume, &mpd_ctx);
        }
    }

    return 0;
}

json_t *get_trade_users_volume(json_t *market_list, json_t *user_list,time_t start_time, time_t end_time)
{
    if (!is_kafka_synced()) {
        return NULL;
    }
    
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func; 
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_destructor = dict_user_detail_val_free;

    dict_t *dict = dict_create(&dt, 1024);
    if (dict == NULL)
        return NULL;

    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *diter;
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        const char *market_name = json_string_value(json_array_get(market_list, i));
        get_trade_users_detail(dict, user_list, market_name, start_time, end_time);
        diter = dict_get_iterator(dict);
        json_t *market_info = json_object();
        while ((entry = dict_next(diter)) != NULL) {
            uint32_t user_id = (uintptr_t)entry->key;
            struct user_detail_val *user_detail = entry->val;

            json_t *item = json_array();
            char user_id_str[20];
            snprintf(user_id_str, sizeof(user_id_str), "%u", user_id);
            json_array_append_new_mpd(item, user_detail->buy_volume);
            json_array_append_new_mpd(item, user_detail->sell_volume);
            json_object_set_new(market_info, user_id_str, item);
        }
        json_object_set_new(result, market_name, market_info);
        dict_release_iterator(diter);
        dict_clear(dict);
    }
    
    dict_release(dict);
    return result;
}

json_t *get_trade_net_rank(json_t *market_list, time_t start_time, time_t end_time)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = uint32_dict_hash_func; 
    dt.key_compare     = uint32_dict_key_compare;
    dt.val_destructor  = dict_user_detail_val_free;

    dict_t *dict = dict_create(&dt, 1024);
    if (dict == NULL)
        return NULL;

    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        const char *market_name = json_string_value(json_array_get(market_list, i));
        update_trade_detail(dict, start_time, end_time, market_name);
    }

    skiplist_type st;
    memset(&st, 0, sizeof(st));
    st.free = trade_net_rank_val_free;
    st.compare = trade_net_rank_val_compare;

    mpd_t *total_net = mpd_qncopy(mpd_zero);
    mpd_t *total_amount = mpd_qncopy(mpd_zero);

    skiplist_t *buy_list = skiplist_create(&st);
    skiplist_t *sell_list = skiplist_create(&st);

    dict_entry *entry;
    dict_iterator *diter = dict_get_iterator(dict);
    while ((entry = dict_next(diter)) != NULL) {
        uint32_t user_id = (uintptr_t)entry->key;
        struct user_detail_val *user_detail = entry->val;

        struct trade_net_rank_val *rank_detail = malloc(sizeof(struct trade_net_rank_val));
        rank_detail->user_id = user_id;
        rank_detail->amount = mpd_qncopy(mpd_zero);
        rank_detail->amount_net = mpd_qncopy(mpd_zero);
        mpd_add(rank_detail->amount, user_detail->buy_amount, user_detail->sell_amount, &mpd_ctx);
        if (mpd_cmp(user_detail->buy_amount, user_detail->sell_amount, &mpd_ctx) >= 0) {
            mpd_sub(rank_detail->amount_net, user_detail->buy_amount, user_detail->sell_amount, &mpd_ctx);
            skiplist_insert(buy_list, rank_detail);
            mpd_add(total_net, total_net, rank_detail->amount_net, &mpd_ctx);
        } else {
            mpd_sub(rank_detail->amount_net, user_detail->sell_amount, user_detail->buy_amount, &mpd_ctx);
            skiplist_insert(sell_list, rank_detail);
        }

        mpd_add(total_amount, total_amount, user_detail->buy_amount, &mpd_ctx);
    }
    dict_release_iterator(diter);
    dict_release(dict);

    skiplist_node *node;
    skiplist_iter *siter;
    size_t count;
    size_t reply_limit = 500;
    int total_buy_users = skiplist_len(buy_list);
    int total_sell_users = skiplist_len(sell_list);

    count = 0;
    json_t *net_buy = json_array();
    siter = skiplist_get_iterator(buy_list);
    while ((node = skiplist_next(siter)) != NULL) {
        struct trade_net_rank_val *val = node->value;
        json_t *item = json_object();
        json_object_set_new_mpd(item, "total", val->amount);
        json_object_set_new_mpd(item, "net", val->amount_net);
        json_object_set_new    (item, "user_id", json_integer(val->user_id));
        json_array_append_new(net_buy, item);

        count += 1;
        if (count >= reply_limit)
            break;
    }
    skiplist_release_iterator(siter);

    count = 0;
    json_t *net_sell = json_array();
    siter = skiplist_get_iterator(sell_list);
    while ((node = skiplist_next(siter)) != NULL) {
        struct trade_net_rank_val *val = node->value;
        json_t *item = json_object();
        json_object_set_new_mpd(item, "total", val->amount);
        json_object_set_new_mpd(item, "net", val->amount_net);
        json_object_set_new    (item, "user_id", json_integer(val->user_id));
        json_array_append_new(net_sell, item);

        count += 1;
        if (count >= reply_limit)
            break;
    }
    skiplist_release_iterator(siter);
    skiplist_release(buy_list);
    skiplist_release(sell_list);

    json_t *result = json_object();
    json_object_set_new(result, "buy", net_buy);
    json_object_set_new(result, "sell", net_sell);
    json_object_set_new(result, "total_buy_users", json_integer(total_buy_users));
    json_object_set_new(result, "total_sell_users", json_integer(total_sell_users));
    json_object_set_new_mpd(result, "total_net", total_net);
    json_object_set_new_mpd(result, "total_amount", total_amount);

    mpd_del(total_net);
    mpd_del(total_amount);

    return result;
}

json_t *get_trade_amount_rank(json_t *market_list, time_t start_time, time_t end_time)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func; 
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_destructor = dict_user_detail_val_free;

    dict_t *dict = dict_create(&dt, 1024);
    if (dict == NULL)
        return NULL;

    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        const char *market_name = json_string_value(json_array_get(market_list, i));
        update_trade_detail(dict, start_time, end_time, market_name);
    }

    skiplist_type st;
    memset(&st, 0, sizeof(st));
    st.free = trade_amount_rank_val_free;
    st.compare = trade_amount_rank_val_compare;

    skiplist_t *buy_amount_list = skiplist_create(&st);
    skiplist_t *sell_amount_list = skiplist_create(&st);

    dict_entry *entry;
    dict_iterator *diter = dict_get_iterator(dict);
    while ((entry = dict_next(diter)) != NULL) {
        uint32_t user_id = (uintptr_t)entry->key;
        struct user_detail_val *user_detail = entry->val;
        if (mpd_cmp(user_detail->buy_amount, mpd_zero, &mpd_ctx) != 0) {
            struct trade_amount_rank_val *buy_rank_detail = malloc(sizeof(struct trade_amount_rank_val));
            memset(buy_rank_detail, 0, sizeof(struct trade_amount_rank_val));
            buy_rank_detail->user_id = user_id;
            buy_rank_detail->amount = mpd_qncopy(user_detail->buy_amount);
            buy_rank_detail->amount_total = mpd_qncopy(mpd_zero);
            mpd_add(buy_rank_detail->amount_total, user_detail->buy_amount, user_detail->sell_amount, &mpd_ctx);
            skiplist_insert(buy_amount_list, buy_rank_detail);
        }

        if (mpd_cmp(user_detail->sell_amount, mpd_zero, &mpd_ctx) != 0) {
            struct trade_amount_rank_val *sell_rank_detail = malloc(sizeof(struct trade_amount_rank_val));
            memset(sell_rank_detail, 0, sizeof(struct trade_amount_rank_val));
            sell_rank_detail->user_id = user_id;
            sell_rank_detail->amount = mpd_qncopy(user_detail->sell_amount);
            sell_rank_detail->amount_total = mpd_qncopy(mpd_zero);
            mpd_add(sell_rank_detail->amount_total, user_detail->buy_amount, user_detail->sell_amount, &mpd_ctx);
            skiplist_insert(sell_amount_list, sell_rank_detail);
        }
    }
    dict_release_iterator(diter);
    dict_release(dict);

    skiplist_node *node;
    skiplist_iter *siter;
    size_t count;
    size_t reply_limit = 500;
    int total_buy_users = skiplist_len(buy_amount_list);
    int total_sell_users = skiplist_len(sell_amount_list);

    count = 0;
    json_t *amount_buy = json_array();
    siter = skiplist_get_iterator(buy_amount_list);
    while ((node = skiplist_next(siter)) != NULL) {
        struct trade_amount_rank_val *val = node->value;
        json_t *item = json_object();
        json_object_set_new_mpd(item, "amount", val->amount);
        json_object_set_new_mpd(item, "total_amount", val->amount_total);
        json_object_set_new    (item, "user_id", json_integer(val->user_id));
        json_array_append_new(amount_buy, item);

        count += 1;
        if (count >= reply_limit)
            break;
    }
    skiplist_release_iterator(siter);

    count = 0;
    json_t *amount_sell = json_array();
    siter = skiplist_get_iterator(sell_amount_list);
    while ((node = skiplist_next(siter)) != NULL) {
        struct trade_amount_rank_val *val = node->value;
        json_t *item = json_object();
        json_object_set_new_mpd(item, "amount", val->amount);
        json_object_set_new_mpd(item, "total_amount", val->amount_total);
        json_object_set_new    (item, "user_id", json_integer(val->user_id));
        json_array_append_new(amount_sell, item);

        count += 1;
        if (count >= reply_limit)
            break;
    }
    skiplist_release_iterator(siter);
    skiplist_release(buy_amount_list);
    skiplist_release(sell_amount_list);

    json_t *result = json_object();
    json_object_set_new(result, "buy", amount_buy);
    json_object_set_new(result, "sell", amount_sell);
    json_object_set_new(result, "total_buy_users", json_integer(total_buy_users));
    json_object_set_new(result, "total_sell_users", json_integer(total_sell_users));

    return result;
}

