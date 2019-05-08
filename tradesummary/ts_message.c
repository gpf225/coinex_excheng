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

static dict_t *dict_market_info;

struct market_info_val {
    dict_t *daily_trade;
    dict_t *users_detail;
};

struct fee_key {
    uint32_t user_id;
    char asset[ASSET_NAME_MAX_LEN];
};

struct user_key {
    uint32_t user_id;
};

struct time_key {
    time_t timestamp;
};

struct fee_val {
    mpd_t *value;
};

struct daily_trade_val {
    dict_t  *users_trade;
    dict_t  *fees_detail;

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

struct users_trade_val {
    mpd_t   *deal_amount;
    mpd_t   *deal_volume;
    mpd_t   *buy_amount;
    mpd_t   *buy_volume;
    mpd_t   *sell_amount;
    mpd_t   *sell_volume;

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
};

// str key
static uint32_t dict_str_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen((char *)key));
}

static int dict_str_key_compare(const void *key1, const void *key2)
{
    return strcmp((char *)key1, (char *)key2);
}

static void *dict_str_key_dup(const void *key)
{
    return strdup((char *)key);
}

static void dict_str_key_free(void *key)
{
    free(key);
}

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

// user key
static uint32_t dict_user_key_hash_func(const void *key)
{
    struct user_key *obj = (void *)key;
    return obj->user_id;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    struct user_key *obj1 = (void *)key1;
    struct user_key *obj2 = (void *)key2;
    return obj1->user_id == obj2->user_id;
}

static void *dict_user_key_dup(const void *key)
{
    struct user_key *obj = malloc(sizeof(struct user_key));
    memcpy(obj, key, sizeof(struct user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

// time key
static uint32_t dict_time_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct time_key));
}

static int dict_time_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct time_key));
}

static void *dict_time_key_dup(const void *key)
{
    struct time_key *obj = malloc(sizeof(struct time_key));
    memcpy(obj, key, sizeof(struct time_key));
    return obj;
}

static void dict_time_key_free(void *key)
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
    free(obj);
}

// daily trade val
static void dict_daily_trade_val_free(void *val)
{
    struct daily_trade_val *obj = val;
    dict_release(obj->users_trade);
    mpd_del(obj->deal_amount);
    mpd_del(obj->deal_volume);
    mpd_del(obj->taker_buy_amount);
    mpd_del(obj->taker_sell_amount);
    free(obj);
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
    free(obj);
}

static int set_message_offset(const char *topic, time_t when, int64_t offset)
{
    redisContext *context = redis_connect(&settings.redis);
    if (context == NULL)
        return -__LINE__;

    redisReply *reply = redisCmd(context, "HSET s:offset:%s %ld %"PRIi64, topic, when, offset);
    if (reply == NULL) {
        return -__LINE__;
    }

    freeReplyObject(reply);
    redisFree(context);
    return 0;
}

static int64_t get_message_offset(const char *topic)
{
    redisContext *context = redis_connect(&settings.redis);
    if (context == NULL)
        return -__LINE__;

    time_t now = time(NULL) / 3600 * 3600;
    for (time_t start = now - settings.keep_days * 86400; start <= now; start += 3600) {
        redisReply *reply = redisCmd(context, "HGET s:offset:%s %ld", topic, start);
        if (reply == NULL) {
            redisFree(context);
            return -__LINE__;
        }
        if (reply->type == REDIS_REPLY_STRING) {
            int64_t offset = strtoll(reply->str, NULL, 0);
            freeReplyObject(reply);
            redisFree(context);
            return offset;
        } else {
            freeReplyObject(reply);
            continue;
        }
    }

    redisFree(context);
    return 0;
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
    dt.hash_function    = dict_time_key_hash_func;
    dt.key_compare      = dict_time_key_compare;
    dt.key_dup          = dict_time_key_dup;
    dt.key_destructor   = dict_time_key_free;
    dt.val_destructor   = dict_daily_trade_val_free;
    market_info->daily_trade = dict_create(&dt, 64);
    if (market_info->daily_trade == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_time_key_hash_func;
    dt.key_compare      = dict_time_key_compare;
    dt.key_dup          = dict_time_key_dup;
    dt.key_destructor   = dict_time_key_free;
    dt.val_destructor   = dict_user_detail_dict_free;
    market_info->users_detail = dict_create(&dt, 64);
    if (market_info->users_detail == NULL)
        return NULL;

    dict_add(dict_market_info, market, market_info);
    return market_info;
}

struct daily_trade_val *get_daily_trade_info(dict_t *dict, time_t timestamp)
{
    time_t day_start = timestamp / 8640 * 86400;
    struct time_key key = { .timestamp = day_start };
    dict_entry *entry = dict_find(dict, &key);
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
    dt.hash_function    = dict_user_key_hash_func;
    dt.key_compare      = dict_user_key_compare;
    dt.key_dup          = dict_user_key_dup;
    dt.key_destructor   = dict_user_key_free;
    dt.val_destructor   = dict_users_trade_val_free;
    trade_info->users_trade = dict_create(&dt, 1024);
    if (trade_info->users_trade == NULL)
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

    dict_add(dict, &key, trade_info);
    return trade_info;
}

struct users_trade_val *get_user_trade_info(dict_t *dict, uint32_t user_id)
{
    struct user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(dict, &key);
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

    return user_info;
}

struct user_detail_val *get_user_detail_info(dict_t *dict, uint32_t user_id, time_t timestamp)
{
    dict_t *user_dict = NULL;
    struct time_key tkey = { .timestamp = timestamp / 60 * 60 };
    dict_entry *entry = dict_find(dict, &tkey);
    if (entry != NULL) {
        user_dict = entry->val;
    } else {
        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function    = dict_user_key_hash_func;
        dt.key_compare      = dict_user_key_compare;
        dt.key_dup          = dict_user_key_dup;
        dt.key_destructor   = dict_user_key_free;
        dt.val_destructor   = dict_user_detail_val_free;
        user_dict = dict_create(&dt, 1024);
        if (user_dict == NULL) {
            return NULL;
        }
        dict_add(dict, &tkey, user_dict);
    }

    struct user_key ukey = { .user_id = user_id };
    dict_find(user_dict, &ukey);
    if (entry != NULL) {
        return entry->val;
    }

    struct user_detail_val *user_detail = malloc(sizeof(struct user_detail_val));
    if (user_detail == NULL)
        return NULL;
    memset(user_detail, 0, sizeof(struct user_detail_val));
    user_detail->buy_amount  = mpd_qncopy(mpd_zero);
    user_detail->sell_amount = mpd_qncopy(mpd_zero);
    dict_add(user_dict, &ukey, user_detail);

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

static int update_user_volume(dict_t *users_trade, dict_t *users_detail, uint32_t user_id, time_t timestamp, int side, mpd_t *amount, mpd_t *volume)
{
    struct users_trade_val *user_info = get_user_trade_info(users_trade, user_id);
    if (user_info == NULL)
        return -__LINE__;

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

    struct user_detail_val *user_detail = get_user_detail_info(users_detail, user_id, timestamp);
    if (user_detail == NULL)
        return -__LINE__;

    if (side == MARKET_TRADE_SIDE_BUY) {
        mpd_add(user_detail->buy_amount, user_detail->buy_amount, amount, &mpd_ctx);
    } else {
        mpd_add(user_detail->sell_amount, user_detail->sell_amount, amount, &mpd_ctx);
    }

    return 0;
}

static int update_fee(dict_t *fees_detail, uint32_t user_id, const char *asset, mpd_t *fee)
{
    struct fee_key key;
    key.user_id = user_id;
    strncpy(key.asset, asset, sizeof(ASSET_NAME_MAX_LEN));
    dict_entry *entry = dict_find(fees_detail, &key);
    if (entry == NULL) {
        struct fee_val *val = malloc(sizeof(struct fee_val));
        val->value = mpd_qncopy(fee);
        entry = dict_add(fees_detail, &key, val);
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

static int update_user_orders(dict_t *users_trade, uint32_t user_id, int order_type, int order_side)
{
    struct users_trade_val *user_info = get_user_trade_info(users_trade, user_id);
    if (user_info == NULL)
        return -__LINE__;

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

    return 0;
}

static void on_deals_message(sds message, int64_t offset)
{
    static int64_t last_offset;
    static time_t  last_message_hour;

    json_t *obj = json_loadb(message, sdslen(message), 0, NULL);
    if (obj == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        return;
    }

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
    if (timestamp == 0 || market == NULL || side == 0 || amount_str == NULL || volume_str == NULL || \
            ask_fee_asset == NULL || bid_fee_asset == NULL || ask_fee_str == NULL || bid_fee_asset == NULL) {
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

    time_t time_hour = ((int)timestamp) / 3600 * 3600;
    if (last_message_hour != 0 && last_message_hour != time_hour)
        set_message_offset("deals", time_hour, last_offset);
    last_message_hour = time_hour;
    last_offset = offset;
    kafka_deals_offset = offset;

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

    update_market_volume(trade_info, side, amount, volume);
    update_user_volume(trade_info->users_trade, market_info->users_detail, ask_user_id, (time_t)timestamp, MARKET_TRADE_SIDE_SELL, amount, volume);
    update_user_volume(trade_info->users_trade, market_info->users_detail, bid_user_id, (time_t)timestamp, MARKET_TRADE_SIDE_BUY,  amount, volume);
    if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
        update_fee(trade_info->fees_detail, ask_user_id, ask_fee_asset, ask_fee);
    }
    if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
        update_fee(trade_info->fees_detail, bid_user_id, bid_fee_asset, bid_fee);
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
    static int64_t last_offset;
    static time_t  last_message_hour;

    json_t *obj = json_loadb(message, sdslen(message), 0, NULL);
    if (obj == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        return;
    }

    int event = json_integer_value(json_object_get(obj, "event"));
    if (event != ORDER_EVENT_PUT) {
        json_decref(obj);
        return;
    }

    json_t *order = json_object_get(obj, "order");
    if (order == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        goto cleanup;
    }

    const char *market = json_string_value(json_object_get(order, "market"));
    double timestamp = json_real_value(json_object_get(order, "ctime"));
    uint32_t user_id = json_integer_value(json_object_get(order, "user"));
    uint32_t order_type = json_integer_value(json_object_get(order, "type"));
    uint32_t order_side = json_integer_value(json_object_get(order, "side"));
    if (market == NULL || timestamp == 0 || user_id == 0 || order_type == 0 || order_side == 0) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        goto cleanup;
    }

    time_t time_hour = ((int)timestamp) / 3600 * 3600;
    if (last_message_hour != 0 && last_message_hour != time_hour)
        set_message_offset("orders", time_hour, last_offset);
    last_message_hour = time_hour;
    last_offset = offset;
    kafka_orders_offset = offset;

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

static time_t get_today_start_utc(void)
{
    time_t now = time(NULL);
    struct tm *timeinfo = localtime(&now);
    struct tm dt;
    memset(&dt, 0, sizeof(dt));
    dt.tm_year = timeinfo->tm_year;
    dt.tm_mon  = timeinfo->tm_mon;
    dt.tm_mday = timeinfo->tm_mday;
    time_t timestamp = mktime(&dt);
    return timestamp + timeinfo->tm_gmtoff;
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

static time_t get_last_dump_time(void)
{
    MYSQL *conn = mysql_connect(&settings.db_summary);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT trade_date from dump_history order by trade_date desc limit 1");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    time_t timestamp = 0;
    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    if (num_rows == 1) {
        MYSQL_ROW row = mysql_fetch_row(result);
        timestamp = get_utc_time_from_date(row[0]);
    }
    mysql_free_result(result);
    mysql_close(conn);

    return timestamp;
}

static int clear_dump_data(MYSQL *conn, time_t timestamp)
{
    int ret = 0;
    sds date_day = sdsnew(get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    sds date_mon = sdsnew(get_utc_date_from_time(timestamp, "%Y%m"));
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DELETE from user_trade_summary_%s where trade_date = '%s'", date_mon, date_day);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        ret = -__LINE__;
        goto cleanup;
    }

    sdsclear(sql);
    sql = sdscatprintf(sql, "DELETE from user_fee_summary_%s where trade_date = '%s'", date_mon, date_day);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        ret = -__LINE__;
        goto cleanup;
    }

    sdsclear(sql);
    sql = sdscatprintf(sql, "DELETE from coin_trade_summary where trade_date = '%s'", date_day);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        ret = -__LINE__;
        goto cleanup;
    }

cleanup:
    sdsfree(sql);
    sdsfree(date_mon);
    sdsfree(date_day);

    return ret;
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

static int dump_market_info(MYSQL *conn, const char *market_name, const char *stock, const char *money, time_t timestamp, struct daily_trade_val *trade_info)
{
    json_t *user_list = json_array();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(trade_info->users_trade);
    while ((entry = dict_next(iter)) != NULL) {
        struct user_key *ukey = entry->key;
        json_array_append_new(user_list, json_integer(ukey->user_id));
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

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret < 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        ret = -__LINE__;
    }

    sdsfree(sql);
    sdsfree(date_day);
    free(user_list_string);
    json_decref(user_list);

    return ret;
}

static int dump_user_dict_info(MYSQL *conn, const char *market_name, const char *stock, const char *money, time_t timestamp, dict_t *users_trade)
{
    char table[512];
    snprintf(table, sizeof(table), "user_trade_summary_%s", get_utc_date_from_time(timestamp, "%Y%m"));

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `user_trade_summary_example`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret < 0) {
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
        struct user_key *ukey = entry->key;
        struct users_trade_val *user_info = entry->val;

        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `trade_date`, `user_id`, `market`, `stock_asset`, `money_asset`, `deal_amount`, `deal_volume`, "
                    "`buy_amount`, `buy_volume`, `sell_amount`, `sell_volume`, `deal_count`, `deal_buy_count`, `deal_sell_count`, "
                    "`limit_buy_order`, `limit_sell_order`, `market_buy_order`, `market_sell_order`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', %u, '%s', '%s', '%s', ", get_utc_date_from_time(timestamp, "%Y-%m-%d"), ukey->user_id, market_name, stock, money);
        sql = sql_append_mpd(sql, user_info->deal_amount, true);
        sql = sql_append_mpd(sql, user_info->deal_volume, true);
        sql = sql_append_mpd(sql, user_info->buy_amount, true);
        sql = sql_append_mpd(sql, user_info->buy_volume, true);
        sql = sql_append_mpd(sql, user_info->sell_amount, true);
        sql = sql_append_mpd(sql, user_info->sell_volume, true);
        sql = sdscatprintf(sql, "%d, %d, %d, %d, %d, %d, %d)", user_info->deal_count, user_info->deal_buy_count, user_info->deal_sell_count,
                user_info->limit_buy_order, user_info->limit_sell_order, user_info->market_buy_order, user_info->market_sell_order);

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret < 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    dict_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret < 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
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
    if (ret < 0) {
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

        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `trade_date`, `user_id`, `market`, `asset`, `fee`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', %u, '%s', %s', ", get_utc_date_from_time(timestamp, "%Y-%m-%d"), fkey->user_id, market_name, fkey->asset);
        sql = sql_append_mpd(sql, fval->value, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret < 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    dict_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret < 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
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
        struct time_key tkey = { .timestamp = timestamp };
        dict_entry *result = dict_find(market_info->daily_trade, &tkey);
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
        ret = dump_fee_dict_info(conn, market_name, timestamp, trade_info->fees_detail);
        if (ret < 0) {
            log_error("dump_fee_info: %s timestamp: %ld fail", market_name, timestamp);
            return -__LINE__;
        }
    }
    dict_release_iterator(iter);

    return 0;
}

static int update_dump_history(MYSQL *conn, time_t timestamp)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `dump_history` (`id`, `time`, `trade_date`) VALUES (NULL, %ld, '%s')", time(NULL), get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret < 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);
    log_info("update dump history to: %s", get_utc_date_from_time(timestamp, "%Y-%m-%d"));

    return 0;
}

static int dump_to_db(time_t timestamp)
{
    log_info("start dump: %s", get_utc_date_from_time(timestamp, "%Y-%m-%d"));
    json_t *markets = get_market_dict();
    if (markets == NULL) {
        log_error("get market list fail");
        return -__LINE__;
    }

    MYSQL *conn = mysql_connect(&settings.db_summary);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    int ret;
    ret = clear_dump_data(conn, timestamp);
    if (ret < 0) {
        log_error("clear_dump_data fail: %d", ret);
        return -__LINE__;
    }

    ret = dump_market(conn, markets, timestamp);
    if (ret < 0) {
        log_error("dump_market_list_info fail: %d", ret);
        return -__LINE__;
    }

    ret = update_dump_history(conn, timestamp);
    if (ret < 0) {
        log_error("update_dump_history fail: %d", ret);
        return -__LINE__;
    }

    mysql_close(conn);
    json_decref(markets);
    return 0;
}

static void dump_summary(time_t last_dump, time_t day_start)
{
    dlog_flush_all();
    int pid = fork();
    if (pid < 0) {
        log_fatal("fork fail: %d", pid);
        return;
    } else if (pid > 0) {
        return;
    }

    for (time_t timestamp = last_dump + 86400; timestamp <= day_start; timestamp += 86400) {
        int ret = dump_to_db(timestamp);
        if (ret < 0) {
            log_error("dump_to_db %ld fail: %d", timestamp, ret);
        }
    }

    profile_inc_real("dump_success", 1);
    exit(0);
}

static void on_dump_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    if (now % 600 >= 60)
        return;
    if (!is_kafka_synced())
        return;

    time_t last_dump = get_last_dump_time();
    time_t day_start = get_today_start_utc() - 86400;
    if (last_dump != day_start) {
        dump_summary(last_dump, day_start);
    }
}

static void clear_market(struct market_info_val *market_info, time_t end)
{
    dict_entry *entry;
    dict_iterator *iter;

    iter = dict_get_iterator(market_info->daily_trade);
    while ((entry = dict_next(iter)) != NULL) {
        struct time_key *key = entry->key;
        if (key->timestamp < end) {
            dict_delete(market_info->daily_trade, key);
        }
    }
    dict_release_iterator(iter);

    iter = dict_get_iterator(market_info->users_detail);
    while ((entry = dict_next(iter)) != NULL) {
        struct time_key *key = entry->key;
        if (key->timestamp < end) {
            dict_delete(market_info->daily_trade, key);
        }
    }
    dict_release_iterator(iter);
}

static void on_clear_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    time_t end = now / 86400 * 86400 - settings.keep_days;
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
    int64_t deals_offset = get_message_offset("deals");
    if (deals_offset > 0) {
        log_info("deals start offset: %"PRIi64, deals_offset);
        settings.deals.offset = deals_offset + 1;
    }
    kafka_deals = kafka_consumer_create(&settings.deals, on_deals_message);
    if (kafka_deals == NULL)
        return -__LINE__;

    int64_t orders_offset = get_message_offset("orders");
    if (orders_offset > 0) {
        log_info("orders start offset: %"PRIi64, deals_offset);
        settings.orders.offset = orders_offset + 1;
    }
    kafka_orders = kafka_consumer_create(&settings.orders, on_orders_message);
    if (kafka_orders == NULL)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_str_key_hash_func;
    dt.key_compare      = dict_str_key_compare;
    dt.key_dup          = dict_str_key_dup;
    dt.key_destructor   = dict_str_key_free;
    dt.val_destructor   = dict_market_info_val_free;

    dict_market_info = dict_create(&dt, 64);
    if (dict_market_info == NULL)
        return -__LINE__;

    nw_timer_set(&dump_timer, 60, true, on_dump_timer, NULL);
    nw_timer_start(&dump_timer);

    nw_timer_set(&clear_timer, 3600, true, on_clear_timer, NULL);
    nw_timer_start(&clear_timer);

    nw_timer_set(&report_timer, 10, true, on_report_timer, NULL);
    nw_timer_start(&report_timer);

    return 0;
}

