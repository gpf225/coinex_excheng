/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/16, create
 */

# include <curl/curl.h>

# include "mp_config.h"
# include "mp_message.h"
# include "mp_kline.h"
# include "mp_history.h"
# include "mp_request.h"

# define TRADE_ZONE_SUFFIX       "_ZONE"
# define TRADE_ZONE_REAL_SUFFIX  "_ZONE_REAL"
# define INDEX_SUFFIX            "_INDEX"

# define DEALS_OFFSET_KEY        "k:offset_worker"
# define INDEX_OFFSET_KEY        "k:offset_worker_index"

# define TRADE_TYPE_NORMAL      1
# define TRADE_TYPE_INDEX       2
# define TRADE_TYPE_ZONE        3

enum {
    INTERVAL_SEC,
    INTERVAL_MIN,
    INTERVAL_HOUR,
    INTERVAL_DAY,
};

struct market_info {
    char   *name;
    char   *stock;
    char   *money;
    int    trade_type;
    mpd_t  *last;
    mpd_t  *sell_total;
    mpd_t  *buy_total;
    dict_t *sec;
    dict_t *min;
    dict_t *hour;
    dict_t *day;
    dict_t *update;
    list_t *deals;
    list_t *deals_json;
    list_t *real_deals;
    list_t *real_deals_json;
    list_node *summary_tail;
    double update_time;
};

struct update_key {
    int interval_type;
    time_t timestamp;
};

static uint64_t max_deals_id;
static int worker_id;
static dict_t *dict_market;

static kafka_consumer_t *deals;
static kafka_consumer_t *indexs;

static double   last_flush;
static int64_t  last_deals_offset;
static int64_t  last_indexs_offset;

static time_t last_min_time;
static time_t last_hour_time;
static time_t last_day_time;

static nw_timer flush_timer;
static nw_timer clear_timer;
static nw_timer redis_timer;
static nw_timer market_timer;
static int pipeline_count;

static void dict_kline_val_free(void *val)
{
    kline_info_free(val);
}

static uint32_t dict_update_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct update_key));
}

static int dict_update_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct update_key));
}

static void *dict_update_key_dup(const void *key)
{
    struct update_key *obj = malloc(sizeof(struct update_key));
    memcpy(obj, key, sizeof(struct update_key));
    return obj;
}

static void dict_update_key_free(void *key)
{
    free(key);
}

static void list_deals_free(void *val)
{
    free(val);
}

static void list_deals_json_free(void *val)
{
    json_decref(val);
}

static redisContext *get_redis_connection()
{
    return redis_connect(&settings.redis);
}

static int load_market_kline(redisContext *context, struct market_info *info, sds key, dict_t *dict, time_t start)
{
    static time_t max_timestamp = 0;
    redisReply *reply = redisCmd(context, "HGETALL %s", key);
    if (reply == NULL) {
        return -__LINE__;
    }
    for (size_t i = 0; i < reply->elements; i += 2) {
        time_t timestamp = strtol(reply->element[i]->str, NULL, 0);
        if (start && timestamp < start)
            continue;
        struct kline_info *kinfo = kline_from_str(reply->element[i + 1]->str);
        if (kinfo) {
            void *key = (void *)(uintptr_t)timestamp;
            dict_add(dict, key, kinfo);

            if (info->trade_type == TRADE_TYPE_NORMAL && timestamp > max_timestamp) {
                max_timestamp = timestamp;
            }
        }
    }
    freeReplyObject(reply);

    last_min_time = max_timestamp / 60 * 60;
    last_hour_time = max_timestamp / 3600 * 3600;
    last_day_time = max_timestamp / 86400 * 86400;

    return 0;
}

static int load_market_deals(redisContext *context, sds key, struct market_info *info)
{
    redisReply *reply = redisCmd(context, "LRANGE %s 0 %d", key, MARKET_DEALS_MAX - 1);
    if (reply == NULL) {
        return -__LINE__;
    }

    uint64_t deals_id = 0;
    for (size_t i = 0; i < reply->elements; ++i) {
        json_t *deal = json_loadb(reply->element[i]->str, reply->element[i]->len, 0, NULL);
        if (deal == NULL) {
            freeReplyObject(reply);
            return -__LINE__;
        }
        list_add_node_tail(info->deals_json, deal);

        if (i == 0) {
            deals_id = json_integer_value(json_object_get(deal, "id"));
        }

        if (i >= settings.deal_summary_max)
            continue;

        mpd_t *amount = decimal(json_string_value(json_object_get(deal, "amount")), 0);
        const char *type = json_string_value(json_object_get(deal, "type"));
        if (strcmp(type, "sell") == 0) {
            mpd_add(info->sell_total, info->sell_total, amount, &mpd_ctx);
        } else {
            mpd_add(info->buy_total, info->buy_total, amount, &mpd_ctx);
        }
        mpd_del(amount);
        info->summary_tail = list_tail(info->deals_json);
    }
    freeReplyObject(reply);

    if (deals_id > max_deals_id) {
        max_deals_id = deals_id;
    }
    return 0;
}

static int load_market_real_deals(redisContext *context, sds key, struct market_info *info)
{
    redisReply *reply = redisCmd(context, "LRANGE %s 0 %d", key, MARKET_DEALS_MAX - 1);
    if (reply == NULL) {
        return -__LINE__;
    }

    for (size_t i = 0; i < reply->elements; ++i) {
        json_t *deal = json_loadb(reply->element[i]->str, reply->element[i]->len, 0, NULL);
        if (deal == NULL) {
            freeReplyObject(reply);
            return -__LINE__;
        }
        list_add_node_tail(info->real_deals_json, deal);
    }

    freeReplyObject(reply);

    return 0;
}

static int load_market_last(redisContext *context, struct market_info *info)
{
    redisReply *reply = redisCmd(context, "GET k:%s:last", info->name);
    if (reply == NULL) {
        return -__LINE__;
    }
    if (reply->type == REDIS_REPLY_STRING) {
        info->last = decimal(reply->str, 0);
        if (info->last == NULL) {
            freeReplyObject(reply);
            return -__LINE__;
        }
    }
    freeReplyObject(reply);

    return 0;
}

static int load_market(redisContext *context, struct market_info *info)
{
    time_t now = time(NULL);
    int ret;

    sds key = sdsempty();
    key = sdscatprintf(key, "k:%s:1s", info->name);
    ret = load_market_kline(context, info, key, info->sec, now - settings.sec_max);
    if (ret < 0) {
        sdsfree(key);
        return ret;
    }

    sdsclear(key);
    key = sdscatprintf(key, "k:%s:1m", info->name);
    ret = load_market_kline(context, info, key, info->min, now / 60 * 60 - settings.min_max * 60);
    if (ret < 0) {
        sdsfree(key);
        return ret;
    }

    sdsclear(key);
    key = sdscatprintf(key, "k:%s:1h", info->name);
    ret = load_market_kline(context, info, key, info->hour, now / 3600 * 3600 - settings.hour_max * 3600);
    if (ret < 0) {
        sdsfree(key);
        return ret;
    }

    sdsclear(key);
    key = sdscatprintf(key, "k:%s:1d", info->name);
    ret = load_market_kline(context, info, key, info->day, 0);
    if (ret < 0) {
        sdsfree(key);
        return ret;
    }

    sdsclear(key);
    key = sdscatprintf(key, "k:%s:deals", info->name);
    ret = load_market_deals(context, key, info);
    if (ret < 0) {
        sdsfree(key);
        return ret;
    }

    sdsclear(key);
    key = sdscatprintf(key, "k:%s:real_deals", info->name);
    ret = load_market_real_deals(context, key, info);
    if (ret < 0) {
        sdsfree(key);
        return ret;
    }
    sdsfree(key);

    ret = load_market_last(context, info);
    if (ret < 0) {
        return ret;
    }

    return 0;
}

static struct market_info *create_market(const char *market, const char *stock, const char *money, int trade_type)
{
    struct market_info *info = malloc(sizeof(struct market_info));
    memset(info, 0, sizeof(struct market_info));
    info->trade_type = trade_type;
    info->name = strdup(market);
    if (stock) {
        info->stock = strdup(stock);
    }
    if (money) {
        info->money = strdup(money);
    }
    info->last = mpd_qncopy(mpd_zero);
    info->buy_total = mpd_qncopy(mpd_zero);
    info->sell_total = mpd_qncopy(mpd_zero);

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = time_dict_key_hash_func;
    dt.key_compare    = time_dict_key_compare;
    dt.val_destructor = dict_kline_val_free;

    info->sec = dict_create(&dt, 1024);
    info->min = dict_create(&dt, 1024);
    info->hour = dict_create(&dt, 1024);
    info->day = dict_create(&dt, 1024);
    if (info->sec == NULL || info->min == NULL || info->hour == NULL || info->day == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_update_key_hash_func;
    dt.key_compare    = dict_update_key_compare;
    dt.key_dup        = dict_update_key_dup;
    dt.key_destructor = dict_update_key_free;
    info->update = dict_create(&dt, 1024);
    if (info->update == NULL)
        return NULL;

    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.free = list_deals_free;
    info->deals = list_create(&lt);
    if (info->deals == NULL)
        return NULL;

    memset(&lt, 0, sizeof(lt));
    lt.free = list_deals_json_free;
    info->deals_json = list_create(&lt);
    if (info->deals_json == NULL)
        return NULL;

    memset(&lt, 0, sizeof(lt));
    lt.free = list_deals_free;
    info->real_deals = list_create(&lt);
    if (info->real_deals == NULL) {
        return NULL;
    }

    memset(&lt, 0, sizeof(lt));
    lt.free = list_deals_json_free;
    info->real_deals_json = list_create(&lt);
    if (info->real_deals_json == NULL) {
        return NULL;
    }

    sds key = sdsnew(market);
    dict_add(dict_market, key, info);

    return info;
}

static int init_single_market(redisContext *context, const char *name, const char *stock, const char *money, int trade_type)
{
    struct market_info *info = create_market(name, stock, money, trade_type);
    if (info == NULL) {
        log_error("create market %s fail", name);
        return -__LINE__;
    }

    if (worker_id < settings.worker_num || trade_type == TRADE_TYPE_ZONE) {
        int ret = load_market(context, info);
        if (ret < 0) {
            log_error("load market %s fail: %d", name, ret);
            return -__LINE__;
        }
    }
    return 0;
}

static char *convert_index_name(const char *name)
{
    static char buf[100];
    snprintf(buf, sizeof(buf), "%s%s", name, INDEX_SUFFIX);
    return buf;
}

static char *convert_trade_zone_name(const char *market_money)
{
    static char buf[100];
    snprintf(buf, sizeof(buf), "%s%s", market_money, TRADE_ZONE_SUFFIX);
    return buf;
}

static char *convert_trade_zone_real_name(const char *market_money)
{
    static char buf[100];
    snprintf(buf, sizeof(buf), "%s%s", market_money, TRADE_ZONE_REAL_SUFFIX);
    return buf;
}

static struct market_info *market_query(const char *market)
{
    sds key = sdsnew(market);
    dict_entry *entry = dict_find(dict_market, key);
    if (entry) {
        sdsfree(key);
        return entry->val;
    }
    sdsfree(key);
    return NULL;
}

static int init_market(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = sds_dict_hash_function;
    type.key_compare    = sds_dict_key_compare;
    type.key_destructor = sds_dict_key_free;
    dict_market = dict_create(&type, 64);
    if (dict_market == NULL)
        return -__LINE__;

    redisContext *context = get_redis_connection();
    if (context == NULL)
        return -__LINE__;

    json_t *market_list = get_market_list();
    if (market_list == NULL) {
        log_error("get market list fail");
        redisFree(context);
        return -__LINE__;
    }
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        json_t *item = json_array_get(market_list, i);
        const char *name = json_string_value(json_object_get(item, "name"));
        log_trace("market: %s", name);
        const char *stock = json_string_value(json_object_get(item, "stock"));
        const char *money = json_string_value(json_object_get(item, "money"));
        if (get_market_id(name) != worker_id && worker_id != settings.worker_num)
            continue;
        int ret = init_single_market(context, name, stock, money, TRADE_TYPE_NORMAL);
        if (ret < 0) {
            json_decref(market_list);
            redisFree(context);
            return -__LINE__;
        }

        if (worker_id == settings.worker_num) {
            char *trade_zone_name = convert_trade_zone_name(money);
            struct market_info *info = market_query(trade_zone_name);
            if (info == NULL) {
                int ret = init_single_market(context, trade_zone_name, NULL, money, TRADE_TYPE_ZONE);
                if (ret < 0) {
                    json_decref(market_list);
                    redisFree(context);
                    return -__LINE__;
                }
                log_info("add market: %s", trade_zone_name);
            }

            char *trade_zone_real_name = convert_trade_zone_real_name(money);
            info = market_query(trade_zone_real_name);
            if (info == NULL) {
                int ret = init_single_market(context, trade_zone_real_name, NULL, money, TRADE_TYPE_ZONE);
                if (ret < 0) {
                    json_decref(market_list);
                    redisFree(context);
                    return -__LINE__;
                }
                log_info("add market: %s", trade_zone_real_name);
            }
        }
    }
    json_decref(market_list);

    json_t *index_list = get_index_list();
    if (index_list == NULL) {
        log_error("get index list fail");
        redisFree(context);
        return -__LINE__;
    }

    const char *key;
    json_t *val;
    json_object_foreach(index_list, key, val) {
        log_trace("index name: %s", key);
        char *index_name = convert_index_name(key);
        if (get_market_id(index_name) != worker_id)
            continue;
        int ret = init_single_market(context, index_name, NULL, NULL, TRADE_TYPE_INDEX);
        if (ret < 0) {
            json_decref(index_list);
            redisFree(context);
            return -__LINE__;
        }
    }
    json_decref(index_list);

    redisFree(context);
    return 0;
}

static struct kline_info *kline_query(dict_t *dict, time_t timestamp)
{
    void *key = (void *)(uintptr_t)timestamp;
    dict_entry *entry = dict_find(dict, key);
    if (entry == NULL)
        return NULL;
    return entry->val;
}

static void add_kline_update(struct market_info *info, int type, time_t timestamp)
{
    struct update_key key;
    key.interval_type = type;
    key.timestamp = timestamp;
    dict_add(info->update, &key, NULL);
}

static void kline_history_process(int type)
{
    dict_iterator *iter = dict_get_iterator(dict_market);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct market_info *info = entry->val;
        struct kline_info *kinfo = NULL;
        time_t timestamp;
        
        if (info->trade_type != TRADE_TYPE_NORMAL || get_market_id(info->name) != worker_id)
            continue;

        if (type == INTERVAL_MIN) {
            kinfo = kline_query(info->min, last_min_time);
            timestamp = last_min_time;
        } else if (type == INTERVAL_HOUR) {
            kinfo = kline_query(info->hour, last_hour_time);
            timestamp = last_hour_time;
        } else if (type == INTERVAL_DAY) {
            kinfo = kline_query(info->day, last_day_time);
            timestamp = last_day_time;
        }

        if (!kinfo)
            continue;
        append_kline_history(info->name, type, timestamp, kinfo);
    }
    dict_release_iterator(iter);
}

static int deal_summary_update(struct market_info *info, int side, mpd_t *amount)
{
    if (side == MARKET_TRADE_SIDE_SELL) {
        mpd_add(info->sell_total, info->sell_total, amount, &mpd_ctx);
    } else {
        mpd_add(info->buy_total, info->buy_total, amount, &mpd_ctx);
    }

    if (list_len(info->deals_json) > settings.deal_summary_max) {
        json_t *deals_json = info->summary_tail->value;
        mpd_t *sub_amount = decimal(json_string_value(json_object_get(deals_json, "amount")), 0);
        const char *type = json_string_value(json_object_get(deals_json, "type"));
        if (strcmp(type, "sell") == 0) {
            mpd_sub(info->sell_total, info->sell_total, sub_amount, &mpd_ctx);
        } else {
            mpd_sub(info->buy_total, info->buy_total, sub_amount, &mpd_ctx);
        }
        mpd_del(sub_amount);
        info->summary_tail = list_prev_node(info->summary_tail);
    } else {
        info->summary_tail = list_tail(info->deals_json);
    }

    return 0;
}

static int market_update(double timestamp, uint64_t id, struct market_info *info, int side, uint32_t ask_user_id, uint32_t bid_user_id, mpd_t *price, mpd_t *amount)
{
    // update sec
    time_t time_sec = (time_t)timestamp;
    void *time_sec_key = (void *)(uintptr_t)time_sec;
    dict_entry *entry;
    struct kline_info *kinfo = NULL;
    entry = dict_find(info->sec, time_sec_key);
    if (entry) {
        kinfo = entry->val;
    } else {
        kinfo = kline_info_new(price);
        if (kinfo == NULL)
            return -__LINE__;
        dict_add(info->sec, time_sec_key, kinfo);
    }
    kline_info_update(kinfo, price, amount);
    add_kline_update(info, INTERVAL_SEC, time_sec);

    // update min
    time_t time_min = time_sec / 60 * 60;
    void *time_min_key = (void *)(uintptr_t)time_min;
    entry = dict_find(info->min, time_min_key);
    if (entry) {
        kinfo = entry->val;
    } else {
        kinfo = kline_info_new(price);
        if (kinfo == NULL)
            return -__LINE__;
        dict_add(info->min, time_min_key, kinfo);
    }
    kline_info_update(kinfo, price, amount);
    add_kline_update(info, INTERVAL_MIN, time_min);

    // update hour
    time_t time_hour = time_sec / 3600 * 3600;
    void *time_hour_key = (void *)(uintptr_t)time_hour;
    entry = dict_find(info->hour, time_hour_key);
    if (entry) {
        kinfo = entry->val;
    } else {
        kinfo = kline_info_new(price);
        if (kinfo == NULL)
            return -__LINE__;
        dict_add(info->hour, time_hour_key, kinfo);
    }
    kline_info_update(kinfo, price, amount);
    add_kline_update(info, INTERVAL_HOUR, time_hour);

    // update day
    time_t time_day = time_sec / 86400 * 86400;
    void *time_day_key = (void *)(uintptr_t)time_day;
    entry = dict_find(info->day, time_day_key);
    if (entry) {
        kinfo = entry->val;
    } else {
        kinfo = kline_info_new(price);
        if (kinfo == NULL)
            return -__LINE__;
        dict_add(info->day, time_day_key, kinfo);
    }
    kline_info_update(kinfo, price, amount);
    add_kline_update(info, INTERVAL_DAY, time_day);

    // update last
    mpd_copy(info->last, price, &mpd_ctx);

    // append deals
    if (id && worker_id != settings.worker_num) {
        json_t *deal = json_object();
        json_object_set_new(deal, "id", json_integer(id));
        json_object_set_new(deal, "time", json_real(timestamp));
        json_object_set_new(deal, "ask_user_id", json_integer(ask_user_id));
        json_object_set_new(deal, "bid_user_id", json_integer(bid_user_id));
        json_object_set_new_mpd(deal, "price", price);
        json_object_set_new_mpd(deal, "amount", amount);
        if (side == MARKET_TRADE_SIDE_SELL) {
            json_object_set_new(deal, "type", json_string("sell"));
        } else {
            json_object_set_new(deal, "type", json_string("buy"));
        }

        list_add_node_tail(info->deals, json_dumps(deal, 0));
        list_add_node_head(info->deals_json, deal);

        if (ask_user_id != 0 && bid_user_id != 0) {
            list_add_node_tail(info->real_deals, json_dumps(deal, 0));
            list_add_node_head(info->real_deals_json, deal);
            json_incref(deal);
        }

        deal_summary_update(info, side, amount);

        if (list_len(info->deals_json) > MARKET_DEALS_MAX) {
            list_del(info->deals_json, list_tail(info->deals_json));
        }

        if (list_len(info->real_deals_json) > MARKET_DEALS_MAX) {
            list_del(info->real_deals_json, list_tail(info->real_deals_json));
        }
    }

    // update time
    info->update_time = current_timestamp();
    return 0;
}

int deals_process(double timestamp, uint64_t id, struct market_info *info, int side, uint32_t ask_user_id, uint32_t bid_user_id, mpd_t *price, mpd_t *amount)
{
    // deals
    if (worker_id != settings.worker_num) {
        int ret = market_update(timestamp, id, info, side, ask_user_id, bid_user_id, price, amount);
        if (ret < 0) {
            log_error("market_update fail %d", ret);
            return -__LINE__;
        }
        return 0;
    }

    // zone deals
    if (info->money == NULL) {
        return 0;
    }
    char *trade_zone_name = convert_trade_zone_name(info->money);
    info = market_query(trade_zone_name);
    if (info == NULL) {
        log_info("trade_zone %s not exist", trade_zone_name);
        return 0;
    }
    int ret = market_update(timestamp, id, info, side, ask_user_id, bid_user_id, price, amount);
    if (ret < 0) {
        log_error("market_update fail %d", ret);
        return -__LINE__;
    }

    // zone real deals
    if (ask_user_id == 0 || bid_user_id == 0) {
        return 0;
    }

    char *trade_zone_real_name = convert_trade_zone_real_name(info->money);
    info = market_query(trade_zone_real_name);
    if (info == NULL) {
        log_info("trade_zone %s not exist", trade_zone_real_name);
        return -__LINE__;
    }

    ret = market_update(timestamp, id, info, side, ask_user_id, bid_user_id, price, amount);
    if (ret < 0) {
        log_error("market_update fail %d", ret);
        return -__LINE__;
    }

    return 0;
}

static void on_deals_message(sds message, int64_t offset)
{
    double task_start = current_timestamp();
    json_t *obj = json_loadb(message, sdslen(message), 0, NULL);
    if (obj == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        return;
    }

    mpd_t *price    = NULL;
    mpd_t *amount   = NULL;

    double timestamp = json_real_value(json_object_get(obj, "timestamp"));
    if (timestamp == 0) {
        goto cleanup;
    }
    uint64_t id = json_integer_value(json_object_get(obj, "id"));
    if (id == 0 || id <= max_deals_id) {
        goto cleanup;
    }
    const char *market = json_string_value(json_object_get(obj, "market"));
    if (!market) {
        goto cleanup;
    }

    int side = json_integer_value(json_object_get(obj, "side"));
    if (side != MARKET_TRADE_SIDE_SELL && side != MARKET_TRADE_SIDE_BUY) {
        goto cleanup;
    }

    uint32_t ask_user_id = json_integer_value(json_object_get(obj, "ask_user_id"));
    uint32_t bid_user_id = json_integer_value(json_object_get(obj, "bid_user_id"));
    
    const char *price_str = json_string_value(json_object_get(obj, "price"));
    if (!price_str || (price = decimal(price_str, 0)) == NULL) {
        goto cleanup;
    }
    const char *amount_str = json_string_value(json_object_get(obj, "amount"));
    if (!amount_str || (amount = decimal(amount_str, 0)) == NULL) {
        goto cleanup;
    }

    if (get_market_id(market) == worker_id || worker_id == settings.worker_num) {
        log_trace("deals message: %s, offset: %"PRIi64, message, offset);

        struct market_info *info = market_query(market);
        if (info == NULL) {
            info = create_market(market, NULL, NULL, TRADE_TYPE_NORMAL);
            if (info == NULL) {
                goto cleanup;
            }
            log_info("add market: %s", market);
        }

        int ret = deals_process(timestamp, id, info, side, ask_user_id, bid_user_id, price, amount);
        if (ret < 0) {
            log_error("deals_process fail %d, message: %s", ret, message);
            goto cleanup;
        }

        time_t tm = (time_t)timestamp;
        time_t min_time = tm / 60 * 60;
        time_t hour_time = tm / 3600 * 3600;
        time_t day_time = tm / 86400 * 86400;

        if (min_time != last_min_time) {
            kline_history_process(INTERVAL_MIN);
            last_min_time = min_time;
        }
        if (hour_time != last_hour_time) {
            kline_history_process(INTERVAL_HOUR);
            last_hour_time = hour_time;
        }
        if (day_time != last_day_time) {
            kline_history_process(INTERVAL_DAY);
            last_day_time = day_time;
        }

        profile_inc("new_message", 1);
        profile_inc("new_message_costs", (int)((current_timestamp() - task_start) * 1000000));
    }

    last_deals_offset = offset;

    mpd_del(price);
    mpd_del(amount);
    json_decref(obj);
    return;

cleanup:
    log_error("invalid message: %s, offset: %"PRIi64, message, offset);
    if (price)
        mpd_del(price);
    if (amount)
        mpd_del(amount);
    json_decref(obj);
}

static void on_indexs_message(sds message, int64_t offset)
{
    json_t *obj = json_loadb(message, sdslen(message), 0, NULL);
    if (obj == NULL) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        return;
    }

    mpd_t *price    = NULL;
    const char *market = json_string_value(json_object_get(obj, "market"));
    if (!market) {
        goto cleanup;
    }
    double timestamp = json_real_value(json_object_get(obj, "timestamp"));
    if (timestamp == 0) {
        goto cleanup;
    }

    const char *price_str = json_string_value(json_object_get(obj, "price"));
    if (!price_str || (price = decimal(price_str, 0)) == NULL) {
        goto cleanup;
    }

    char *index_name = convert_index_name(market);
    if (get_market_id(index_name) == worker_id) {
        log_trace("indexs message: %s, offset: %"PRIi64, message, offset);

        struct market_info *info = market_query(index_name);
        if (info == NULL) {
            info = create_market(index_name, NULL, NULL, TRADE_TYPE_INDEX);
            if (info == NULL) {
                goto cleanup;
            }
            log_info("add market: %s", index_name);
        }

        int ret = market_update(timestamp, 0, info, 0, 0, 0, price, mpd_zero);
        if (ret < 0) {
            log_error("market_update fail %d, message: %s", ret, message);
            goto cleanup;
        }
    }

    last_indexs_offset = offset;

    mpd_del(price);
    json_decref(obj);
    return;

cleanup:
    log_error("invalid message: %s, offset: %"PRIi64, message, offset);
    if (price)
        mpd_del(price);
    json_decref(obj);
    return;
}

static int pipeline_excute(redisContext *context)
{
    log_trace("pipeline_count: %d", pipeline_count);
    if (pipeline_count == 0)
        return 0;

    int count = 0;
    redisReply *reply = NULL;
    while(redisGetReply(context,(void *)&reply) == REDIS_OK) {
        freeReplyObject(reply);
        count++;
        if (count >= pipeline_count)
            break;
    }

    if (count < pipeline_count) {
        return -__LINE__;
    }
    pipeline_count = 0;
    return 0;
}

static int flush_deals(redisContext *context, const char *market, sds key, list_t *list)
{
    int argc = 2 + list->len;
    const char **argv = malloc(sizeof(char *) * argc);
    size_t *argvlen = malloc(sizeof(size_t) * argc);

    argv[0] = "LPUSH";
    argvlen[0] = strlen(argv[0]);

    argv[1] = key;
    argvlen[1] = sdslen(key);

    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    list_node *node;
    size_t index = 0;
    while ((node = list_next(iter)) != NULL) {
        argv[2 + index] = node->value;
        argvlen[2 + index] = strlen((char *)node->value);
        index += 1;
    }
    list_release_iterator(iter);

    redisAppendCommandArgv(context, argc, argv, argvlen);
    pipeline_count++;
    free(argv);
    free(argvlen);

    redisAppendCommand(context, "LTRIM %s 0 %d", key, MARKET_DEALS_MAX - 1);
    pipeline_count++;
    list_clear(list);
    return 0;
}

static int flush_kline(redisContext *context, struct market_info *info, struct update_key *ukey)
{
    sds key = sdsempty();
    struct kline_info *kinfo = NULL;
    if (ukey->interval_type == INTERVAL_SEC) {
        key = sdscatprintf(key, "k:%s:1s", info->name);
        kinfo = kline_query(info->sec, ukey->timestamp);
    } else if (ukey->interval_type == INTERVAL_MIN) {
        key = sdscatprintf(key, "k:%s:1m", info->name);
        kinfo = kline_query(info->min, ukey->timestamp);
    } else if (ukey->interval_type == INTERVAL_HOUR) {
        key = sdscatprintf(key, "k:%s:1h", info->name);
        kinfo = kline_query(info->hour, ukey->timestamp);
    } else {
        key = sdscatprintf(key, "k:%s:1d", info->name);
        kinfo = kline_query(info->day, ukey->timestamp);
    }
    if (kinfo == NULL) {
        sdsfree(key);
        return -__LINE__;
    }

    char *str = kline_to_str(kinfo);
    if (str == NULL) {
        sdsfree(key);
        return -__LINE__;
    }
    redisAppendCommand(context, "HSET %s %ld %s", key, ukey->timestamp, str);
    free(str);
    sdsfree(key);

    pipeline_count++;
    if (pipeline_count >= settings.pipeline_len_max) {
        int ret = pipeline_excute(context);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

static int64_t get_offset(const char *offset_key)
{
    redisContext *context = get_redis_connection();
    if (context == NULL)
        return -__LINE__;

    int64_t min_offset = 0;
    redisReply *reply = redisCmd(context, "HGETALL %s", offset_key);
    if (reply == NULL)
        return -__LINE__;
    for (size_t i = 0; i < reply->elements; i += 2) {
        int work_id = strtoll(reply->element[i]->str, NULL, 0);
        int64_t offset = strtoll(reply->element[i + 1]->str, NULL, 0);

        if (work_id > settings.worker_num) {
            redisCmd(context, "HDEL %s %d", offset_key, work_id);
            log_info("discard old offset, work_id: %d, offset: %ld", work_id, offset);
        }

        if (min_offset == 0) {
            min_offset = offset;
        } else if (offset < min_offset) {
            min_offset = offset;
        }
    }
    freeReplyObject(reply);
    redisFree(context);

    return min_offset;
}

static int flush_deals_offset(redisContext *context)
{
    redisReply *reply = redisCmd(context, "HSET %s %d %"PRIi64, DEALS_OFFSET_KEY, worker_id, last_deals_offset);
    if (reply == NULL) {
        return -__LINE__;
    }
    freeReplyObject(reply);

    return 0;
}

static int flush_indexs_offset(redisContext *context)
{
    redisReply *reply = redisCmd(context, "HSET %s %d %"PRIi64, INDEX_OFFSET_KEY, worker_id, last_indexs_offset);
    if (reply == NULL) {
        return -__LINE__;
    }
    freeReplyObject(reply);

    return 0;
}

static int flush_last(redisContext *context, const char *market, mpd_t *last)
{
    char *last_str = mpd_format(last, "f", &mpd_ctx);
    if (last_str == NULL)
        return -__LINE__;

    redisAppendCommand(context, "SET k:%s:last %s", market, last_str);
    pipeline_count++;
    free(last_str);

    return 0;
}
static int flush_update(redisContext *context, struct market_info *info)
{
    dict_iterator *iter = dict_get_iterator(info->update);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct update_key *key = entry->key;
        log_trace("flush_kline type: %d, timestamp: %ld", key->interval_type, key->timestamp);
        int ret = flush_kline(context, info, key);
        if (ret < 0) {
            log_fatal("flush_kline fail: %d, type: %d, timestamp: %ld", ret, key->interval_type, key->timestamp);
        }
        dict_delete(info->update, entry->key);
    }
    dict_release_iterator(iter);

    return 0;
}

static int flush_market(void)
{
    redisContext *context = get_redis_connection();
    if (context == NULL)
        return -__LINE__;

    int ret;
    pipeline_count = 0;
    dict_iterator *iter = dict_get_iterator(dict_market);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct market_info *info = entry->val;
        if (info->update_time < last_flush)
            continue;
        ret = flush_update(context, info);
        if (ret < 0) {
            redisFree(context);
            dict_release_iterator(iter);
            return ret;
        }

        if (worker_id < settings.worker_num) {   
            ret = flush_last(context, info->name, info->last);
            if (ret < 0) {
                redisFree(context);
                dict_release_iterator(iter);
                return ret;
            }
        }

        if (info->deals->len != 0) {
            sds key = sdsempty();
            key = sdscatprintf(key, "k:%s:deals", info->name);
            ret = flush_deals(context, info->name, key, info->deals);
            sdsfree(key);
            if (ret < 0) {
                redisFree(context);
                dict_release_iterator(iter);
                return ret;
            }
        }

        if (info->real_deals->len != 0) {
            sds key = sdsempty();
            key = sdscatprintf(key, "k:%s:real_deals", info->name);
            ret = flush_deals(context, info->name, key, info->real_deals);
            sdsfree(key);
            if (ret < 0) {
                redisFree(context);
                dict_release_iterator(iter);
                return ret;
            }
        }

        if (pipeline_count >= settings.pipeline_len_max) {
            ret = pipeline_excute(context);
            if (ret < 0) {
                redisFree(context);
                dict_release_iterator(iter);
                return ret;
            }
        }
    }
    dict_release_iterator(iter);

    ret = pipeline_excute(context);
    if (ret < 0) {
        redisFree(context);
        return ret;
    }

    ret = flush_deals_offset(context);
    if (ret < 0) {
        redisFree(context);
        return -__LINE__;
    }
    ret = flush_indexs_offset(context);
    if (ret < 0) {
        redisFree(context);
        return -__LINE__;
    }
    log_info("flush deal_offset: %ld, index_offset: %ld, work_id: %d", last_deals_offset, last_indexs_offset, worker_id);

    last_flush = current_timestamp();
    redisFree(context);
    profile_inc("flush_market_success", 1);

    return 0;
}

static void clear_dict(dict_t *dict, time_t start)
{
    dict_iterator *iter = dict_get_iterator(dict);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        time_t timestamp = (uintptr_t)entry->key;
        if (timestamp < start) {
            dict_delete(dict, entry->key);
        }
    }
    dict_release_iterator(iter);
}

static void clear_kline(void)
{
    time_t now = time(NULL);
    dict_iterator *iter = dict_get_iterator(dict_market);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct market_info *info = entry->val;
        clear_dict(info->sec, now - settings.sec_max);
        clear_dict(info->min, now / 60 * 60 - settings.min_max * 60);
        clear_dict(info->hour, now / 3600 * 3600 - settings.hour_max * 3600);
    }
    dict_release_iterator(iter);
}

static void on_flush_timer(nw_timer *timer, void *privdata)
{
    double start = current_timestamp();
    int ret = flush_market();
    if (ret < 0) {
        log_fatal("flush_market fail: %d", ret);
    }
    log_info("flush market cost: %.6f", current_timestamp() - start);
}

static void on_clear_timer(nw_timer *timer, void *privdata)
{
    clear_kline();
}

static int clear_key(redisContext *context, const char *key, time_t end)
{
    redisReply *reply = redisCmd(context, "HGETALL %s", key);
    if (reply == NULL)
        return -__LINE__;
    for (size_t i = 0; i < reply->elements; i += 2) {
        time_t timestamp = strtol(reply->element[i]->str, NULL, 0);
        if (timestamp >= end)
            continue;
        redisReply *r = redisCmd(context, "HDEL %s %ld", key, timestamp);
        if (r == NULL) {
            freeReplyObject(reply);
            return -__LINE__;
        }
        freeReplyObject(r);
    }
    freeReplyObject(reply);

    return 0;
}

static int clear_redis(void)
{
    redisContext *context = get_redis_connection();
    if (context == NULL)
        return 1;
    time_t now = time(NULL);
    dict_iterator *iter = dict_get_iterator(dict_market);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct market_info *info = entry->val;
        sds key = sdsempty();
        key = sdscatprintf(key, "k:%s:1s", info->name);
        clear_key(context, key, now - settings.sec_max);
        sdsclear(key);

        key = sdscatprintf(key, "k:%s:1m", info->name);
        clear_key(context, key, now / 60 * 60 - settings.min_max * 60);
        sdsclear(key);

        key = sdscatprintf(key, "k:%s:1h", info->name);
        clear_key(context, key, now / 3600 * 3600 - settings.hour_max * 3600);
        sdsfree(key);
    }
    redisFree(context);

    return 0;
}

static void on_redis_timer(nw_timer *timer, void *privdata)
{
    int pid = fork();
    if (pid == 0) {
        _exit(clear_redis());
    }
}

static int update_market_list(json_t *market_list)
{
    if (market_list == NULL)
        return -__LINE__;

    double start = current_timestamp();
    size_t market_count = json_array_size(market_list);
    for (size_t i = 0; i < market_count; ++i) {
        json_t *item = json_array_get(market_list, i);
        const char *name = json_string_value(json_object_get(item, "name"));
        const char *stock = json_string_value(json_object_get(item, "stock"));
        const char *money = json_string_value(json_object_get(item, "money"));
        if (get_market_id(name) != worker_id && worker_id != settings.worker_num)
            continue;
        struct market_info *info = market_query(name);
        if (info == NULL) {
            info = create_market(name, stock, money, TRADE_TYPE_NORMAL);
            if (info == NULL) {
                return -__LINE__;
            }
            log_info("add market: %s", name);
        } else {
            if (info->stock == NULL) {
                info->stock = strdup(stock);
            }
            if (info->money == NULL) {
                info->money = strdup(money);
            }
        }

        if (worker_id == settings.worker_num) {
            char *trade_zone_name = convert_trade_zone_name(money);
            struct market_info *info = market_query(trade_zone_name);
            if (info == NULL) {
                info = create_market(trade_zone_name, NULL, NULL, TRADE_TYPE_ZONE);
                if (info == NULL) {
                    return -__LINE__;
                }
                log_info("add market: %s", trade_zone_name);
            }

            char *trade_zone_real_name = convert_trade_zone_real_name(money);
            info = market_query(trade_zone_real_name);
            if (info == NULL) {
                info = create_market(trade_zone_real_name, NULL, NULL, TRADE_TYPE_ZONE);
                if (info == NULL) {
                    return -__LINE__;
                }
                log_info("add market: %s", trade_zone_real_name);
            }
        }
    }
    double end = current_timestamp();
    log_info("update_market_list cost: %f, market count: %zu", end - start, market_count);
    return 0;
}

static int update_index_list(json_t *index_list)
{
    if (index_list == NULL)
        return -__LINE__;

    const char *key;
    json_t *val;
    int index_count = 0;
    double start = current_timestamp();
    json_object_foreach(index_list, key, val) {
        index_count++;
        char *index_name = convert_index_name(key);
        if (get_market_id(index_name) != worker_id)
            continue;
        struct market_info *info = market_query(index_name);
        if (info == NULL) {
            info = create_market(index_name, NULL, NULL, TRADE_TYPE_INDEX);
            if (info == NULL) {
                return -__LINE__;
            }
            log_info("add market: %s", index_name);
        }
    }
    double end = current_timestamp();
    log_info("update_index_list cost: %f, market count: %d", end - start, index_count);
    return 0;
}

static void on_market_timer(nw_timer *timer, void *privdata)
{
    int ret;
    ret = add_request("market.list", update_market_list);
    if (ret < 0) {
        log_error("update_market_list fail: %d", ret);
    }
    ret = add_request("index.list", update_index_list);
    if (ret < 0) {
        log_error("update_index_list fail: %d", ret);
    }
}

int init_message(int id)
{
    worker_id = id;

    int ret;
    ret = init_request();
    if (ret < 0) {
        return ret;
    }

    ret = init_market();
    if (ret < 0) {
        return ret;
    }

    int64_t offset = 0;
    last_deals_offset = get_offset(DEALS_OFFSET_KEY);
    if (last_deals_offset < 0) {
        return -__LINE__;
    }
    offset = last_deals_offset == 0 ? RD_KAFKA_OFFSET_END : last_deals_offset + 1;
    deals = kafka_consumer_create(settings.brokers, TOPIC_DEAL, 0, offset, on_deals_message);
    if (deals == NULL) {
        return -__LINE__;
    }

    last_indexs_offset = get_offset(INDEX_OFFSET_KEY);
    if (last_indexs_offset < 0) {
        return -__LINE__;
    }
    offset = last_indexs_offset == 0 ? RD_KAFKA_OFFSET_END : last_indexs_offset + 1;
    indexs = kafka_consumer_create(settings.brokers, TOPIC_INDEX, 0, offset, on_indexs_message);
    if (indexs == NULL) {
        return -__LINE__;
    }

    log_info("work_id: %d, max_deals_id: %ld, last_min_time: %ld, last_hour_time: %ld, last_day_time: %ld, deals_offset: %ld, indexs_offset: %ld", 
            worker_id, max_deals_id, last_min_time, last_hour_time, last_day_time, last_deals_offset, last_indexs_offset);
    log_stderr("work_id: %d, max_deals_id: %ld, last_min_time: %ld, last_hour_time: %ld, last_day_time: %ld, deals_offset: %ld, indexs_offset: %ld", 
            worker_id, max_deals_id, last_min_time, last_hour_time, last_day_time, last_deals_offset, last_indexs_offset);

    nw_timer_set(&flush_timer, 10, true, on_flush_timer, NULL);
    nw_timer_start(&flush_timer);

    nw_timer_set(&clear_timer, 3600, true, on_clear_timer, NULL);
    nw_timer_start(&clear_timer);

    nw_timer_set(&redis_timer, 86400, true, on_redis_timer, NULL);
    nw_timer_start(&redis_timer);

    nw_timer_set(&market_timer, 60, true, on_market_timer, NULL);
    nw_timer_start(&market_timer);

    return 0;
}

int get_market_id(const char *market)
{
    if (strstr(market, TRADE_ZONE_REAL_SUFFIX) != NULL) {
        return settings.worker_num;
    } else if (strstr(market, TRADE_ZONE_SUFFIX) != NULL) {
        return settings.worker_num;
    } else {
        uint32_t hash = dict_generic_hash_function(market, strlen(market));
        return hash % settings.worker_num;
    }
}

bool market_exist(const char *market)
{
    struct market_info *info = market_query(market);
    if (info)
        return true;
    return false;
}

static struct kline_info *get_last_kline(dict_t *dict, time_t start, time_t end, int interval)
{
    for (; start >= end; start -= interval) {
        void *key = (void *)(uintptr_t)start;
        dict_entry *entry = dict_find(dict, key);
        if (entry) {
            return entry->val;
        }
    }

    return NULL;
}

json_t *get_market_status(const char *market, int period)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    struct kline_info *kinfo = NULL;
    time_t now = time(NULL);
    time_t start = now - period;
    time_t start_min = start / 60 * 60 + 60;
    time_t start_hour = start / 3600 * 3600 + 3600;

    for (time_t timestamp = start; timestamp < start_min; timestamp++) {
        void *key = (void *)(uintptr_t)timestamp;
        dict_entry *entry = dict_find(info->sec, key);
        if (!entry)
            continue;
        struct kline_info *sinfo = entry->val;
        if (kinfo == NULL) {
            kinfo = kline_info_new(sinfo->open);
        }
        kline_info_merge(kinfo, sinfo);
    }

    for (time_t timestamp = start_min; timestamp < start_hour; timestamp += 60) {
        void *key = (void *)(uintptr_t)timestamp;
        dict_entry *entry = dict_find(info->min, key);
        if (!entry)
            continue;
        struct kline_info *sinfo = entry->val;
        if (kinfo == NULL) {
            kinfo = kline_info_new(sinfo->open);
        }
        kline_info_merge(kinfo, sinfo);
    }

    for (time_t timestamp = start_hour; timestamp < now; timestamp += 3600) {
        void *key = (void *)(uintptr_t)timestamp;
        dict_entry *entry = dict_find(info->hour, key);
        if (!entry)
            continue;
        struct kline_info *sinfo = entry->val;
        if (kinfo == NULL) {
            kinfo = kline_info_new(sinfo->open);
        }
        kline_info_merge(kinfo, sinfo);
    }

    if (kinfo == NULL)
        kinfo = kline_info_new(mpd_zero);

    json_t *result = json_object();
    if (info->trade_type == TRADE_TYPE_ZONE) {
        json_object_set_new_mpd(result, "last", mpd_zero);
        json_object_set_new_mpd(result, "open", mpd_zero);
        json_object_set_new_mpd(result, "close", mpd_zero);
        json_object_set_new_mpd(result, "high", mpd_zero);
        json_object_set_new_mpd(result, "low", mpd_zero);
        json_object_set_new_mpd(result, "volume", mpd_zero);
        json_object_set_new_mpd(result, "sell_total", mpd_zero);
        json_object_set_new_mpd(result, "buy_total", mpd_zero);
    } else {
        json_object_set_new_mpd(result, "last", info->last);
        json_object_set_new_mpd(result, "open", kinfo->open);
        json_object_set_new_mpd(result, "close", kinfo->close);
        json_object_set_new_mpd(result, "high", kinfo->high);
        json_object_set_new_mpd(result, "low", kinfo->low);
        json_object_set_new_mpd(result, "volume", kinfo->volume);
        json_object_set_new_mpd(result, "sell_total", info->sell_total);
        json_object_set_new_mpd(result, "buy_total", info->buy_total);
    }
    json_object_set_new(result, "period", json_integer(period));
    json_object_set_new_mpd(result, "deal", kinfo->deal);

    kline_info_free(kinfo);

    return result;
}

static int append_kinfo(json_t *result, time_t timestamp, struct kline_info *kinfo, const char *market)
{
    json_t *unit = json_array();
    json_array_append_new(unit, json_integer(timestamp));
    json_array_append_new_mpd(unit, kinfo->open);
    json_array_append_new_mpd(unit, kinfo->close);
    json_array_append_new_mpd(unit, kinfo->high);
    json_array_append_new_mpd(unit, kinfo->low);
    json_array_append_new_mpd(unit, kinfo->volume);
    json_array_append_new_mpd(unit, kinfo->deal);
    json_array_append_new(unit, json_string(market));
    json_array_append_new(result, unit);

    return 0;
}

json_t *get_market_kline_sec(const char *market, time_t start, time_t end, int interval)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    json_t *result = json_array();
    time_t now = time(NULL);
    if (start < now - settings.sec_max)
        start = now - settings.sec_max;
    start = start / interval * interval;
    struct kline_info *kbefor = get_last_kline(info->sec, start - 1, now - settings.sec_max, 1);
    struct kline_info *klast = kbefor;
    for (; start <= end; start += interval) {
        struct kline_info *kinfo = NULL;
        for (int i = 0; i < interval; ++i) {
            time_t timestamp = start + i;
            void *key = (void *)(uintptr_t)timestamp;
            dict_entry *entry = dict_find(info->sec, key);
            if (entry == NULL)
                continue;
            struct kline_info *item = entry->val;
            if (kinfo == NULL)
                kinfo = kline_info_new(item->open);
            kline_info_merge(kinfo, item);
        }
        if (kinfo == NULL) {
            if (klast == NULL) {
                continue;
            }
            kinfo = kline_info_new(klast->close);
        }
        append_kinfo(result, start, kinfo, market);
        if (klast && klast != kbefor)
            kline_info_free(klast);
        klast = kinfo;
    }
    if (klast && klast != kbefor)
        kline_info_free(klast);

    return result;
}

json_t *get_market_kline_min(const char *market, time_t start, time_t end, int interval)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    json_t *result = json_array();
    time_t now = time(NULL);
    time_t start_min = now / 60 * 60 - settings.min_max * 60;
    if (start < start_min)
        start = start_min;
    start = start / interval * interval;
    struct kline_info *kbefor = get_last_kline(info->min, start - 60, start_min, 60);
    struct kline_info *klast = kbefor;
    int step = interval / 60;
    for (; start <= end; start += interval) {
        struct kline_info *kinfo = NULL;
        for (int i = 0; i < step; ++i) {
            time_t timestamp = start + i * 60;
            void *key = (void *)(uintptr_t)timestamp;
            dict_entry *entry = dict_find(info->min, key);
            if (entry == NULL)
                continue;
            struct kline_info *item = entry->val;
            if (kinfo == NULL)
                kinfo = kline_info_new(item->open);
            kline_info_merge(kinfo, item);
        }
        if (kinfo == NULL) {
            if (klast == NULL) {
                continue;
            }
            kinfo = kline_info_new(klast->close);
        }
        append_kinfo(result, start, kinfo, market);
        if (klast && klast != kbefor)
            kline_info_free(klast);
        klast = kinfo;
    }
    if (klast && klast != kbefor)
        kline_info_free(klast);

    return result;
}

json_t *get_market_kline_hour(const char *market, time_t start, time_t end, int interval)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    json_t *result = json_array();
    time_t now = time(NULL);
    time_t start_min = now / 3600 * 3600 - settings.hour_max * 3600;
    if (start < start_min)
        start = start_min;
    time_t base = start / 86400 * 86400;
    while ((base + interval) <= start)
        base += interval;
    start = base;

    struct kline_info *kbefor = get_last_kline(info->hour, start - 3600, start_min, 3600);
    struct kline_info *klast = kbefor;
    int step = interval / 3600;
    for (; start <= end; start += interval) {
        struct kline_info *kinfo = NULL;
        for (int i = 0; i < step; ++i) {
            time_t timestamp = start + i * 3600;
            void *key = (void *)(uintptr_t)timestamp;
            dict_entry *entry = dict_find(info->hour, key);
            if (entry == NULL)
                continue;
            struct kline_info *item = entry->val;
            if (kinfo == NULL)
                kinfo = kline_info_new(item->open);
            kline_info_merge(kinfo, item);
        }
        if (kinfo == NULL) {
            if (klast == NULL) {
                continue;
            }
            kinfo = kline_info_new(klast->close);
        }
        append_kinfo(result, start, kinfo, market);
        if (klast && klast != kbefor)
            kline_info_free(klast);
        klast = kinfo;
    }
    if (klast && klast != kbefor)
        kline_info_free(klast);

    return result;
}

json_t *get_market_kline_day(const char *market, time_t start, time_t end, int interval)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    json_t *result = json_array();
    start = start / interval * interval;

    struct kline_info *kbefor = get_last_kline(info->day, start - 86400, start - 86400 * 30, 86400);
    struct kline_info *klast = kbefor;
    int step = interval / 86400;
    for (; start <= end; start += interval) {
        struct kline_info *kinfo = NULL;
        for (int i = 0; i < step; ++i) {
            time_t timestamp = start + i * 86400;
            void *key = (void *)(uintptr_t)timestamp;
            dict_entry *entry = dict_find(info->day, key);
            if (entry == NULL)
                continue;
            struct kline_info *item = entry->val;
            if (kinfo == NULL)
                kinfo = kline_info_new(item->open);
            kline_info_merge(kinfo, item);
        }
        if (kinfo == NULL) {
            if (klast == NULL) {
                continue;
            }
            kinfo = kline_info_new(klast->close);
        }
        append_kinfo(result, start, kinfo, market);
        if (klast && klast != kbefor)
            kline_info_free(klast);
        klast = kinfo;
    }
    if (klast && klast != kbefor)
        kline_info_free(klast);

    return result;
}

json_t *get_market_kline_week(const char *market, time_t start, time_t end, int interval)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    json_t *result = json_array();
    time_t base = start / interval * interval - 3 * 86400;
    while ((base + interval) <= start)
        base += interval;
    start = base;

    struct kline_info *kbefor = get_last_kline(info->day, start - 86400, start - 86400 * 30, 86400);
    struct kline_info *klast = kbefor;
    int step = interval / 86400;
    for (; start <= end; start += interval) {
        struct kline_info *kinfo = NULL;
        for (int i = 0; i < step; ++i) {
            time_t timestamp = start + i * 86400;
            void *key = (void *)(uintptr_t)timestamp;
            dict_entry *entry = dict_find(info->day, key);
            if (entry == NULL)
                continue;
            struct kline_info *item = entry->val;
            if (kinfo == NULL)
                kinfo = kline_info_new(item->open);
            kline_info_merge(kinfo, item);
        }
        if (kinfo == NULL) {
            if (klast == NULL) {
                continue;
            }
            kinfo = kline_info_new(klast->close);
        }
        append_kinfo(result, start, kinfo, market);
        if (klast && klast != kbefor)
            kline_info_free(klast);
        klast = kinfo;
    }
    if (klast && klast != kbefor)
        kline_info_free(klast);

    return result;
}

static time_t get_this_month(int tm_year, int tm_mon)
{
    struct tm mtm;
    memset(&mtm, 0, sizeof(mtm));
    mtm.tm_year = tm_year;
    mtm.tm_mon  = tm_mon;
    mtm.tm_mday = 1;
    return mktime(&mtm);
}

static time_t get_next_month(int *tm_year, int *tm_mon)
{
    if (*tm_mon == 11) {
        *tm_mon = 0;
        *tm_year += 1;
    } else {
        *tm_mon += 1;
    }
    struct tm mtm;
    memset(&mtm, 0, sizeof(mtm));
    mtm.tm_year = *tm_year;
    mtm.tm_mon  = *tm_mon;
    mtm.tm_mday = 1;
    return mktime(&mtm);
}

json_t *get_market_kline_month(const char *market, time_t start, time_t end, int interval)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    json_t *result = json_array();
    struct tm *timeinfo = localtime(&start);
    int tm_year = timeinfo->tm_year;
    int tm_mon  = timeinfo->tm_mon;
    time_t mon_start = get_this_month(tm_year, tm_mon);

    struct kline_info *kbefor = get_last_kline(info->day, mon_start - 86400, start - 86400 * 30, 86400);
    struct kline_info *klast = kbefor;
    for (; mon_start <= end; ) {
        struct kline_info *kinfo = NULL;
        time_t mon_next = get_next_month(&tm_year, &tm_mon);
        time_t timestamp = mon_start;
        for (; timestamp < mon_next && timestamp <= end; timestamp += 86400) {
            void *key = (void *)(uintptr_t)timestamp;
            dict_entry *entry = dict_find(info->day, key);
            if (entry == NULL)
                continue;
            struct kline_info *item = entry->val;
            if (kinfo == NULL)
                kinfo = kline_info_new(item->open);
            kline_info_merge(kinfo, item);
        }
        if (kinfo == NULL) {
            if (klast == NULL) {
                mon_start = mon_next;
                continue;
            }
            kinfo = kline_info_new(klast->close);
        }
        append_kinfo(result, mon_start, kinfo, market);
        mon_start = mon_next;
        if (klast && klast != kbefor)
            kline_info_free(klast);
        klast = kinfo;
    }
    if (klast && klast != kbefor)
        kline_info_free(klast);

    return result;
}

json_t *get_market_deals(const char *market, int limit, uint64_t last_id)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    int count = 0;
    json_t *result = json_array();
    list_iter *iter = list_get_iterator(info->deals_json, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        json_t *deal = node->value;
        uint64_t id = json_integer_value(json_object_get(deal, "id"));
        if (id <= last_id) {
            break;
        }
        json_t *item = json_object();
        json_object_set(item, "id", json_object_get(deal, "id"));
        json_object_set(item, "time", json_object_get(deal, "time"));
        json_object_set(item, "type", json_object_get(deal, "type"));
        json_object_set(item, "price", json_object_get(deal, "price"));
        json_object_set(item, "amount", json_object_get(deal, "amount"));
        json_array_append_new(result, item);
        count += 1;
        if (count == limit) {
            break;
        }
    }
    list_release_iterator(iter);

    return result;
}

json_t *get_market_deals_ext(const char *market, int limit, uint64_t last_id)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    int count = 0;
    json_t *result = json_array();
    list_iter *iter = list_get_iterator(info->real_deals_json, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        json_t *deal = node->value;
        uint64_t id = json_integer_value(json_object_get(deal, "id"));
        if (id <= last_id) {
            break;
        }
        json_array_append(result, deal);
        count += 1;
        if (count == limit) {
            break;
        }
    }
    list_release_iterator(iter);

    return result;
}

mpd_t  *get_market_last_price(const char *market)
{
    struct market_info *info = market_query(market);
    if (info == NULL)
        return NULL;

    return info->last;
}

