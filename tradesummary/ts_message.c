/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "ts_config.h"
# include "ts_message.h"

static nw_timer timer;

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_orders;

static int64_t kafka_deals_offset = 0;
static int64_t kafka_orders_offset = 0;

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

    double timestamp = json_real_value(json_object_get(order, "ctime"));
    uint32_t user_id = json_integer_value(json_object_get(order, "user"));
    uint32_t order_type = json_integer_value(json_object_get(order, "type"));
    uint32_t order_side = json_integer_value(json_object_get(order, "side"));
    if (timestamp == 0 || user_id == 0 || order_type == 0 || order_side == 0) {
        log_error("invalid message: %s, offset: %"PRIi64, message, offset);
        goto cleanup;
    }

    time_t time_hour = ((int)timestamp) / 3600 * 3600;
    if (last_message_hour != 0 && last_message_hour != time_hour)
        set_message_offset("orders", time_hour, last_offset);
    last_message_hour = time_hour;
    last_offset = offset;
    kafka_orders_offset = offset;

cleanup:
    json_decref(obj);
}

static void report_offset(kafka_consumer_t *consumer, int64_t current_offset)
{
    int64_t low = 0;
    int64_t high = 0;
    if (kafka_query_offset(consumer, &low, &high) < 0) {
        log_error("kafka_query_offset %s fail", consumer->topic);
    } else {
        log_info("hightest offset: %"PRIi64", currently: %"PRIi64", gap: %"PRIi64, high, current_offset, high - current_offset);
    }
}

static void on_timer(nw_timer *timer, void *privdata)
{
    report_offset(kafka_deals, kafka_deals_offset);
    report_offset(kafka_orders, kafka_orders_offset);
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

    nw_timer_set(&timer, 5, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

