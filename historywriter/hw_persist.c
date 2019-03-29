/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_persist.h"
# include "hw_message.h"

static const char *key_kafka_offset_orders   = "k:hw:kafka:offset:orders";
static const char *key_kafka_offset_stops    = "k:hw:kafka:offset:stops";
static const char *key_kafka_offset_deals    = "k:hw:kafka:offset:deals";
static const char *key_kafka_offset_balances = "k:hw:kafka:offset:balances";

static nw_timer timer;

static redisContext *get_redis_connection()
{
    return redis_connect(&settings.redis);
}

static int flush_int64(redisContext *context, const char *key, int64_t offset)
{
    redisReply *reply = redisCmd(context, "SET %s %"PRIi64, key, offset);
    if (reply == NULL) {
        log_error("save offset failed, key:%s offset:%"PRIi64, key, offset);
        return -__LINE__;
    }
    freeReplyObject(reply);

    return 0;
}

static void on_timer(nw_timer *t, void *privdata)
{
    redisContext *context = get_redis_connection();
    if (context == NULL) {
        log_fatal("could not connect to redis");
        return ;
    }

    flush_int64(context, key_kafka_offset_orders, get_last_order_offset());
    flush_int64(context, key_kafka_offset_stops, get_last_stop_offset());
    flush_int64(context, key_kafka_offset_deals, get_last_deal_offset());
    flush_int64(context, key_kafka_offset_balances, get_last_balance_offset());
    redisFree(context);
}

static int64_t load_int64(const char *key)
{
    redisContext *context = get_redis_connection();
    if (context == NULL) {
        log_error("load_int64 failed, key:%s", key);
        return 0;
    }

    redisReply *reply = redisCmd(context, "GET %s", key);
    if (reply == NULL) {
        log_error("load_int64 failed, reply null,  key:%s", key);
        redisFree(context);
        return 0;
    }

    int64_t offset = 0;
    if (reply->type == REDIS_REPLY_STRING) {
        offset = strtoll(reply->str, NULL, 0);
    }
    freeReplyObject(reply);
    redisFree(context);

    return offset;
}

int init_persist(void)
{
    nw_timer_set(&timer, settings.flush_his_interval, true, on_timer, NULL);
    nw_timer_start(&timer);
    return 0;
}

int64_t load_last_order_offset(void)
{
    return load_int64(key_kafka_offset_orders);
}

int64_t load_last_stop_offset(void)
{
    return load_int64(key_kafka_offset_stops);
}

int64_t load_last_deal_offset(void)
{
    return load_int64(key_kafka_offset_deals);
}

int64_t load_last_balance_offset(void)
{
    return load_int64(key_kafka_offset_balances);
}