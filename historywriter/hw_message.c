/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_message.h"
# include "hw_dispatcher.h"

static nw_timer timer;

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_stops;
static kafka_consumer_t *kafka_orders;
static kafka_consumer_t *kafka_balances;

static int64_t last_deal_offset = 0;
static int64_t last_stop_offset = 0;
static int64_t last_order_offset = 0;
static int64_t last_balance_offset = 0;

static char *get_offset_full_key(const char *type)
{
    static char buf[100];
    snprintf(buf, sizeof(buf), "k:hw:kafka:offset:%s", type);
    return buf;
}

static int load_offset(redisContext *context, const char *type, int64_t *value)
{
    redisReply *reply = redisCmd(context, "GET %s", get_offset_full_key(type));
    if (reply == NULL)
        return -__LINE__;

    *value = 0;
    if (reply->type == REDIS_REPLY_STRING) {
        *value = strtoll(reply->str, NULL, 0);
    }
    freeReplyObject(reply);
    return 0;
}

static int dump_offset(redisContext *context, const char *type, int64_t value)
{
    redisReply *reply = redisCmd(context, "SET %s %"PRIi64, get_offset_full_key(type), value);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);
    return 0;
}

static int message_offset_load(void)
{
    redisContext *context = redis_connect(&settings.redis);
    if (context == NULL)
        return -__LINE__;

    load_offset(context, "deals", &last_deal_offset);
    load_offset(context, "stops", &last_stop_offset);
    load_offset(context, "orders", &last_order_offset);
    load_offset(context, "balances", &last_balance_offset);

    redisFree(context);
    return 0;
}

static int message_offset_dump(void)
{
    redisContext *context = redis_connect(&settings.redis);
    if (context == NULL)
        return -__LINE__;

    dump_offset(context, "deals", last_deal_offset);
    dump_offset(context, "stops", last_stop_offset);
    dump_offset(context, "orders", last_order_offset);
    dump_offset(context, "balances", last_balance_offset);

    redisFree(context);
    return 0;
}

static void on_deals_message(sds message, int64_t offset)
{
    log_trace("deal message: %s", message);
    profile_inc("message_deal ", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid deal message: %s", message);
        return;
    }

    int ret = dispatch_deal(msg);
    if (ret < 0) {
        log_error("dispatch_deal: %s fail: %d", message, ret);
        json_decref(msg);
        return ;
    }

    json_decref(msg);
    last_deal_offset = offset;
}

static void on_stops_message(sds message, int64_t offset)
{
    log_trace("stop message: %s", message);
    profile_inc("message_stop", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid stop message: %s", message);
        return;
    }

    int ret = dispatch_stop(msg);
    if (ret < 0) {
        log_error("dispatch_stop: %s fail: %d", message, ret);
        json_decref(msg);
        return ;
    }

    json_decref(msg);
    last_stop_offset = offset;
}

static void on_orders_message(sds message, int64_t offset)
{
    log_trace("order message: %s", message);
    profile_inc("message_order", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid order message: %s", message);
        return;
    }

    int ret = dispatch_order(msg);
    if (ret < 0) {
        log_error("dispatch_order: %s fail: %d", message, ret);
        json_decref(msg);
        return ;
    }

    json_decref(msg);
    last_order_offset = offset;
}

static void on_balances_message(sds message, int64_t offset)
{
    log_trace("balance message: %s", message);
    profile_inc("message_balance", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid balance message: %s", message);
        return;
    }

    int ret = dispatch_balance(msg);
    if (ret < 0) {
        log_error("dispatch_balance: %s fail: %d", message, ret);
        json_decref(msg);
        return ;
    }

    json_decref(msg);
    last_balance_offset = offset;
}

static void on_timer(nw_timer *timer, void *privdata)
{
    message_offset_dump();
}

int init_message(void)
{
    int ret = message_offset_load();
    if (ret < 0)
        return ret;

    settings.deals.offset = last_deal_offset + 1;
    kafka_deals = kafka_consumer_create(&settings.deals, on_deals_message);
    if (kafka_deals == NULL) {
        return -__LINE__;
    }

    settings.stops.offset = last_stop_offset + 1;
    kafka_stops = kafka_consumer_create(&settings.stops, on_stops_message);
    if (kafka_stops == NULL) {
        return -__LINE__;
    }

    settings.orders.offset = last_order_offset + 1;
    kafka_orders = kafka_consumer_create(&settings.orders, on_orders_message);
    if (kafka_orders == NULL) {
        return -__LINE__;
    }

    settings.balances.offset = last_balance_offset + 1;
    kafka_balances = kafka_consumer_create(&settings.balances, on_balances_message);
    if (kafka_balances == NULL) {
        return -__LINE__;
    }

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

void suspend_message(void)
{
    kafka_consumer_suspend(kafka_deals);
    kafka_consumer_suspend(kafka_stops);
    kafka_consumer_suspend(kafka_orders);
    kafka_consumer_suspend(kafka_balances);
}

void resume_message(void)
{
    kafka_consumer_resume(kafka_deals);
    kafka_consumer_resume(kafka_stops);
    kafka_consumer_resume(kafka_orders);
    kafka_consumer_resume(kafka_balances);
}

void fini_message(void)
{
    suspend_message();
    message_offset_dump();
}

sds message_status(sds reply)
{
    reply = sdscatprintf(reply, "last deal offset: %"PRIu64"\n", last_deal_offset);
    reply = sdscatprintf(reply, "last stop offset: %"PRIu64"\n", last_stop_offset);
    reply = sdscatprintf(reply, "last order offset: %"PRIu64"\n", last_order_offset);
    reply = sdscatprintf(reply, "last balance hffset: %"PRIu64"\n", last_balance_offset);
    return reply;
}

