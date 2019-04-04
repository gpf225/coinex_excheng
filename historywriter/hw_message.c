/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_message.h"
# include "hw_dispatcher.h"
# include "hw_persist.h"
# include "ut_profile.h"

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_stops;
static kafka_consumer_t *kafka_orders;
static kafka_consumer_t *kafka_balances;

static int64_t last_order_offset = 0;
static int64_t last_stop_offset = 0;
static int64_t last_deal_offset = 0;
static int64_t last_balance_offset = 0;

static int message_control = 0;

# define CHECK_MESSAGE_CONTROL          \
    if (message_control) {              \
        return ;                        \
    }

static void on_deals_message(sds message, int64_t offset)
{
    CHECK_MESSAGE_CONTROL

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
    CHECK_MESSAGE_CONTROL

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
    CHECK_MESSAGE_CONTROL

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
    CHECK_MESSAGE_CONTROL

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

int init_message(void)
{
    last_order_offset = load_last_order_offset();
    last_stop_offset = load_last_stop_offset();
    last_deal_offset = load_last_deal_offset();
    last_balance_offset = load_last_balance_offset();

    settings.orders.offset = last_order_offset + 1;
    kafka_orders = kafka_consumer_create(&settings.orders, on_orders_message);
    if (kafka_orders == NULL) {
        return -__LINE__;
    }

    settings.stops.offset = last_stop_offset + 1;
    kafka_stops = kafka_consumer_create(&settings.stops, on_stops_message);
    if (kafka_stops == NULL) {
        return -__LINE__;
    }

    settings.deals.offset = last_deal_offset + 1;
    kafka_deals = kafka_consumer_create(&settings.deals, on_deals_message);
    if (kafka_deals == NULL) {
        return -__LINE__;
    }

    settings.balances.offset = last_balance_offset + 1;
    kafka_balances = kafka_consumer_create(&settings.balances, on_balances_message);
    if (kafka_balances == NULL) {
        return -__LINE__;
    }

    return 0;
}

int64_t get_last_order_offset(void)
{
    return last_order_offset;
}

int64_t get_last_stop_offset(void)
{
    return last_stop_offset;
}

int64_t get_last_deal_offset(void)
{
    return last_deal_offset;
}

int64_t get_last_balance_offset(void)
{
    return last_balance_offset;
}

void message_stop(int flag)
{
    if (flag <= 0) {
        return ;
    }
    log_info("message stop, flag:%d.", flag);  
    message_control = flag;
}

sds message_status(sds reply)
{
    reply = sdscatprintf(reply, "last_order_offset:%"PRIu64"\n", last_order_offset);
    reply = sdscatprintf(reply, "last_stop_offset:%"PRIu64"\n", last_stop_offset);
    reply = sdscatprintf(reply, "last_deal_offset:%"PRIu64"\n", last_deal_offset);
    reply = sdscatprintf(reply, "last_balance_offset:%"PRIu64"\n", last_balance_offset);
    return reply;
}
