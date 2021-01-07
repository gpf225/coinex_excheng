/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/28, create
 */

# include "aw_config.h"
# include "aw_message.h"
# include "aw_deals.h"
# include "aw_asset.h"
# include "aw_order.h"
# include "aw_index.h"
# include "aw_server.h"
# include "aw_auth.h"
# include "aw_notice.h"

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_stops;
static kafka_consumer_t *kafka_orders;
static kafka_consumer_t *kafka_indexs;
static kafka_consumer_t *kafka_balances;
static kafka_consumer_t *kafka_notice;

static int64_t kafka_deals_offset = 0;
static int64_t kafka_stops_offset = 0;
static int64_t kafka_orders_offset = 0;
static int64_t kafka_indexs_offset = 0;
static int64_t kafka_balances_offset = 0;
static int64_t kafka_notice_offset = 0;
static nw_timer report_timer;

static int process_deals_message(json_t *msg)
{
    uint64_t ask_order_id = json_integer_value(json_object_get(msg, "ask_id"));
    uint64_t bid_order_id = json_integer_value(json_object_get(msg, "bid_id"));
    uint32_t ask_user_id = json_integer_value(json_object_get(msg, "ask_user_id"));
    uint32_t bid_user_id = json_integer_value(json_object_get(msg, "bid_user_id"));
    const char *ask_client_id  = json_string_value(json_object_get(msg, "ask_client_id"));
    const char *bid_client_id  = json_string_value(json_object_get(msg, "bid_client_id"));
    uint64_t id = json_integer_value(json_object_get(msg, "id"));
    double timestamp = json_real_value(json_object_get(msg, "timestamp"));
    const char *market = json_string_value(json_object_get(msg, "market"));
    const char *amount = json_string_value(json_object_get(msg, "amount"));
    const char *price  = json_string_value(json_object_get(msg, "price"));

    deals_new(ask_user_id, ask_order_id, id, timestamp, MARKET_TRADE_SIDE_SELL, ask_client_id, market, amount, price);
    deals_new(bid_user_id, bid_order_id, id, timestamp, MARKET_TRADE_SIDE_BUY,  bid_client_id, market, amount, price);

    return 0;
}

static void on_deals_message(sds message, int64_t offset)
{
    kafka_deals_offset = offset;
    log_trace("deal message: %s", message);
    profile_inc("message_deal", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid deal message: %s", message);
        return;
    }

    int ret = process_deals_message(msg);
    if (ret < 0) {
        log_error("process_deals_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

static int process_stops_message(json_t *msg)
{
    int event = json_integer_value(json_object_get(msg, "event"));
    if (event == 0)
        return -__LINE__;
    json_t *order = json_object_get(msg, "order");
    if (order == NULL)
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_object_get(order, "user"));
    if (user_id == 0)
        return -__LINE__;

    order_on_update_stop(user_id, event, order);

    return 0;
}

static void on_stops_message(sds message, int64_t offset)
{
    kafka_stops_offset = offset;
    log_trace("stop message: %s", message);
    profile_inc("message_stop", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid stops message: %s", message);
        return;
    }

    int ret = process_stops_message(msg);
    if (ret < 0) {
        log_error("process_stops_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

static int process_orders_message(json_t *msg)
{
    int event = json_integer_value(json_object_get(msg, "event"));
    if (event == 0)
        return -__LINE__;

    json_t *order = json_object_get(msg, "order");
    json_t *balance = json_object_get(msg, "balance");
    if (order == NULL || balance == NULL)
        return -__LINE__;

    uint32_t user_id = json_integer_value(json_object_get(order, "user"));
    uint32_t account = json_integer_value(json_object_get(order, "account"));
    double timestamp = json_real_value(json_object_get(order, "mtime"));

    const char *stock = json_string_value(json_object_get(msg, "stock"));
    const char *money = json_string_value(json_object_get(msg, "money"));

    const char *stock_available = json_string_value(json_object_get(balance, "stock_available"));
    const char *stock_frozen = json_string_value(json_object_get(balance, "stock_frozen"));

    const char *money_available = json_string_value(json_object_get(balance, "money_available"));
    const char *money_frozen = json_string_value(json_object_get(balance, "money_frozen"));

    if (user_id == 0 || stock == NULL || money == NULL || stock_available == NULL || stock_frozen == NULL || money_available == NULL || money_frozen == NULL)
        return -__LINE__;

    order_on_update(user_id, event, order);
    asset_on_update(user_id, account, stock, stock_available, stock_frozen, timestamp);
    asset_on_update(user_id, account, money, money_available, money_frozen, timestamp);

    json_t *fee = json_object_get(balance, "fee");
    if (fee != NULL) {
        uint32_t fee_account = json_integer_value(json_object_get(fee, "account"));
        const char *fee_asset = json_string_value(json_object_get(fee, "asset"));
        const char *fee_available = json_string_value(json_object_get(fee, "available"));
        const char *fee_frozen = json_string_value(json_object_get(fee, "frozen"));
        if (fee_asset == NULL || fee_available == NULL || fee_frozen == NULL)
            return -__LINE__;

        asset_on_update(user_id, fee_account, fee_asset, fee_available, fee_frozen, timestamp);
    }

    return 0;
}

static void on_orders_message(sds message, int64_t offset)
{
    kafka_orders_offset = offset;
    log_trace("order message: %s", message);
    profile_inc("message_order", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid order message: %s", message);
        return;
    }

    int ret = process_orders_message(msg);
    if (ret < 0) {
        log_error("process_orders_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

static int process_indexs_message(json_t *msg)
{
    const char *market = json_string_value(json_object_get(msg, "market"));
    const char *price  = json_string_value(json_object_get(msg, "price"));

    return index_on_update(market, price);
}

static void on_indexs_message(sds message, int64_t offset)
{
    kafka_indexs_offset = offset;
    log_trace("index message: %s", message);
    profile_inc("message_index", 1);

    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid index message: %s", message);
        return;
    }

    int ret = process_indexs_message(msg);
    if (ret < 0) {
        log_error("process_indexs_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

static int process_balances_message(json_t *msg)
{
    uint32_t user_id = json_integer_value(json_object_get(msg, "user_id"));
    uint32_t account = json_integer_value(json_object_get(msg, "account"));
    const char *asset = json_string_value(json_object_get(msg, "asset"));
    const char *available = json_string_value(json_object_get(msg, "available"));
    const char *frozen = json_string_value(json_object_get(msg, "frozen"));
    double timestamp = json_real_value(json_object_get(msg, "timestamp"));

    if (user_id == 0 || asset == NULL || available == NULL || frozen == NULL) {
        return -__LINE__;
    }

    asset_on_update(user_id, account, asset, available, frozen, timestamp);
    return 0;
}

static void on_balances_message(sds message, int64_t offset)
{
    kafka_balances_offset = offset;
    log_trace("balance message: %s", message);
    profile_inc("message_balance", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid balance message: %s", message);
        return;
    }

    int ret = process_balances_message(msg);
    if (ret < 0) {
        log_error("process_balances_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

static int process_notice_message(json_t *msg)
{
    notice_message(msg);
    return 0;
}

static void on_notice_message(sds message, int64_t offset)
{
    kafka_notice_offset = offset;
    log_trace("notice message: %s", message);
    profile_inc("message_notice", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid notice message: %s", message);
        return;
    }

    int ret = process_notice_message(msg);
    if (ret < 0) {
        log_error("process_notice_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
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
    report_kafka_offset(kafka_stops, kafka_stops_offset);
    report_kafka_offset(kafka_orders, kafka_orders_offset);
    report_kafka_offset(kafka_indexs, kafka_indexs_offset);
    report_kafka_offset(kafka_balances, kafka_balances_offset);
    report_kafka_offset(kafka_notice, kafka_notice_offset);
}

int init_message(void)
{
    kafka_deals = kafka_consumer_create(settings.brokers, TOPIC_DEAL, 0, RD_KAFKA_OFFSET_END, on_deals_message);
    if (kafka_deals == NULL) {
        return -__LINE__;
    }

    kafka_stops = kafka_consumer_create(settings.brokers, TOPIC_STOP, 0, RD_KAFKA_OFFSET_END, on_stops_message);
    if (kafka_stops == NULL) {
        return -__LINE__;
    }

    kafka_orders = kafka_consumer_create(settings.brokers, TOPIC_ORDER, 0, RD_KAFKA_OFFSET_END, on_orders_message);
    if (kafka_orders == NULL) {
        return -__LINE__;
    }

    kafka_indexs = kafka_consumer_create(settings.brokers, TOPIC_INDEX, 0, RD_KAFKA_OFFSET_END, on_indexs_message);
    if (kafka_indexs == NULL) {
        return -__LINE__;
    }

    kafka_balances = kafka_consumer_create(settings.brokers, TOPIC_BALANCE, 0, RD_KAFKA_OFFSET_END, on_balances_message);
    if (kafka_balances == NULL) {
        return -__LINE__;
    }

    kafka_notice = kafka_consumer_create(settings.brokers, TOPIC_NOTICE, 0, RD_KAFKA_OFFSET_END, on_notice_message);
    if (kafka_notice == NULL) {
        return -__LINE__;
    }

    nw_timer_set(&report_timer, 10, true, on_report_timer, NULL);
    nw_timer_start(&report_timer);

    return 0;
}

