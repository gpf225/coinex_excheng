/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/28, create
 */

# include "aw_config.h"
# include "aw_message.h"
# include "aw_deals.h"
# include "aw_asset.h"
# include "aw_order.h"

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_orders;
static kafka_consumer_t *kafka_balances;

static int process_deals_message(json_t *msg)
{
    uint32_t ask_user_id = json_integer_value(json_object_get(msg, "ask_user_id"));
    uint32_t bid_user_id = json_integer_value(json_object_get(msg, "bid_user_id"));
    uint64_t id = json_integer_value(json_object_get(msg, "id"));
    double timestamp = json_real_value(json_object_get(msg, "timestamp"));
    const char *market = json_string_value(json_object_get(msg, "market"));
    const char *amount = json_string_value(json_object_get(msg, "amount"));
    const char *price  = json_string_value(json_object_get(msg, "price"));

    deals_new(ask_user_id, id, timestamp, MARKET_TRADE_SIDE_SELL, market, amount, price);
    deals_new(bid_user_id, id, timestamp, MARKET_TRADE_SIDE_BUY,  market, amount, price);

    return 0;
}

static void on_deals_message(sds message, int64_t offset)
{
    log_trace("deal message: %s", message);
    monitor_inc("message_deal ", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid balance message: %s", message);
        return;
    }

    int ret = process_deals_message(msg);
    if (ret < 0) {
        log_error("process_deals_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

static int process_orders_message(json_t *msg)
{
    int event = json_integer_value(json_object_get(msg, "event"));
    if (event == 0)
        return -__LINE__;

    json_t *order = json_object_get(msg, "order");
    if (order == NULL)
        return -__LINE__;

    uint32_t user_id = json_integer_value(json_object_get(order, "user"));
    const char *stock = json_string_value(json_object_get(msg, "stock"));
    const char *money = json_string_value(json_object_get(msg, "money"));
    const char *fee_asset = json_string_value(json_object_get(msg, "fee_asset"));
    if (user_id == 0 || stock == NULL || money == NULL)
        return -__LINE__;

    order_on_update(user_id, event, order);
    asset_on_update(user_id, stock);
    asset_on_update(user_id, money);
    if (fee_asset && strcmp(fee_asset, stock) != 0 && strcmp(fee_asset, money) != 0) {
        asset_on_update(user_id, fee_asset);
    }

    return 0;
}

static void on_orders_message(sds message, int64_t offset)
{
    log_trace("order message: %s", message);
    monitor_inc("message_order", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid balance message: %s", message);
        return;
    }

    int ret = process_orders_message(msg);
    if (ret < 0) {
        log_error("process_orders_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

static int process_balances_message(json_t *msg)
{
    uint32_t user_id = json_integer_value(json_object_get(msg, "user_id"));
    const char *asset = json_string_value(json_object_get(msg, "asset"));
    if (user_id == 0 || asset == NULL) {
        return -__LINE__;
    }

    asset_on_update(user_id, asset);

    return 0;
}

static void on_balances_message(sds message, int64_t offset)
{
    log_trace("balance message: %s", message);
    monitor_inc("message_balance", 1);
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

int init_message(void)
{
    settings.deals.offset = RD_KAFKA_OFFSET_END;
    kafka_deals = kafka_consumer_create(&settings.deals, on_deals_message);
    if (kafka_deals == NULL) {
        return -__LINE__;
    }

    settings.orders.offset = RD_KAFKA_OFFSET_END;
    kafka_orders = kafka_consumer_create(&settings.orders, on_orders_message);
    if (kafka_orders == NULL) {
        return -__LINE__;
    }

    settings.balances.offset = RD_KAFKA_OFFSET_END;
    kafka_balances = kafka_consumer_create(&settings.balances, on_balances_message);
    if (kafka_balances == NULL) {
        return -__LINE__;
    }

    return 0;
}

