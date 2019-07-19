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
# include "aw_asset_sub.h"

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_stops;
static kafka_consumer_t *kafka_orders;
static kafka_consumer_t *kafka_indexs;
static kafka_consumer_t *kafka_balances;
static kafka_consumer_t *kafka_users;

static int process_users_message(json_t *msg)
{
    uint32_t user_id = json_integer_value(json_object_get(msg, "user_id"));
    nw_ses *ses = get_auth_user_ses(user_id);
    if (ses) {
        ws_send_notify(ses, "user.message", msg);
    }

    return 0;
}

static void on_users_message(sds message, int64_t offset)
{
    log_trace("users message: %s", message);
    profile_inc("message_user ", 1);
    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("invalid user message: %s", message);
        return;
    }

    int ret = process_users_message(msg);
    if (ret < 0) {
        log_error("process_users_message: %s fail: %d", message, ret);
    }

    json_decref(msg);
}

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
    profile_inc("message_deal ", 1);
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
    if (order == NULL)
        return -__LINE__;

    uint32_t user_id = json_integer_value(json_object_get(order, "user"));
    uint32_t account = json_integer_value(json_object_get(order, "account"));
    const char *stock = json_string_value(json_object_get(msg, "stock"));
    const char *money = json_string_value(json_object_get(msg, "money"));
    const char *fee_asset = json_string_value(json_object_get(order, "fee_asset"));
    if (user_id == 0 || stock == NULL || money == NULL)
        return -__LINE__;

    order_on_update(user_id, event, order);

    asset_on_update(user_id, account, stock);
    asset_on_update(user_id, account, money);
    if (account == 0) {
        asset_on_update_sub(user_id, stock);
        asset_on_update_sub(user_id, money);
    }

    if (fee_asset && strcmp(fee_asset, stock) != 0 && strcmp(fee_asset, money) != 0) {
        asset_on_update(user_id, account, fee_asset);
        if (account == 0) {
            asset_on_update_sub(user_id, fee_asset);
        }
    }

    return 0;
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
    if (user_id == 0 || asset == NULL) {
        return -__LINE__;
    }

    asset_on_update(user_id, account, asset);
    if (account == 0) {
        asset_on_update_sub(user_id, asset);
    }

    return 0;
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

    settings.stops.offset = RD_KAFKA_OFFSET_END;
    kafka_stops = kafka_consumer_create(&settings.stops, on_stops_message);
    if (kafka_stops == NULL) {
        return -__LINE__;
    }

    settings.orders.offset = RD_KAFKA_OFFSET_END;
    kafka_orders = kafka_consumer_create(&settings.orders, on_orders_message);
    if (kafka_orders == NULL) {
        return -__LINE__;
    }

    settings.indexs.offset = RD_KAFKA_OFFSET_END;
    kafka_indexs = kafka_consumer_create(&settings.indexs, on_indexs_message);
    if (kafka_indexs == NULL) {
        return -__LINE__;
    }

    settings.balances.offset = RD_KAFKA_OFFSET_END;
    kafka_balances = kafka_consumer_create(&settings.balances, on_balances_message);
    if (kafka_balances == NULL) {
        return -__LINE__;
    }

    settings.users.offset = RD_KAFKA_OFFSET_END;
    kafka_users = kafka_consumer_create(&settings.users, on_users_message);
    if (kafka_users == NULL) {
        return -__LINE__;
    }

    return 0;
}

