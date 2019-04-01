/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "dr_config.h"
# include "dr_message.h"
# include "dr_history.h"
# include "dr_deal.h"
# include "dr_fee_rate.h"
# include "dr_operlog.h"

static kafka_consumer_t *kafka_deals;
#define OFFSET_FILE  "./offset.json"

static void write_offset_file(int64_t offset)
{
    json_t* root = json_object();
    json_object_set_new(root,"offset",json_integer(offset));

    int ret = json_dump_file(root, OFFSET_FILE, JSON_PRESERVE_ORDER);
    if (ret != 0) {
        log_error("json_dump_file fail, ret: %d", ret);
    }
    json_decref(root);

    return;
}

static int read_offset_file(void)
{
    json_error_t error;
    json_t *root = json_load_file(OFFSET_FILE, JSON_DISABLE_EOF_CHECK | JSON_DECODE_ANY, &error);

    if(!json_is_object(root)){
        return -__LINE__;
    }

    uint64_t offset;
    json_t *offset_obj = json_object_get(root, "offset");
    if (offset) {
        offset = json_integer_value(offset_obj);
    } else {
        json_decref(root);
        return -__LINE__;
    }

    json_decref(root);
    return offset;
}

void store_message(uint32_t ask_user_id, uint32_t bid_user_id, uint32_t taker_user_id, double timestamp, const char *market, const char *stock, 
        const char *amount, const char *price, const char *ask_fee_asset, const char *bid_fee_asset, const char *ask_fee,
        const char *bid_fee, const char *ask_fee_rate, const char *bid_fee_rate)
{
    bool ask_is_taker = false;
    bool bid_is_taker = false;

    if (ask_user_id == taker_user_id) {
        ask_is_taker = true;
    } else {
        bid_is_taker = true;
    }

    int ret;
    ret = deals_process(ask_is_taker, ask_user_id, timestamp, MARKET_TRADE_SIDE_SELL, market, stock, amount, price, ask_fee_asset, ask_fee);
    if (ret != 0) {
        log_error("deals_process fail, ret: %d", ret);
    }

    ret = deals_process(bid_is_taker, bid_user_id, timestamp, MARKET_TRADE_SIDE_BUY, market, stock, amount, price, bid_fee_asset, bid_fee);
    if (ret != 0) {
        log_error("deals_process fail, ret: %d", ret);
    }

    ret = fee_rate_process(market, stock, ask_fee_rate);
    if (ret != 0) {
        log_error("fee_rate_process fail, ret: %d", ret);
    }

    ret = fee_rate_process(market, stock, bid_fee_rate);
    if (ret != 0) {
        log_error("fee_rate_process fail, ret: %d", ret);
    } 
}

static void append_deal_msg_process(uint32_t ask_user_id, uint32_t bid_user_id, uint32_t taker_user_id, double timestamp, const char *market, const char *stock, 
        const char *amount, const char *price, const char *ask_fee_asset, const char *bid_fee_asset, const char *ask_fee, const char *bid_fee, const char *ask_fee_rate, const char *bid_fee_rate)
{
    bool need_del_operlog = false;
    static time_t d_last = 0;
    if (d_last == 0)
        d_last = get_day_start(timestamp);

    time_t d_current = get_day_start(timestamp);
    if (d_last != d_current) {
        d_last = d_current;
        need_del_operlog = true;
    }

    append_deal_msg(need_del_operlog, ask_user_id, bid_user_id, taker_user_id, timestamp, market, stock, amount, price, 
            ask_fee_asset, bid_fee_asset, ask_fee, bid_fee, ask_fee_rate, bid_fee_rate);
    return;
}

 void process_deals_message(json_t *msg) //oux
{
    uint32_t ask_user_id = json_integer_value(json_object_get(msg, "ask_user_id"));
    uint32_t bid_user_id = json_integer_value(json_object_get(msg, "bid_user_id"));
    uint32_t taker_user_id = json_integer_value(json_object_get(msg, "taker_user_id"));

    double timestamp = json_real_value(json_object_get(msg, "timestamp"));
    const char *market = json_string_value(json_object_get(msg, "market"));
    const char *stock = json_string_value(json_object_get(msg, "stock"));
    const char *amount_str = json_string_value(json_object_get(msg, "amount"));
    const char *price_str  = json_string_value(json_object_get(msg, "price"));

    const char *ask_fee_asset_str  = json_string_value(json_object_get(msg, "ask_fee_asset"));
    const char *bid_fee_asset_str  = json_string_value(json_object_get(msg, "bid_fee_asset"));
    const char *ask_fee_str  = json_string_value(json_object_get(msg, "ask_fee"));
    const char *bid_fee_str  = json_string_value(json_object_get(msg, "bid_fee"));

    const char *ask_fee_rate_str  = json_string_value(json_object_get(msg, "ask_fee_rate"));
    const char *bid_fee_rate_str  = json_string_value(json_object_get(msg, "bid_fee_rate"));

    append_deal_msg_process(ask_user_id, bid_user_id, taker_user_id, timestamp, market, stock, amount_str, price_str, ask_fee_asset_str, 
        bid_fee_asset_str, ask_fee_str, bid_fee_str, ask_fee_rate_str, bid_fee_rate_str);

    store_message(ask_user_id, bid_user_id, taker_user_id, timestamp, market, stock, amount_str, price_str, ask_fee_asset_str, 
        bid_fee_asset_str, ask_fee_str, bid_fee_str, ask_fee_rate_str, bid_fee_rate_str);

    return;
}

static void on_deals_message(sds message, int64_t offset)
{
    static uint32_t num = 0;
    if (num % 500 == 0) {
        log_info("part deal message: %s", message);
    }
    num++;

    json_t *msg = json_loads(message, 0, NULL);
    if (!msg) {
        log_error("json_loads fail, message: %s", message);
        json_decref(msg);
        return;
    }

    process_deals_message(msg);

    write_offset_file(offset);
    json_decref(msg);
}

int init_message(void)
{
    int64_t offset = read_offset_file();
    log_info("kafka offset: %zd", offset);

    if (offset > 0) {
        settings.deals.offset = offset;
    } else {
        settings.deals.offset = RD_KAFKA_OFFSET_END;
    }

    kafka_deals = kafka_consumer_create(&settings.deals, on_deals_message);
    if (kafka_deals == NULL) {
        return -__LINE__;
    }

    return 0;
}


