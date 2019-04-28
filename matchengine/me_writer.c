/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */

# include "me_config.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_operlog.h"
# include "me_history.h"
# include "me_message.h"
# include "me_asset_backup.h"
# include "me_persist.h"
# include "ut_queue.h"
# include "me_writer.h"
# include "me_reply.h"

static rpc_svr *svr;
static cli_svr *svrcli;
static queue_t *queue_writers;

static int push_operlog(const char *method, json_t *params)
{
    json_t *data = json_object();
    json_object_set_new(data, "method", json_string(method));
    json_object_set(data, "params", params);
    char *detail = json_dumps(data, JSON_SORT_KEYS);
    json_decref(data);

    log_trace("operlog: %s", detail);
    for (int i = 0; i < settings.reader_num; ++i) {
        queue_push(&queue_writers[i], detail, strlen(detail));
    }
    append_operlog(detail);
    free(detail);
    return 0;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static int on_cmd_asset_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return reply_error_invalid_argument(ses, pkg);

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("update_balance", params);
    return reply_success(ses, pkg);
}

static int on_cmd_asset_lock(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 5)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return reply_error_invalid_argument(ses, pkg);

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *amount = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (amount == NULL)
        return reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(amount);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_lock(true, user_id, asset, business, business_id, amount);
    mpd_del(amount);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("asset_lock", params);
    return reply_success(ses, pkg);
}

static int on_cmd_asset_unlock(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 5)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return reply_error_invalid_argument(ses, pkg);

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *amount = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (amount == NULL)
        return reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(amount);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_unlock(true, user_id, asset, business, business_id, amount);
    mpd_del(amount);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("asset_unlock", params);
    return reply_success(ses, pkg);
}

static int on_cmd_asset_backup(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_object();
    int ret = make_asset_backup(result);
    if (ret < 0) {
        json_decref(result);
        return reply_error_internal_error(ses, pkg);
    }

    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_put_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 8)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount    = NULL;
    mpd_t *price     = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *maker_fee = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price 
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // maker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    maker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (maker_fee == NULL || mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    const char *fee_asset = NULL;
    if (json_array_size(params) >= 10) {
        // fee asset
        if (json_is_string(json_array_get(params, 8))) {
            fee_asset = json_string_value(json_array_get(params, 8));
            if (!asset_exist(fee_asset))
                goto invalid_argument;
            if (!get_fee_price(market, fee_asset)) {
                log_error("get fee price fail: %s", fee_asset);
                goto invalid_argument;
            }
        } else if (!json_is_null(json_array_get(params, 8))) {
            goto invalid_argument;
        }

        // fee discount
        if (fee_asset && json_is_string(json_array_get(params, 9))) {
            fee_discount = decimal(json_string_value(json_array_get(params, 9)), 4);
            if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
                goto invalid_argument;
        }
    }

    json_t *result = NULL;
    int ret = market_put_limit_order(true, &result, market, user_id, side, amount, price, taker_fee, maker_fee, source, fee_asset, fee_discount);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("limit_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (maker_fee)
        mpd_del(maker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    return reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_order_put_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 6)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 4)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 5));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    const char *fee_asset = NULL;
    if (json_array_size(params) >= 8) {
        // fee asset
        if (json_is_string(json_array_get(params, 6))) {
            fee_asset = json_string_value(json_array_get(params, 6));
            if (!asset_exist(fee_asset))
                goto invalid_argument;
            if (!get_fee_price(market, fee_asset))
                goto invalid_argument;
        } else if (!json_is_null(json_array_get(params, 6))) {
            goto invalid_argument;
        }

        // fee discount
        if (fee_asset && json_is_string(json_array_get(params, 7))) {
            fee_discount = decimal(json_string_value(json_array_get(params, 7)), 4);
            if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
                goto invalid_argument;
        }
    }

    json_t *result = NULL;
    int ret = market_put_market_order(true, &result, market, user_id, side, amount, taker_fee, source, fee_asset, fee_discount);

    mpd_del(amount);
    mpd_del(taker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "amount too small");
    } else if (ret == -3) {
        return reply_error(ses, pkg, 12, "no enough trader");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("market_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    return reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_order_cancel(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }
    if (order->user_id != user_id) {
        return reply_error(ses, pkg, 11, "user not match");
    }

    json_t *result = NULL;
    int ret = market_cancel_order(true, &result, market, order);
    if (ret < 0) {
        log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("cancel_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_put_stop_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 11)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount       = NULL;
    mpd_t *stop_price   = NULL;
    mpd_t *price        = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *maker_fee    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // stop price 
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    stop_price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (stop_price == NULL || mpd_cmp(stop_price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price 
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // maker fee
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    maker_fee = decimal(json_string_value(json_array_get(params, 7)), market->fee_prec);
    if (maker_fee == NULL || mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 8)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 8));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // fee asset
    if (!json_is_string(json_array_get(params, 9)))
        goto invalid_argument;
    const char *fee_asset = json_string_value(json_array_get(params, 9));
    if (strlen(fee_asset) > 0 && !asset_exist(fee_asset))
        goto invalid_argument;

    // fee discount
    if (json_is_string(json_array_get(params, 10)) && strlen(json_string_value(json_array_get(params, 10))) > 0) {
        fee_discount = decimal(json_string_value(json_array_get(params, 10)), 4);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto invalid_argument;
    }

    int ret = market_put_stop_limit(true, market, user_id, side, amount, stop_price, price, taker_fee, maker_fee, source, fee_asset, fee_discount);

    mpd_del(amount);
    mpd_del(stop_price);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {           
        return reply_error(ses, pkg, 11, "invalid stop price");
    } else if (ret == -3) {
        return reply_error(ses, pkg, 12, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("stop_limit", params);
    ret = reply_success(ses, pkg);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (stop_price)
        mpd_del(stop_price);
    if (price)
        mpd_del(price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (maker_fee)
        mpd_del(maker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    return reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_put_stop_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 9)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount       = NULL;
    mpd_t *stop_price   = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // stop price 
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    stop_price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (stop_price == NULL || mpd_cmp(stop_price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 6));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // fee asset
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    const char *fee_asset = json_string_value(json_array_get(params, 7));
    if (strlen(fee_asset) > 0 && !asset_exist(fee_asset))
        goto invalid_argument;

    // fee discount
    if (json_is_string(json_array_get(params, 8)) && strlen(json_string_value(json_array_get(params, 8))) > 0) {
        fee_discount = decimal(json_string_value(json_array_get(params, 8)), 4);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto invalid_argument;
    }

    int ret = market_put_stop_market(true, market, user_id, side, amount, stop_price, taker_fee, source, fee_asset, fee_discount);

    mpd_del(amount);
    mpd_del(stop_price);
    mpd_del(taker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "invalid stop price");
    } else if (ret == -3) {
        return reply_error(ses, pkg, 12, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("stop_market", params);
    ret = reply_success(ses, pkg);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (stop_price)
        mpd_del(stop_price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (fee_discount)
        mpd_del(fee_discount);

    return reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_cancel_stop(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    stop_t *stop = market_get_stop(market, order_id);
    if (stop == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }
    if (stop->user_id != user_id) {
        return reply_error(ses, pkg, 11, "user not match");
    }

    json_t *result = NULL;
    int ret = market_cancel_stop(true, &result, market, stop);
    if (ret < 0) {
        log_fatal("cancel stop order: %"PRIu64" fail: %d", stop->id, ret);
        return reply_error_internal_error(ses, pkg);
    }

    push_operlog("cancel_stop", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_update_asset_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret;
    ret = update_asset_config();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    ret = update_balance();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    log_info("update asset config success!");
    return reply_success(ses, pkg);
}

static int on_cmd_update_market_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret;
    ret = update_market_config();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    ret = update_trade();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    log_info("update market config success!");
    return reply_success(ses, pkg);
}

static int self_market_deal(market_t *market, mpd_t *amount, mpd_t *price, uint32_t side)
{
    // get ask_price_1
    mpd_t *ask_price_1 = NULL;
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    if (iter != NULL) {
        skiplist_node *node = skiplist_next(iter);
        if (node != NULL) {
            order_t *order = node->value;
            ask_price_1 = mpd_new(&mpd_ctx);
            mpd_copy(ask_price_1, order->price, &mpd_ctx);
        }
        skiplist_release_iterator(iter);
    } else {
        return -__LINE__;
    }

    // get bid_price_1
    mpd_t *bid_price_1 = NULL;
    iter = skiplist_get_iterator(market->bids);
    if (iter != NULL) {
        skiplist_node *node = skiplist_next(iter);
        if (node != NULL) {
            order_t *order = node->value;
            bid_price_1 = mpd_new(&mpd_ctx);
            mpd_copy(bid_price_1, order->price, &mpd_ctx);
        }
        skiplist_release_iterator(iter);
    } else {
        if (ask_price_1 != NULL)
            mpd_del(ask_price_1);
        return -__LINE__;
    }

    mpd_t *deal_min_gear = mpd_new(&mpd_ctx);
    mpd_set_i32(deal_min_gear, -market->money_prec, &mpd_ctx);
    mpd_pow(deal_min_gear, mpd_ten, deal_min_gear, &mpd_ctx);

    if (ask_price_1 != NULL && bid_price_1 != NULL) {
        mpd_t *ask_bid_sub = mpd_new(&mpd_ctx);
        mpd_sub(ask_bid_sub, ask_price_1, bid_price_1, &mpd_ctx);
        if (mpd_cmp(deal_min_gear, ask_bid_sub, &mpd_ctx) == 0) {
            mpd_del(ask_bid_sub);
            mpd_del(deal_min_gear);

            if (ask_price_1 != NULL)
                mpd_del(ask_price_1);
            if (bid_price_1 != NULL)
                mpd_del(bid_price_1);
            return -1;
        }
        mpd_del(ask_bid_sub);
    }

    mpd_t *real_price = mpd_qncopy(price);
    if (ask_price_1 != NULL && mpd_cmp(price, ask_price_1, &mpd_ctx) >= 0) {
        mpd_sub(real_price, ask_price_1, deal_min_gear, &mpd_ctx);
    } else if (bid_price_1 != NULL && mpd_cmp(price, bid_price_1, &mpd_ctx) <= 0){
        mpd_add(real_price, bid_price_1, deal_min_gear, &mpd_ctx);
    }

    mpd_t *deal = mpd_new(&mpd_ctx);
    mpd_mul(deal, real_price, amount, &mpd_ctx);

    uint64_t deal_id = ++deals_id_start;
    double update_time = current_timestamp();

    order_t *order = malloc(sizeof(order_t));
    order->id        = 0;
    order->user_id   = 0;

    push_deal_message(update_time, deal_id, market, side, order, order, real_price, amount, deal, market->money, mpd_zero, market->stock, mpd_zero);

    free(order);
    mpd_del(deal);
    mpd_del(real_price);
    mpd_del(deal_min_gear);
    if (bid_price_1 != NULL)
        mpd_del(bid_price_1);
    if (ask_price_1 != NULL)
        mpd_del(ask_price_1);

    return 0;
}

static int on_cmd_self_market_deal(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount  = NULL;
    mpd_t *price   = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 1)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 1)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price 
    if (!json_is_string(json_array_get(params, 2)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 2)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_TRADE_SIDE_SELL && side != MARKET_TRADE_SIDE_BUY)
        return reply_error_invalid_argument(ses, pkg);

    int ret = self_market_deal(market, amount, price, side);

    mpd_del(amount);
    mpd_del(price);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "no reasonable price");
    } else if (ret < 0) {
        log_fatal("self_market_deal fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    ret = reply_success(ses, pkg);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);

    return reply_error_invalid_argument(ses, pkg);
}

static bool is_queue_block()
{
    for (int i = 0; i < settings.reader_num; i++) {
        uint32_t left = queue_left(&queue_writers[i]);
        if (left <= QUEUE_MEM_MIN) {
            log_error("queue: %d block, mem left: %u", i, left);
            return true;
        }
    }
    return false;
}

static bool is_service_availablce(void)
{
    bool queue_block = is_queue_block();
    bool operlog_block = is_operlog_block();
    bool history_block = is_history_block();
    bool message_block = is_message_block();
    if (queue_block || operlog_block || history_block || message_block) {
        log_fatal("service unavailable, queue: %d, operlog: %d, history: %d, message: %d",
                queue_block, operlog_block, history_block, message_block);
        return false;
    }
    return true;
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_ASSET_UPDATE:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_update", 1);
        ret = on_cmd_asset_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_update %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_LOCK:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_lock", 1);
        ret = on_cmd_asset_lock(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_lock %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_UNLOCK:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_unlock", 1);
        ret = on_cmd_asset_unlock(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_unlock %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_BACKUP:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_backup", 1);
        ret = on_cmd_asset_backup(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_backup %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_LIMIT:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_limit", 1);
        ret = on_cmd_order_put_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_MARKET:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_market", 1);
        ret = on_cmd_order_put_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_market %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_cancel", 1);
        ret = on_cmd_order_cancel(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_STOP_LIMIT:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_stop_limit", 1);
        ret = on_cmd_put_stop_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_put_stop_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_STOP_MARKET:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_stop_market", 1);
        ret = on_cmd_put_stop_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_put_stop_market %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL_STOP:
        if (!is_service_availablce()) {
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_cancel_stop", 1);
        ret = on_cmd_cancel_stop(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_cancel_stop%s fail: %d", params_str, ret);
        }
        break;
    case CMD_CONFIG_UPDATE_ASSET:
        profile_inc("cmd_config_update_asset", 1);
        ret = on_cmd_update_asset_config(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_update_asset_config fail: %d", ret);
        }
        break;
    case CMD_CONFIG_UPDATE_MARKET:
        profile_inc("cmd_config_update_market", 1);
        ret = on_cmd_update_market_config(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_update_market_config fail: %d", ret);
        }
        break;
    case CMD_MARKET_SELF_DEAL:
        profile_inc("cmd_market_self_deal", 1);
        ret = on_cmd_self_market_deal(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_self_market_deal fail: %d", ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

cleanup:
    sdsfree(params_str);
    json_decref(params);
    return;

decode_error:
    if (params) {
        json_decref(params);
    }
    sds hex = hexdump(pkg->body, pkg->body_size);
    log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", \
            nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
    sdsfree(hex);
    rpc_svr_close_clt(svr, ses);

    return;
}

static int init_server()
{
    if (settings.svr.bind_count != 1)
        return -__LINE__;
    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET)
        return -__LINE__;
    bind_arr->addr.in.sin_port = htons(ntohs(bind_arr->addr.in.sin_port) + 1);

    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    return 0;
}

static sds queue_status(sds reply)
{
    uint32_t mem_num = 0;
    uint32_t mem_size = 0;
    for (int i = 0; i < settings.reader_num; i++) {
        queue_stat(&queue_writers[i], &mem_num, &mem_size);
        reply = sdscatprintf(reply, "queue: %d, num: %u, size: %u\n", i, mem_num, mem_size);
    }
    return reply;
}

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = market_status(reply);
    reply = operlog_status(reply);
    reply = history_status(reply);
    reply = message_status(reply);
    reply = queue_status(reply);
    return reply;
}

static sds on_cmd_makeslice(const char *cmd, int argc, sds *argv)
{
    time_t now = time(NULL);
    make_slice(now);
    return sdsnew("OK\n");
}

static sds on_cmd_unfreeze(const char *cmd, int argc, sds *argv)
{
    if (argc != 3) {
        sds reply = sdsempty();
        return sdscatprintf(reply, "usage: %s user_id asset amount\n", cmd);
    }

    uint32_t user_id = strtoul(argv[0], NULL, 0);
    if (user_id <= 0) {
        return sdsnew("failed, user_id error\n");
    }

    char *asset = strdup(argv[1]);
    if (!asset) {
        return sdsnew("failed, asset error\n");
    }
    if (!asset_exist(asset)) {
        free(asset);
        return sdsnew("failed, asset not exist\n");
    }

    mpd_t *amount = decimal(argv[2], asset_prec(asset));
    if (!amount) {
        free(asset);
        return sdsnew("failed, amount error\n");
    }

    mpd_t *frozen = balance_unfreeze(user_id, BALANCE_TYPE_FROZEN, asset, amount);
    if (!frozen) {
        free(asset);
        mpd_del(amount);
        sds reply = sdsempty();
        return sdscatprintf(reply, "unfreeze failed, user_id: %d\n", user_id);
    }

    free(asset);
    mpd_del(amount);
    sds reply = sdsempty();
    return sdscatprintf(reply, "unfreeze success, user_id: %d\n", user_id);
}

static sds on_cmd_history_control(const char *cmd, int argc, sds *argv)
{
    if (argc != 1) {
        sds reply = sdsempty();
        return sdscatprintf(reply, "usage: %s history_mode[1, 2, 3]\n", cmd);
    }

    uint32_t mode = strtoul(argv[0], NULL, 0);
    if (mode != HISTORY_MODE_DIRECT && mode != HISTORY_MODE_KAFKA && mode != HISTORY_MODE_DOUBLE) {
        return sdsnew("failed, mode should be 1, 2 or 3\n");
    }

    settings.history_mode = mode;
    sds reply = sdsempty();
    return sdscatprintf(reply, "change history mode success: %d\n", mode);
}

static int init_cli()
{
    if (settings.cli.addr.family == AF_INET) {
        settings.cli.addr.in.sin_port = htons(ntohs(settings.cli.addr.in.sin_port) + 1);
    } else if (settings.cli.addr.family == AF_INET6) {
        settings.cli.addr.in6.sin6_port = htons(ntohs(settings.cli.addr.in6.sin6_port) + 1);
    }

    svrcli = cli_svr_create(&settings.cli);
    if (svrcli == NULL) {
        return -__LINE__;
    }

    
    cli_svr_add_cmd(svrcli, "status", on_cmd_status);
    cli_svr_add_cmd(svrcli, "makeslice", on_cmd_makeslice);
    cli_svr_add_cmd(svrcli, "unfreeze", on_cmd_unfreeze);
    cli_svr_add_cmd(svrcli, "hiscontrol", on_cmd_history_control);

    return 0;
}

static int init_queue()
{
    queue_writers = (queue_t *)malloc(sizeof(queue_t) * settings.reader_num);
    memset(queue_writers, 0, sizeof(queue_t) * settings.reader_num);

    for (int i = 0; i < settings.reader_num; ++i) {
        sds queue_name = sdsempty();
        queue_name = sdscatprintf(queue_name, "%s_%d", QUEUE_NAME, i);

        sds queue_pipe_path = sdsempty();
        queue_pipe_path = sdscatprintf(queue_pipe_path, "%s_%d", QUEUE_PIPE_PATH, i);

        key_t queue_shm_key = QUEUE_SHMKEY_START + i;

        int ret = queue_writer_init(&queue_writers[i], NULL, queue_name, queue_pipe_path, queue_shm_key, QUEUE_MEM_SIZE);

        sdsfree(queue_name);
        sdsfree(queue_pipe_path);

        if (ret < 0) {
            log_error("init_queue %d failed, ret: %d", i, ret);
            return ret;
        }
    }
    return 0;
}

int init_writer()
{
    int ret;
    ret = init_queue();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_cli();
    if(ret < 0) {
        return -__LINE__;
    }

    ret = init_server();
    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}
