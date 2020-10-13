/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */

# include "me_config.h"
# include "me_asset.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_operlog.h"
# include "me_history.h"
# include "me_message.h"
# include "me_asset.h"
# include "me_persist.h"
# include "ut_queue.h"
# include "me_writer.h"
# include "me_request.h"

static rpc_svr *svr;
static cli_svr *svrcli;
static queue_t *queue_writers;
static mpd_t *mpd_maximum;

static int push_operlog(const char *method, json_t *params)
{
    json_t *data = json_object();
    json_object_set_new(data, "method", json_string(method));
    json_object_set(data, "params", params);
    char *detail = json_dumps(data, JSON_SORT_KEYS);
    json_decref(data);

    log_trace("operlog: %s", detail);
    for (int i = 0; i < settings.reader_num; ++i) {
        int ret = queue_push(&queue_writers[i], detail, strlen(detail));
        if (ret < 0) {
            log_fatal("queue push fail, queue: %d, detail: %s", i, detail);
        }
    }
    append_operlog(detail);
    free(detail);
    return 0;
}

static bool is_client_id_valid(const char *client_id)
{
    if (!client_id) {
        return false;
    }

    size_t len = strlen(client_id);
    bool is_valid = true;
    for (int i = 0; i < len; ++i) {
        if (client_id[i] == '-' || client_id[i] == '_' || isalnum(client_id[i])) {
            continue;
        }
        is_valid = false;
        break;
    }
    return is_valid;
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
    if (json_array_size(params) != 7)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // asset
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 2));
    int prec = asset_prec_show(account, asset);
    if (prec < 0)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // business
    if (!json_is_string(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 3));

    // business_id
    if (!json_is_integer(json_array_get(params, 4)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 4));

    // change
    if (!json_is_string(json_array_get(params, 5)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 5)), prec);
    if (change == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 6);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return rpc_reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_balance(true, user_id, account, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return rpc_reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return rpc_reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("update_balance", params);
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_asset_lock(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // asset
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 2));
    int prec = asset_prec_show(account, asset);
    if (prec < 0)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // business
    if (!json_is_string(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 3));

    // business_id
    if (!json_is_integer(json_array_get(params, 4)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 4));

    // amount
    if (!json_is_string(json_array_get(params, 5)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    mpd_t *amount = decimal(json_string_value(json_array_get(params, 5)), prec);
    if (amount == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(amount);
        return rpc_reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_lock(true, user_id, account, asset, business, business_id, amount);
    mpd_del(amount);
    if (ret == -1) {
        return rpc_reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return rpc_reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("asset_lock", params);
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_asset_unlock(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // asset
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 2));
    int prec = asset_prec_show(account, asset);
    if (prec < 0)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // business
    if (!json_is_string(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 3));

    // business_id
    if (!json_is_integer(json_array_get(params, 4)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 4));

    // amount
    if (!json_is_string(json_array_get(params, 5)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    mpd_t *amount = decimal(json_string_value(json_array_get(params, 5)), prec);
    if (amount == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(amount);
        return rpc_reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_unlock(true, user_id, account, asset, business, business_id, amount);
    mpd_del(amount);
    if (ret == -1) {
        return rpc_reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return rpc_reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("asset_unlock", params);
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_asset_backup(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_object();
    int ret = make_asset_backup(result);
    if (ret < 0) {
        json_decref(result);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));
    if (!account_exist(account))
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = balance_query_list(user_id, account, params);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query_all(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    json_t *result = balance_query_all(user_id);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query_users(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 0));
    if (account == 0 || !account_exist(account))
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_array(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    json_t *users = json_array_get(params, 1);

    if (json_array_size(users) > MAX_QUERY_ASSET_USER_NUM)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = balance_query_users(account, users);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_asset_query_lock(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 2)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));
    if (!account_exist(account))
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = balance_query_lock_list(user_id, account, params);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_put_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 12)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL || !check_market_account(account, market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    mpd_t *amount    = NULL;
    mpd_t *price     = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *maker_fee = NULL;
    mpd_t *fee_price = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price 
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0 || mpd_cmp(price, mpd_maximum, &mpd_ctx) >= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (!check_fee_rate(taker_fee))
        goto invalid_argument;

    // maker fee
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    maker_fee = decimal(json_string_value(json_array_get(params, 7)), market->fee_prec);
    if (!check_fee_rate(maker_fee))
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 8)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 8));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 9))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 9));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto invalid_argument;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 10))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 10)), settings.discount_prec);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto invalid_argument;
    }

    // option
    uint32_t option = 0;
    if (json_is_integer(json_array_get(params, 11))) {
        option = json_integer_value(json_array_get(params, 11));
        if ((option & (~OPTION_CHECK_MASK)) != 0)
            goto invalid_argument;
    }

    // user self-define order id
    const char *client_id = NULL;
    if (json_array_size(params) >= 13) {
        if (json_is_string(json_array_get(params, 12))) {
            client_id = json_string_value(json_array_get(params, 12));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN || !is_client_id_valid(client_id))
                goto invalid_argument;
        }
    }

    json_t *result = NULL;
    int ret = market_put_limit_order(true, &result, market, user_id, account, side, amount, price, taker_fee, maker_fee, source, fee_asset, fee_price, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {
        return rpc_reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return rpc_reply_error(ses, pkg, 11, "amount too small");
    } else if (ret == -3) {
        return rpc_reply_error(ses, pkg, 12, "can't be completely executed, kill the order");
    } else if (ret == -4) {
        return rpc_reply_error(ses, pkg, 13, "the order may be executed, kill the order");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("limit_order", params);
    ret = rpc_reply_result(ses, pkg, result);
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
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return rpc_reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_order_put_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 10)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL || !check_market_account(account, market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    mpd_t *amount = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *fee_price = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (!check_fee_rate(taker_fee))
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 6));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 7))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 7));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto invalid_argument;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 8))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 8)), settings.discount_prec);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto invalid_argument;
    }

    // option
    uint32_t option = 0;
    if (json_is_integer(json_array_get(params, 9))) {
        option = json_integer_value(json_array_get(params, 9));
        if ((option & (~OPTION_CHECK_MASK)) != 0)
            goto invalid_argument;
    }

    // user self-define order id
    const char *client_id = NULL;
    if (json_array_size(params) >= 11) {
        if (json_is_string(json_array_get(params, 10))) {
            client_id = json_string_value(json_array_get(params, 10));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN || !is_client_id_valid(client_id))
                goto invalid_argument;
        }
    }

    json_t *result = NULL;
    int ret = market_put_market_order(true, &result, market, user_id, account, side, amount, taker_fee, source, fee_asset, fee_price, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(taker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {
        return rpc_reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return rpc_reply_error(ses, pkg, 11, "amount too small");
    } else if (ret == -3) {
        return rpc_reply_error(ses, pkg, 12, "no enough trader");
    } else if (ret == -4) {
        return rpc_reply_error(ses, pkg, 13, "can't be completely executed, kill the order");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("market_order", params);
    ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return rpc_reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_order_cancel(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return rpc_reply_error(ses, pkg, 10, "order not found");
    }
    if (order->user_id != user_id) {
        return rpc_reply_error(ses, pkg, 11, "user not match");
    }

    json_t *result = NULL;
    int ret = market_cancel_order(true, &result, market, order);
    if (ret < 0) {
        log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("cancel_order", params);
    ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static json_t *cancel_order_batch(market_t *market, uint32_t user_id, json_t *order_id_list)
{
    json_t *result = json_array();
    for (size_t i = 0; i < json_array_size(order_id_list); i++) {
        uint64_t order_id = json_integer_value(json_array_get(order_id_list, i));
        order_t *order = market_get_order(market, order_id);
        json_t *item = json_object();
        if (order == NULL) {
            json_object_set_new(item, "code", json_integer(10));
            json_object_set_new(item, "message", json_string("order not found"));
            json_object_set_new(item, "order", json_null());
        } else if (order->user_id != user_id) {
            json_object_set_new(item, "code", json_integer(11));
            json_object_set_new(item, "message", json_string("user not match"));
            json_object_set_new(item, "order", json_null());
        } else {
            json_t *order_result = NULL;
            int ret = market_cancel_order(true, &order_result, market, order);
            if (ret == 0) {
                json_object_set_new(item, "code", json_integer(0));
                json_object_set_new(item, "message", json_string(""));
                json_object_set_new(item, "order", order_result);

                json_t *operlog_params = json_array();
                json_array_append_new(operlog_params, json_integer(user_id));
                json_array_append_new(operlog_params, json_string(market->name));
                json_array_append_new(operlog_params, json_integer(order_id));
                push_operlog("cancel_order", operlog_params);
                json_decref(operlog_params);
            } else {
                log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
                json_object_set_new(item, "code", json_integer(1));
                json_object_set_new(item, "message", json_string("internal error"));
                json_object_set_new(item, "order", json_null());
                if (order_result) {
                    json_decref(order_result);
                }
            }
        }
        json_array_append_new(result, item);
    }
    return result;
}

static int on_cmd_order_cancel_batch(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_array(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *order_id_list = json_array_get(params, 2);
    size_t order_len = json_array_size(order_id_list);
    if (order_len == 0 || order_len > ORDER_LIST_MAX_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = cancel_order_batch(market, user_id, order_id_list);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_cancel_all(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    //account
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    int32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // side
    uint32_t side = 0;
    if (json_array_size(params) == 4) {
        if (!json_is_integer(json_array_get(params, 3)))
            return rpc_reply_error_invalid_argument(ses, pkg);
        side = json_integer_value(json_array_get(params, 3));
        if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    int ret = market_cancel_order_all(true, user_id, account, market, side);
    if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("cancel_order_all", params);
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_order_pending(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    int account = json_integer_value(json_array_get(params, 1));

    // market
    market_t *market = NULL;
    if (json_is_string(json_array_get(params, 2))) {
        const char *market_name = json_string_value(json_array_get(params, 2));
        market = get_market(market_name);
        if (market == NULL)
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 4)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 4));

    // limit
    if (!json_is_integer(json_array_get(params, 5)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 5));
    if (limit > ORDER_LIST_MAX_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *orders = json_array();
    skiplist_t *order_list = get_user_order_list(market, user_id, account);
    if (order_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        size_t count = 0;
        size_t total = 0;
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(order_list);
        while((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (side && order->side != side)
                continue;

            if (total >= offset && count < limit) {
                count += 1;
                json_array_append_new(orders, get_order_info(order, false));
            }
            total += 1;
        }
        skiplist_release_iterator(iter);
        json_object_set_new(result, "total", json_integer(total));
    }

    json_object_set_new(result, "records", orders);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static bool check_stop_option(uint32_t option)
{
    if ((option & (~OPTION_CHECK_MASK)) != 0 || (option & OPTION_MAKER_ONLY) || (option & OPTION_FILL_OR_KILL))
        return false;
    return true;
}

static int on_cmd_put_stop_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 13)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL || !check_market_account(account, market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    mpd_t *amount       = NULL;
    mpd_t *stop_price   = NULL;
    mpd_t *price        = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *maker_fee    = NULL;
    mpd_t *fee_price    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // stop price 
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    stop_price = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
    if (stop_price == NULL || mpd_cmp(stop_price, mpd_zero, &mpd_ctx) <= 0 || mpd_cmp(stop_price, mpd_maximum, &mpd_ctx) >= 0)
        goto invalid_argument;

    // price 
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 6)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0 || mpd_cmp(price, mpd_maximum, &mpd_ctx) >= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 7)), market->fee_prec);
    if (!check_fee_rate(taker_fee))
        goto invalid_argument;

    // maker fee
    if (!json_is_string(json_array_get(params, 8)))
        goto invalid_argument;
    maker_fee = decimal(json_string_value(json_array_get(params, 8)), market->fee_prec);
    if (!check_fee_rate(maker_fee))
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 9)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 9));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 10))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 10));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto invalid_argument;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 11))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 11)), settings.discount_prec);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto invalid_argument;
    }

    // option
    uint32_t option = 0;
    if (json_is_integer(json_array_get(params, 12))) {
        option = json_integer_value(json_array_get(params, 12));
        if (!check_stop_option(option))
            goto invalid_argument;
    }

    // user self-define order id
    const char *client_id = NULL;
    if (json_array_size(params) >= 14) {
        if (json_is_string(json_array_get(params, 13))) {
            client_id = json_string_value(json_array_get(params, 13));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN || !is_client_id_valid(client_id))
                goto invalid_argument;
        }
    }

    int ret = market_put_stop_limit(true, market, user_id, account, side, amount, stop_price, price, taker_fee, maker_fee, source, fee_asset, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(stop_price);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {           
        return rpc_reply_error(ses, pkg, 11, "invalid stop price");
    } else if (ret == -2) {
        return rpc_reply_error(ses, pkg, 12, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("stop_limit", params);
    ret = rpc_reply_success(ses, pkg);
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
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return rpc_reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_put_stop_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 11)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL || !check_market_account(account, market))
        return rpc_reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    mpd_t *amount       = NULL;
    mpd_t *stop_price   = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *fee_price    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // stop price 
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    stop_price = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
    if (stop_price == NULL || mpd_cmp(stop_price, mpd_zero, &mpd_ctx) <= 0 || mpd_cmp(stop_price, mpd_maximum, &mpd_ctx) >= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (!check_fee_rate(taker_fee))
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 8))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 8));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto invalid_argument;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 9))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 9)), settings.discount_prec);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto invalid_argument;
    }

    // option
    uint32_t option = 0;
    if (json_is_integer(json_array_get(params, 10))) {
        option = json_integer_value(json_array_get(params, 10));
        if (!check_stop_option(option))
            goto invalid_argument;
    }

    // user self-define order id
    const char *client_id = NULL;
    if (json_array_size(params) >= 12) {
        if (json_is_string(json_array_get(params, 11))) {
            client_id = json_string_value(json_array_get(params, 11));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN || !is_client_id_valid(client_id))
                goto invalid_argument;
        }
    }
    
    int ret = market_put_stop_market(true, market, user_id, account, side, amount, stop_price, taker_fee, source, fee_asset, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(stop_price);
    mpd_del(taker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    if (ret == -1) {
        return rpc_reply_error(ses, pkg, 11, "invalid stop price");
    } else if (ret == -2) {
        return rpc_reply_error(ses, pkg, 12, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("stop_market", params);
    ret = rpc_reply_success(ses, pkg);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (stop_price)
        mpd_del(stop_price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return rpc_reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_cancel_stop(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    stop_t *stop = market_get_stop(market, order_id);
    if (stop == NULL) {
        return rpc_reply_error(ses, pkg, 10, "order not found");
    }
    if (stop->user_id != user_id) {
        return rpc_reply_error(ses, pkg, 11, "user not match");
    }

    json_t *result = NULL;
    int ret = market_cancel_stop(true, &result, market, stop);
    if (ret < 0) {
        log_fatal("cancel stop order: %"PRIu64" fail: %d", stop->id, ret);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("cancel_stop", params);
    ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_cancel_stop_all(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) < 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    //account
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    int32_t account = json_integer_value(json_array_get(params, 1));

    //market
    if (!json_is_string(json_array_get(params, 2)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // side
    uint32_t side = 0;
    if (json_array_size(params) == 4) {
        if (!json_is_integer(json_array_get(params, 3)))
            return rpc_reply_error_invalid_argument(ses, pkg);
        side = json_integer_value(json_array_get(params, 3));
        if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    int ret = market_cancel_stop_all(true, user_id, account, market, side);
    if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("cancel_stop_all", params);
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_pending_stop(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    int account = json_integer_value(json_array_get(params, 1));

    // market
    market_t *market = NULL;
    if (json_is_string(json_array_get(params, 2))) {
        const char *market_name = json_string_value(json_array_get(params, 2));
        market = get_market(market_name);
        if (market == NULL)
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != 0 && side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 4)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 4));

    // limit
    if (!json_is_integer(json_array_get(params, 5)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 5));
    if (limit > ORDER_LIST_MAX_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *stops = json_array();
    skiplist_t *stop_list = get_user_stop_list(market, user_id, account);
    if (stop_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        size_t count = 0;
        size_t total = 0;
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(stop_list);
        while((node = skiplist_next(iter)) != NULL) {
            stop_t *stop = node->value;
            if (side && stop->side != side)
                continue;

            if (total >= offset && count < limit) {
                count += 1;
                json_array_append_new(stops, get_stop_info(stop));
            }
            total += 1;
        }
        skiplist_release_iterator(iter);
        json_object_set_new(result, "total", json_integer(total));
    }

    json_object_set_new(result, "records", stops);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_asset_config_callback(json_t *reply, nw_ses *ses, rpc_pkg *pkg)
{
    if (!reply) {
        log_info("update asset config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    if (settings.asset_cfg)
        json_decref(settings.asset_cfg);
    settings.asset_cfg = reply;

    int ret = update_asset();
    if (ret < 0) {
        log_info("update asset config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    log_info("update asset config success");
    return rpc_reply_success(ses, pkg);
}

static int on_market_config_callback(json_t *reply, nw_ses *ses, rpc_pkg *pkg)
{
    if (!reply) {
        log_info("update market config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    if (settings.market_cfg)
        json_decref(settings.market_cfg);
    settings.market_cfg = reply;

    int ret = update_trade();
    if (ret < 0) {
        log_info("update market config fail");
        return rpc_reply_error_internal_error(ses, pkg);
    }

    log_info("update market config success");
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_update_asset_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret = update_assert_config(ses, pkg, on_asset_config_callback);
    if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }
    return 0;
}

static int on_cmd_update_market_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret = update_market_config(ses, pkg, on_market_config_callback);
    if (ret < 0) {
        return rpc_reply_error_internal_error(ses, pkg);
    }
    return 0;
}

static int on_cmd_self_market_deal(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

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
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0 || mpd_cmp(price, mpd_maximum, &mpd_ctx) >= 0)
        goto invalid_argument;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        goto invalid_argument;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_TRADE_SIDE_SELL && side != MARKET_TRADE_SIDE_BUY)
        goto invalid_argument;

    int ret = market_self_deal(true, market, amount, price, side);

    mpd_del(amount);
    mpd_del(price);

    if (ret == -1) {
        return rpc_reply_error(ses, pkg, 10, "no reasonable price");
    } else if (ret < 0) {
        log_fatal("self_market_deal fail: %d", ret);
        return rpc_reply_error_internal_error(ses, pkg);
    }

    push_operlog("self_deal", params);
    ret = rpc_reply_success(ses, pkg);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);

    return rpc_reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_call_auction_start(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);

    market_start_call_auction(market);
    push_operlog("call_start", params);
    return rpc_reply_success(ses, pkg);
}

static int on_cmd_call_auction_execute(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return rpc_reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 0)))
        return rpc_reply_error_invalid_argument(ses, pkg);

    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return rpc_reply_error_invalid_argument(ses, pkg);
    
    if (!market->call_auction)
        return rpc_reply_error_invalid_argument(ses, pkg);

    mpd_t *volume = mpd_qncopy(mpd_zero);
    int ret = market_execute_call_auction(true, market, volume);
    json_t *result = json_object();
    if(ret == 0) {
        json_object_set_new_mpd(result, "price", market->last);
        json_object_set_new_mpd(result, "volume", volume);
    } else {
        json_object_set_new_mpd(result, "price", mpd_zero);
        json_object_set_new_mpd(result, "volume", mpd_zero);
    }
    ret = rpc_reply_result(ses, pkg, result);
    push_operlog("call_execute", params);
    mpd_del(volume);
    json_decref(result);
    return ret;
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

static bool is_service_available(void)
{
    bool queue_block = is_queue_block();
    bool operlog_block = is_operlog_block();
    bool message_block = is_message_block();
    if (queue_block || operlog_block || message_block) {
        log_fatal("service unavailable, queue: %d, operlog: %d, message: %d", queue_block, operlog_block, message_block);
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
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_update", 1);
        ret = on_cmd_asset_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_update %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_LOCK:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_lock", 1);
        ret = on_cmd_asset_lock(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_lock %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_UNLOCK:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_unlock", 1);
        ret = on_cmd_asset_unlock(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_unlock %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_BACKUP:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_asset_backup", 1);
        ret = on_cmd_asset_backup(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_backup %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_QUERY_INTIME:
        profile_inc("cmd_asset_query_intime", 1);
        ret = on_cmd_asset_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_intime %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_QUERY_ALL_INTIME:
        profile_inc("cmd_asset_query_all_intime", 1);
        ret = on_cmd_asset_query_all(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_all_intime %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_QUERY_LOCK_INTIME:
        profile_inc("cmd_asset_query_lock_intime", 1);
        ret = on_cmd_asset_query_lock(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_lock_intime %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_QUERY_USERS_INTIME:
        profile_inc("cmd_asset_query_users_intime", 1);
        ret = on_cmd_asset_query_users(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_query_users_intime %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_LIMIT:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_limit", 1);
        ret = on_cmd_order_put_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_MARKET:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_market", 1);
        ret = on_cmd_order_put_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_market %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_cancel", 1);
        ret = on_cmd_order_cancel(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL_BATCH:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_cancel_batch", 1);
        ret = on_cmd_order_cancel_batch(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel_batch %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL_ALL:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_cancel_all", 1);
        ret = on_cmd_order_cancel_all(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel_all %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PENDING_INTIME:
        profile_inc("cmd_order_pending_intime", 1);
        ret = on_cmd_order_pending(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_pending_intime %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_STOP_LIMIT:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_stop_limit", 1);
        ret = on_cmd_put_stop_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_put_stop_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_STOP_MARKET:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_put_stop_market", 1);
        ret = on_cmd_put_stop_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_put_stop_market %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL_STOP:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_cancel_stop", 1);
        ret = on_cmd_cancel_stop(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_cancel_stop%s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL_STOP_ALL:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_order_cancel_stop_all", 1);
        ret = on_cmd_cancel_stop_all(ses, pkg, params);
        if (ret < 0) {
            log_error("cmd_order_cancel_stop_all %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PENDING_STOP_INTIME:
        profile_inc("cmd_order_pending_stop_intime", 1);
        ret = on_cmd_pending_stop(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_pending_stop_intime %s fail: %d", params_str, ret);
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
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_market_self_deal", 1);
        ret = on_cmd_self_market_deal(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_self_market_deal fail: %d", ret);
        }
        break;
    case CMD_CALL_AUCTION_START:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_call_auction_start", 1);
        ret = on_cmd_call_auction_start(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_call_auction_start fail: %d", ret);
        }
        break;
    case CMD_CALL_AUCTION_EXECUTE:
        if (!is_service_available()) {
            rpc_reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        profile_inc("cmd_call_auction_execute", 1);
        ret = on_cmd_call_auction_execute(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_call_auction_execute fail: %d", ret);
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
        reply = sdscatprintf(reply, "queue: %d, used num: %u, used size: %u\n", i, mem_num, mem_size);
    }
    return reply;
}

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = market_status(reply);
    reply = operlog_status(reply);
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
    sds reply = sdsempty();
    if (argc != 4) {
        return sdscatprintf(reply, "usage: %s user_id account asset amount\n", cmd);
    }

    uint32_t user_id = strtoul(argv[0], NULL, 0);
    uint32_t account = strtoul(argv[1], NULL, 0);
    const char *asset = argv[2];
    int prec = asset_prec_show(account, asset);
    if (prec < 0) {
        return sdscatprintf(reply, "failed, asset not exist\n");
    }

    mpd_t *amount = decimal(argv[3], prec);
    if (!amount) {
        return sdscatprintf(reply, "failed, amount invalid\n");
    }

    mpd_t *frozen = balance_unfreeze(user_id, account, BALANCE_TYPE_FROZEN, asset, amount);
    if (!frozen) {
        mpd_del(amount);
        return sdscatprintf(reply, "unfreeze failed, user_id: %d\n", user_id);
    }

    mpd_del(amount);
    return sdscatprintf(reply, "unfreeze success, user_id: %d\n", user_id);
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
    mpd_maximum = mpd_new(&mpd_ctx);
    mpd_set_string(mpd_maximum, "1000000000000", &mpd_ctx);

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

    ret = init_request();
    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}
