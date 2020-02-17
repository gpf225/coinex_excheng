/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include "ut_mysql.h"
# include "me_trade.h"
# include "me_asset.h"
# include "me_market.h"
# include "me_update.h"
# include "me_balance.h"
# include "me_config.h"

int load_orders(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `t`, `side`, `create_time`, `update_time`, `user_id`, `account`, `market`, `source`, `fee_asset`, `fee_discount`, "
                "`price`, `amount`, `taker_fee`, `maker_fee`, `left`, `frozen`, `deal_stock`, `deal_money`, `deal_fee`, `asset_fee`, `option`, `client_id` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            market_t *market = get_market(row[7]);
            if (market == NULL)
                continue;

            order_t *order = malloc(sizeof(order_t));
            if (order == NULL)
                return -__LINE__;
            memset(order, 0, sizeof(order_t));

            order->id           = strtoull(row[0], NULL, 0);
            order->type         = strtoul(row[1], NULL, 0);
            order->side         = strtoul(row[2], NULL, 0);
            order->create_time  = strtod(row[3], NULL);
            order->update_time  = strtod(row[4], NULL);
            order->user_id      = strtoul(row[5], NULL, 0);
            order->account      = strtoul(row[6], NULL, 0);
            order->market       = strdup(row[7]);
            order->source       = strdup(row[8]);
            if (strlen(row[9]) == 0) {
                order->fee_asset = NULL;
            } else {
                order->fee_asset = strdup(row[9]);
            }
            order->fee_price    = mpd_qncopy(mpd_zero);
            order->fee_discount = decimal(row[10], 4);
            order->price        = decimal(row[11], market->money_prec);
            order->amount       = decimal(row[12], market->stock_prec);
            order->taker_fee    = decimal(row[13], market->fee_prec);
            order->maker_fee    = decimal(row[14], market->fee_prec);
            order->left         = decimal(row[15], market->stock_prec);
            order->frozen       = decimal(row[16], 0);
            order->deal_stock   = decimal(row[17], 0);
            order->deal_money   = decimal(row[18], 0);
            order->deal_fee     = decimal(row[19], 0);
            order->asset_fee    = decimal(row[20], 0);
            order->option       = strtoul(row[21], NULL, 0);
            order->last_deal_amount = mpd_qncopy(mpd_zero);
            order->last_deal_price  = mpd_qncopy(mpd_zero);
            if (strlen(row[22]) == 0) {
                order->client_id = NULL;
            } else {
                order->client_id = strdup(row[22]);
            }

            if (!order->market || !order->source || !order->price || !order->amount || !order->taker_fee || !order->maker_fee ||
                    !order->left || !order->frozen || !order->deal_stock || !order->deal_money || !order->deal_fee || !order->asset_fee) {
                log_error("get order detail of order id: %"PRIu64" fail", order->id);
                mysql_free_result(result);
                return -__LINE__;
            }

            int ret = market_put_order(market, order);
            if (ret != 0) {
                log_stderr("market_put_order fail ret: %d", ret);
                mysql_free_result(result);
                return -__LINE__;
            }
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

int load_stops(MYSQL *conn, const char *table)
{
    if (!is_table_exists(conn, table)) {
        log_stderr("table %s not exist", table);
        return 0;
    }

    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `t`, `side`, `create_time`, `update_time`, `user_id`, `account`, `market`, `source`, "
                "`fee_asset`, `fee_discount`, `stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`, `option`, `client_id` FROM `%s` "
                "WHERE `id` > %"PRIu64" order BY `id` LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            market_t *market = get_market(row[7]);
            if (market == NULL)
                continue;

            stop_t *stop = malloc(sizeof(stop_t));
            if (stop == NULL) {
                mysql_free_result(result);
                return -__LINE__;
            }
            memset(stop, 0, sizeof(stop_t));

            stop->id            = strtoull(row[0], NULL, 0);
            stop->type          = strtoul(row[1], NULL, 0);
            stop->side          = strtoul(row[2], NULL, 0);
            stop->create_time   = strtod(row[3], NULL);
            stop->update_time   = strtod(row[4], NULL);
            stop->user_id       = strtoul(row[5], NULL, 0);
            stop->account       = strtoul(row[6], NULL, 0);
            stop->market        = strdup(row[7]);
            stop->source        = strdup(row[8]);
            if (strlen(row[9]) == 0) {
                stop->fee_asset = NULL;
            } else {
                stop->fee_asset = strdup(row[9]);
            }
            stop->fee_discount  = decimal(row[10],  4);
            stop->stop_price    = decimal(row[11], market->money_prec);
            stop->price         = decimal(row[12], market->money_prec);
            stop->amount        = decimal(row[13], market->stock_prec);
            stop->taker_fee     = decimal(row[14], market->fee_prec);
            stop->maker_fee     = decimal(row[15], market->fee_prec);
            stop->option        = strtoul(row[16], NULL, 0);
            if (strlen(row[17]) == 0){
                stop->client_id = NULL;
            } else {
                stop->client_id = strdup(row[17]);
            }

            if (mpd_cmp(stop->stop_price, market->last, &mpd_ctx) < 0) {
                stop->state = STOP_STATE_LOW;
            } else {
                stop->state = STOP_STATE_HIGH;
            }

            if (!stop->market || !stop->source || !stop->stop_price || !stop->price || !stop->amount || !stop->taker_fee || !stop->maker_fee) {
                log_error("get stop detail of stop id: %"PRIu64" fail", stop->id);
                mysql_free_result(result);
                return -__LINE__;
            }

            int ret = market_put_stop(market, stop);
            if (ret != 0) {
                log_stderr("market_put_stop fail ret: %d", ret);
                mysql_free_result(result);
                return -__LINE__;
            }
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

int load_balance(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `user_id`, `account`, `asset`, `t`, `balance` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY id LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            uint32_t user_id = strtoul(row[1], NULL, 0);
            uint32_t account = strtoul(row[2], NULL, 0);
            const char *asset = row[3];
            if (!asset_exist(account, asset)) {
                continue;
            }
            uint32_t type = strtoul(row[4], NULL, 0);
            mpd_t *balance = decimal(row[5], asset_prec_save(account, asset));
            balance_set(user_id, account, type, asset, balance);
            mpd_del(balance);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

int load_update(MYSQL *conn, const char *table)
{
    if (!is_table_exists(conn, table)) {
        log_stderr("table %s not exist", table);
        return 0;
    }

    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `create_time`, `user_id`, `account`, `asset`, `business`, `business_id` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY id LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            double create_time = strtod(row[1], NULL);
            uint32_t user_id = strtoul(row[2], NULL, 0);
            uint32_t account = strtoul(row[3], NULL, 0);
            const char *asset = row[4];
            const char *business = row[5];
            uint64_t business_id = strtoull(row[6], NULL, 0);
            update_add(user_id, account, asset, business, business_id, create_time);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

static int load_update_balance(json_t *params)
{
    if (json_array_size(params) != 7)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // asset
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *asset = json_string_value(json_array_get(params, 2));
    int prec = asset_prec_show(account, asset);
    if (prec < 0)
        return 0;

    // business
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    const char *business = json_string_value(json_array_get(params, 3));

    // business_id
    if (!json_is_integer(json_array_get(params, 4)))
        return -__LINE__;
    uint64_t business_id = json_integer_value(json_array_get(params, 4));

    // change
    if (!json_is_string(json_array_get(params, 5)))
        return -__LINE__;
    mpd_t *change = decimal(json_string_value(json_array_get(params, 5)), prec);
    if (change == NULL)
        return -__LINE__;

    // detail
    json_t *detail = json_array_get(params, 6);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return -__LINE__;
    }

    int ret = update_user_balance(false, user_id, account, asset, business, business_id, change, detail);
    mpd_del(change);

    if (ret < 0) {
        log_stderr("update_user_balance failed, ret:%d", ret);
        return -__LINE__;
    }

    return 0;
}

static int load_asset_lock(json_t *params)
{
    if (json_array_size(params) != 6)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // asset
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *asset = json_string_value(json_array_get(params, 2));
    int prec = asset_prec_show(account, asset);
    if (prec < 0)
        return 0;

    // business
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    const char *business = json_string_value(json_array_get(params, 3));

    // business_id
    if (!json_is_integer(json_array_get(params, 4)))
        return -__LINE__;
    uint64_t business_id = json_integer_value(json_array_get(params, 4));

    // amount
    if (!json_is_string(json_array_get(params, 5)))
        return -__LINE__;
    mpd_t *amount = decimal(json_string_value(json_array_get(params, 5)), prec);
    if (amount == NULL)
        return -__LINE__;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(amount);
        return -__LINE__;
    }

    int ret = update_user_lock(false, user_id, account, asset, business, business_id, amount);
    mpd_del(amount);
    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}

static int load_asset_unlock(json_t *params)
{
    if (json_array_size(params) != 6)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // asset
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *asset = json_string_value(json_array_get(params, 2));
    int prec = asset_prec_show(account, asset);
    if (prec < 0)
        return 0;

    // business
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    const char *business = json_string_value(json_array_get(params, 3));

    // business_id
    if (!json_is_integer(json_array_get(params, 4)))
        return -__LINE__;
    uint64_t business_id = json_integer_value(json_array_get(params, 4));

    // amount
    if (!json_is_string(json_array_get(params, 5)))
        return -__LINE__;
    mpd_t *amount = decimal(json_string_value(json_array_get(params, 5)), prec);
    if (amount == NULL)
        return -__LINE__;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(amount);
        return -__LINE__;
    }

    int ret = update_user_unlock(false, user_id, account, asset, business, business_id, amount);
    mpd_del(amount);
    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}

static int load_limit_order(json_t *params)
{
    if (json_array_size(params) < 11)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount       = NULL;
    mpd_t *price        = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *maker_fee    = NULL;
    mpd_t *fee_price    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // price 
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    price = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
    if (price == NULL) 
        goto error;
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // maker fee
    if (!json_is_string(json_array_get(params, 7)))
        goto error;
    maker_fee = decimal(json_string_value(json_array_get(params, 7)), market->fee_prec);
    if (maker_fee == NULL)
        goto error;
    if (mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 8)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 8));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 9))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 9));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto error;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 10))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 10)), 4);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto error;
    }

    // option
    uint32_t option = 0;
    if (json_array_size(params) >= 12) {
        if (json_is_integer(json_array_get(params, 11))) {
            option = json_integer_value(json_array_get(params, 11));
            if ((option & (~OPTION_CHECK_MASK)) != 0)
                goto error;
        }
    }

    // client self-define id
    const char *client_id = NULL;
    if (json_array_size(params) >= 13) {
        if (json_is_string(json_array_get(params, 12))) {
            client_id = json_string_value(json_array_get(params, 12));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN)
                goto error;
        }
    }

    int ret = market_put_limit_order(false, NULL, market, user_id, account, side, amount, price, taker_fee, maker_fee, source, fee_asset, fee_price, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return ret;

error:
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

    return -__LINE__;
}

static int load_market_order(json_t *params)
{
    if (json_array_size(params) < 9)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount       = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *fee_price    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 6)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 6));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 7))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 7));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto error;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 8))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 8)), 4);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto error;
    }

    // option
    uint32_t option = 0;
    if (json_array_size(params) >= 10) {
        if (json_is_integer(json_array_get(params, 9))) {
            option = json_integer_value(json_array_get(params, 9));
            if ((option & (~OPTION_CHECK_MASK)) != 0)
                goto error;
        }
    }

    // client self-define id
    const char *client_id = NULL;
    if (json_array_size(params) >= 11) {
        if (json_is_string(json_array_get(params, 10))) {
            client_id = json_string_value(json_array_get(params, 10));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN)
                goto error;
        }
    }

    int ret = market_put_market_order(false, NULL, market, user_id, account, side, amount, taker_fee, source, fee_asset, fee_price, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(taker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return ret;

error:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);
  
    return -__LINE__;
}

static int load_cancel_order(json_t *params)
{
    if (json_array_size(params) != 3)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }

    int ret = market_cancel_order(false, NULL, market, order);
    if (ret < 0) {
        log_error("market_cancel_order id: %"PRIu64", user id: %u, market: %s", order_id, user_id, market_name);
        return -__LINE__;
    }

    return 0;
}

static int load_cancel_order_all(json_t *params)
{
    if (json_array_size(params) < 3)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    int32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    uint32_t side = 0;
    if (json_array_size(params) == 4) {
        if (!json_is_integer(json_array_get(params, 3)))
            return -__LINE__;
        side = json_integer_value(json_array_get(params, 3));
        if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
            return -__LINE__;
    }

    int ret = market_cancel_order_all(false, user_id, account, market, side);
    if (ret < 0) {
        log_error("market_cancel_order_all user id: %u, account: %d, market: %s", user_id, account, market_name);
        return -__LINE__;
    }

    return 0;
}

static int load_stop_limit(json_t *params)
{
    if (json_array_size(params) < 12)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount       = NULL;
    mpd_t *stop_price   = NULL;
    mpd_t *price        = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *maker_fee    = NULL;
    mpd_t *fee_price    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // stop price 
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    stop_price = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
    if (stop_price == NULL) 
        goto error;
    if (mpd_cmp(stop_price, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // price 
    if (!json_is_string(json_array_get(params, 6)))
        goto error;
    price = decimal(json_string_value(json_array_get(params, 6)), market->money_prec);
    if (price == NULL) 
        goto error;
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 7)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 7)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // maker fee
    if (!json_is_string(json_array_get(params, 8)))
        goto error;
    maker_fee = decimal(json_string_value(json_array_get(params, 8)), market->fee_prec);
    if (maker_fee == NULL)
        goto error;
    if (mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 9)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 9));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 10))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 10));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto error;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 11))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 11)), 4);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto error;
    }

    // option
    uint32_t option = 0;
    if (json_array_size(params) >= 13) {
        if (json_is_integer(json_array_get(params, 12))) {
            option = json_integer_value(json_array_get(params, 12));
            if ((option & (~OPTION_CHECK_MASK)) != 0)
                goto error;
        }
    }

    // client self-define id
    const char *client_id = NULL;
    if (json_array_size(params) >= 14) {
        if (json_is_string(json_array_get(params, 13))) {
            client_id = json_string_value(json_array_get(params, 13));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN)
                goto error;
        }
    }

    int ret = market_put_stop_limit(false, market, user_id, account, side, amount, stop_price, price, taker_fee, maker_fee, source, fee_asset, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(stop_price);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return ret;

error:
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

    return -__LINE__;
}

static int load_stop_market(json_t *params)
{
    if (json_array_size(params) < 10)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account 
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    uint32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount       = NULL;
    mpd_t *stop_price   = NULL;
    mpd_t *taker_fee    = NULL;
    mpd_t *fee_price    = NULL;
    mpd_t *fee_discount = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 4)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // stop price 
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    stop_price = decimal(json_string_value(json_array_get(params, 5)), market->money_prec);
    if (stop_price == NULL) 
        goto error;
    if (mpd_cmp(stop_price, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    // fee asset
    const char *fee_asset = NULL;
    if (json_is_string(json_array_get(params, 8))) {
        fee_price = mpd_new(&mpd_ctx);
        fee_asset = json_string_value(json_array_get(params, 8));
        get_fee_price(market, fee_asset, fee_price);
        if (mpd_cmp(fee_price, mpd_zero, &mpd_ctx) == 0)
            goto error;
    }

    // fee discount
    if (fee_asset && json_is_string(json_array_get(params, 9))) {
        fee_discount = decimal(json_string_value(json_array_get(params, 9)), 4);
        if (fee_discount == NULL || mpd_cmp(fee_discount, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(fee_discount, mpd_one, &mpd_ctx) > 0)
            goto error;
    }

    // option
    uint32_t option = 0;
    if (json_array_size(params) >= 11) {
        if (json_is_integer(json_array_get(params, 10))) {
            option = json_integer_value(json_array_get(params, 10));
            if ((option & (~OPTION_CHECK_MASK)) != 0)
                goto error;
        }
    }

    // client self-define id
    const char *client_id = NULL;
    if (json_array_size(params) >= 12) {
        if (json_is_string(json_array_get(params, 11))) {
            client_id = json_string_value(json_array_get(params, 11));
            if (strlen(client_id) > CLIENT_ID_MAX_LEN)
                goto error;
        }
    }

    int ret = market_put_stop_market(false, market, user_id, account, side, amount, stop_price, taker_fee, source, fee_asset, fee_discount, option, client_id);

    mpd_del(amount);
    mpd_del(stop_price);
    mpd_del(taker_fee);
    if (fee_price)
        mpd_del(fee_price);
    if (fee_discount)
        mpd_del(fee_discount);

    return ret;

error:
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

    return -__LINE__;
}

static int load_cancel_stop(json_t *params)
{
    if (json_array_size(params) != 3)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    stop_t *stop = market_get_stop(market, order_id);
    if (stop == NULL) {
        return -__LINE__;
    }

    int ret = market_cancel_stop(false, NULL, market, stop);
    if (ret < 0) {
        log_error("market_cancel_order id: %"PRIu64", user id: %u, market: %s", order_id, user_id, market_name);
        return -__LINE__;
    }

    return 0;
}

static int load_cancel_stop_all(json_t *params)
{
    if (json_array_size(params) < 3)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // account
    if (!json_is_integer(json_array_get(params, 1)))
        return -__LINE__;
    int32_t account = json_integer_value(json_array_get(params, 1));

    // market
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 2));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    uint32_t side = 0;
    if (json_array_size(params) == 4) {
        if (!json_is_integer(json_array_get(params, 3)))
            return -__LINE__;
        side = json_integer_value(json_array_get(params, 3));
        if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
            return -__LINE__;
    }

    int ret = market_cancel_stop_all(false, user_id, account, market, side);
    if (ret < 0) {
        log_error("market_cancel_stop_all user id: %u, account: %d, market: %s", user_id, account, market_name);
        return -__LINE__;
    }

    return 0;
}

static int load_self_deal(json_t *params)
{
    if (json_array_size(params) != 4)
        return -__LINE__;

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    mpd_t *amount  = NULL;
    mpd_t *price   = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 1)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 1)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // price 
    if (!json_is_string(json_array_get(params, 2)))
        goto error;
    price = decimal(json_string_value(json_array_get(params, 2)), market->money_prec);
    if (price == NULL) 
        goto error;
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        goto error;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != MARKET_TRADE_SIDE_SELL && side != MARKET_TRADE_SIDE_BUY)
        goto error;

    int ret = market_self_deal(false, market, amount, price, side);

    mpd_del(amount);
    mpd_del(price);
    return ret;
error:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);

    return __LINE__;
}

static int load_call_auction_start(json_t *params)
{
    if (json_array_size(params) != 1)
        return -__LINE__;

    if (!json_is_string(json_array_get(params, 0)))
        return -__LINE__;

    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    market_start_call_auction(market);
    return 0;
}

static int load_call_auction_execute(json_t *params)
{
    if (json_array_size(params) != 1)
        return -__LINE__;

    if (!json_is_string(json_array_get(params, 0)))
        return -__LINE__;

    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    market_execute_call_auction(false, market, NULL);
    return 0;
}

int load_oper(json_t *detail)
{
    const char *method = json_string_value(json_object_get(detail, "method"));
    if (method == NULL)
        return -__LINE__;
    json_t *params = json_object_get(detail, "params");
    if (params == NULL || !json_is_array(params))
        return -__LINE__;

    int ret = 0;
    if (strcmp(method, "update_balance") == 0) {
        ret = load_update_balance(params);
    } else if (strcmp(method, "asset_lock") == 0) {
        ret = load_asset_lock(params);
    } else if (strcmp(method, "asset_unlock") == 0) {
        ret = load_asset_unlock(params);
    } else if (strcmp(method, "limit_order") == 0) {
        ret = load_limit_order(params);
    } else if (strcmp(method, "market_order") == 0) {
        ret = load_market_order(params);
    } else if (strcmp(method, "cancel_order") == 0) {
        ret = load_cancel_order(params);
    } else if (strcmp(method, "cancel_order_all") == 0) {
        ret = load_cancel_order_all(params);
    } else if (strcmp(method, "stop_limit") == 0) {
        ret = load_stop_limit(params);
    } else if (strcmp(method, "stop_market") == 0) {
        ret = load_stop_market(params);
    } else if (strcmp(method, "cancel_stop") == 0) {
        ret = load_cancel_stop(params);
    } else if (strcmp(method, "cancel_stop_all") == 0) {
        ret = load_cancel_stop_all(params);
    } else if (strcmp(method, "self_deal") == 0) {
        ret = load_self_deal(params);
    } else if (strcmp(method, "call_start") == 0) {
        ret = load_call_auction_start(params);
    } else if (strcmp(method, "call_execute") == 0) {
        ret = load_call_auction_execute(params);
    } else {
        return -__LINE__;
    }

    return ret;
}

int load_operlog(MYSQL *conn, const char *table, uint64_t *start_id)
{
    size_t query_limit = 1000;
    uint64_t last_id = *start_id;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `detail` from `%s` WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            uint64_t id = strtoull(row[0], NULL, 0);
            if (id != last_id + 1) {
                log_error("invalid id: %"PRIu64", last id: %"PRIu64"", id, last_id);
                return -__LINE__;
            }
            last_id = id;
            json_t *detail = json_loadb(row[1], strlen(row[1]), 0, NULL);
            if (detail == NULL) {
                log_error("invalid detail data: %s", row[1]);
                mysql_free_result(result);
                return -__LINE__;
            }

            ret = load_oper(detail);
            if (ret < 0) {
                char *detail_msg = json_dumps(detail, 0);
                log_stderr("detail:%s", detail_msg);
                log_stderr("load_oper: %"PRIu64":%s fail: %d", id, row[1], ret);
                
                free(detail_msg);
                json_decref(detail);
                mysql_free_result(result);
                return -__LINE__;
            }
            json_decref(detail);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    *start_id = last_id;
    return 0;
}

