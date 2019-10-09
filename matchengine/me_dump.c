/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include "ut_mysql.h"
# include "me_trade.h"
# include "me_update.h"
# include "me_market.h"
# include "me_balance.h"

static sds sql_append_mpd(sds sql, mpd_t *val, bool comma)
{
    char *str = mpd_to_sci(val, 0);
    sql = sdscatprintf(sql, "'%s'", str);
    if (comma) {
        sql = sdscatprintf(sql, ", ");
    }
    free(str);
    return sql;
}

static int dump_stops_list(MYSQL *conn, const char *table, skiplist_t *list)
{
    sds sql = sdsempty();

    size_t insert_limit = 1000;
    size_t index = 0;
    skiplist_iter *iter = skiplist_get_iterator(list);
    skiplist_node *node;
    while ((node = skiplist_next(iter)) != NULL) {
        stop_t *stop = node->value;
        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `t`, `side`, `create_time`, `update_time`, `user_id`, `account`, `option`, `market`, "
                    "`source`, `client_id`, `fee_asset`, `fee_discount`, `stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(%"PRIu64", %u, %u, %f, %f, %u, %u, %u, '%s', '%s', '%s', '%s', ",
                stop->id, stop->type, stop->side, stop->create_time, stop->update_time,
                stop->user_id, stop->account, stop->option, stop->market, stop->source,
                stop->client_id ? stop->client_id : "", stop->fee_asset ? stop->fee_asset : "");
        sql = sql_append_mpd(sql, stop->fee_discount, true);
        sql = sql_append_mpd(sql, stop->stop_price, true);
        sql = sql_append_mpd(sql, stop->price, true);
        sql = sql_append_mpd(sql, stop->amount, true);
        sql = sql_append_mpd(sql, stop->taker_fee, true);
        sql = sql_append_mpd(sql, stop->maker_fee, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                skiplist_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    skiplist_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
    return 0;
}

int dump_stops(MYSQL *conn, const char *table)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE IF EXISTS `%s`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_stop_example`", table);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        market_t *market = entry->val;
        int ret = dump_stops_list(conn, table, market->stop_high);
        if (ret < 0) {
            log_error("dump market: %s high stops list fail: %d", market->name, ret);
            return -__LINE__;
        }
        ret = dump_stops_list(conn, table, market->stop_low);
        if (ret < 0) {
            log_error("dump market: %s low stops list fail: %d", market->name, ret);
            return -__LINE__;
        }
    }
    dict_release_iterator(iter);

    return 0;
}

static int dump_orders_list(MYSQL *conn, const char *table, skiplist_t *list)
{
    sds sql = sdsempty();

    size_t insert_limit = 1000;
    size_t index = 0;
    skiplist_iter *iter = skiplist_get_iterator(list);
    skiplist_node *node;
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `t`, `side`, `create_time`, `update_time`, `user_id`, `account`, `option`, `market`, `source`, `fee_asset`, `client_id`, "
                    "`fee_discount`, `price`, `amount`, `taker_fee`, `maker_fee`, `left`, `frozen`, `deal_stock`, `deal_money`, `deal_fee`, `asset_fee`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(%"PRIu64", %u, %u, %f, %f, %u, %u, %u, '%s', '%s', '%s', '%s', ",
                order->id, order->type, order->side, order->create_time, order->update_time, order->user_id, order->account,
                order->option, order->market, order->source, order->fee_asset ? order->fee_asset : "",
                order->client_id ? order->client_id : "");
        sql = sql_append_mpd(sql, order->fee_discount, true);
        sql = sql_append_mpd(sql, order->price, true);
        sql = sql_append_mpd(sql, order->amount, true);
        sql = sql_append_mpd(sql, order->taker_fee, true);
        sql = sql_append_mpd(sql, order->maker_fee, true);
        sql = sql_append_mpd(sql, order->left, true);
        sql = sql_append_mpd(sql, order->frozen, true);
        sql = sql_append_mpd(sql, order->deal_stock, true);
        sql = sql_append_mpd(sql, order->deal_money, true);
        sql = sql_append_mpd(sql, order->deal_fee, true);
        sql = sql_append_mpd(sql, order->asset_fee, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                skiplist_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    skiplist_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
    return 0;
}

int dump_orders(MYSQL *conn, const char *table)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE IF EXISTS `%s`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_order_example`", table);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        market_t *market = entry->val;
        int ret = dump_orders_list(conn, table, market->asks);
        if (ret < 0) {
            log_error("dump market: %s asks orders list fail: %d", market->name, ret);
            return -__LINE__;
        }
        ret = dump_orders_list(conn, table, market->bids);
        if (ret < 0) {
            log_error("dump market: %s bids orders list fail: %d", market->name, ret);
            return -__LINE__;
        }
    }
    dict_release_iterator(iter);

    return 0;
}

static int dump_balance_dict(MYSQL *conn, const char *table, dict_t *dict)
{
    sds sql = sdsempty();

    size_t insert_limit = 1000;
    size_t index = 0;

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t user_id = (uintptr_t)entry->key;
        dict_t *dict_user = entry->val;

        dict_entry *entry_user;
        dict_iterator *iter_user = dict_get_iterator(dict_user);
        while ((entry_user = dict_next(iter_user)) != NULL) {
            uint32_t account = (uintptr_t)entry_user->key;
            dict_t *dict_account = entry_user->val;

            dict_entry *entry_account;
            dict_iterator *iter_account = dict_get_iterator(dict_account);
            while ((entry_account = dict_next(iter_account)) != NULL) {
                struct balance_key *key = entry_account->key;
                mpd_t *balance = entry_account->val;

                if (index == 0) {
                    sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `user_id`, `account`, `asset`, `t`, `balance`) VALUES ", table);
                } else {
                    sql = sdscatprintf(sql, ", ");
                }

                sql = sdscatprintf(sql, "(NULL, %u, %u, '%s', %u, ", user_id, account, key->asset, key->type);
                sql = sql_append_mpd(sql, balance, false);
                sql = sdscatprintf(sql, ")");

                index += 1;
                if (index == insert_limit) {
                    log_trace("exec sql: %s", sql);
                    int ret = mysql_real_query(conn, sql, sdslen(sql));
                    if (ret != 0) {
                        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                        dict_release_iterator(iter_account);
                        dict_release_iterator(iter_user);
                        dict_release_iterator(iter);
                        sdsfree(sql);
                        return -__LINE__;
                    }
                    sdsclear(sql);
                    index = 0;
                }
            }
            dict_release_iterator(iter_account);
        }
        dict_release_iterator(iter_user);
    }
    dict_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
    return 0;
}

int dump_balance(MYSQL *conn, const char *table)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE IF EXISTS `%s`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_balance_example`", table);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    ret = dump_balance_dict(conn, table, dict_balance);
    if (ret < 0) {
        log_error("dump_balance_dict fail: %d", ret);
        return -__LINE__;
    }

    return 0;
}

static int dump_update_dict(MYSQL *conn, const char *table, dict_t *dict)
{
    sds sql = sdsempty();

    size_t insert_limit = 1000;
    size_t index = 0;
    dict_iterator *iter = dict_get_iterator(dict);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct update_key *key = entry->key;
        struct update_val *val = entry->val;
        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `create_time`, `user_id`, `account`, `asset`, `business`, `business_id`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }
        sql = sdscatprintf(sql, "(NULL, %f, %u, %u, '%s', '%s', %"PRIu64")", val->create_time, key->user_id, key->account, key->asset, key->business, key->business_id);

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    dict_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
    return 0;
}

int dump_update(MYSQL *conn, const char *table)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE IF EXISTS `%s`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_update_example`", table);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    ret = dump_update_dict(conn, table, dict_update);
    if (ret < 0) {
        log_error("dump_update_dict fail: %d", ret);
        return -__LINE__;
    }

    return 0;
}

