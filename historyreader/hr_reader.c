/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/24, create
 */

# include "hr_config.h"
# include "hr_reader.h"
# include "ut_decimal.h"

json_t *get_user_balance_history(MYSQL *conn, uint32_t user_id, uint32_t account,
        const char *asset, const char *business, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `user_id`, `account`, `time`, `asset`, `business`, `change`, `balance`, `detail` FROM `balance_history_%u` WHERE `user_id` = %u AND `account` = %u",
            user_id % HISTORY_HASH_NUM, user_id, account);

    size_t asset_len = strlen(asset);
    if (asset_len > 0) {
        char _asset[2 * asset_len + 1];
        mysql_real_escape_string(conn, _asset, asset, strlen(asset));
        sql = sdscatprintf(sql, " AND `asset` = '%s'", _asset);
    }
    size_t business_len = strlen(business);
    if (business_len > 0) {
        char _business[2 * business_len + 1];
        mysql_real_escape_string(conn, _business, business, strlen(business));
        sql = sdscatprintf(sql, " AND `business` = '%s'", _business);
    }

    if (start_time) {
        sql = sdscatprintf(sql, " AND `time` >= %"PRIu64, start_time);
    }
    if (end_time) {
        sql = sdscatprintf(sql, " AND `time` < %"PRIu64, end_time);
    }

    sql = sdscatprintf(sql, " ORDER BY `time` DESC, `id` DESC");
    if (offset) {
        sql = sdscatprintf(sql, " LIMIT %zu, %zu", offset, limit);
    } else {
        sql = sdscatprintf(sql, " LIMIT %zu", limit);
    }

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *records = json_array();
    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);

        uint32_t user_id = strtoul(row[0], NULL, 0);
        uint32_t account = strtoul(row[1], NULL, 0);
        double timestamp = strtod(row[2], NULL);
        json_object_set_new(record, "user", json_integer(user_id));
        json_object_set_new(record, "account", json_integer(account));
        json_object_set_new(record, "time", json_real(timestamp));
        json_object_set_new(record, "asset", json_string(row[3]));
        json_object_set_new(record, "business", json_string(row[4]));
        json_object_set_new(record, "change", json_string(rstripzero(row[5])));
        json_object_set_new(record, "balance", json_string(rstripzero(row[6])));
        json_t *detail = json_loads(row[7], 0, NULL);
        if (detail == NULL || !json_is_object(detail)) {
            if (detail) {
                json_decref(detail);
            }
            detail = json_object();
        }
        json_object_set_new(record, "detail", detail);

        json_array_append_new(records, record);
    }
    mysql_free_result(result);

    return records;
}

json_t *get_user_order_history(MYSQL *conn, uint32_t user_id, int32_t account, 
        const char *market, int side, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `order_id`, `create_time`, `finish_time`, `user_id`, `account`, `option`, `market`, `source`, `t`, `side`, `price`, `amount`, "
            "`taker_fee`, `maker_fee`, `deal_stock`, `deal_money`, `money_fee`, `stock_fee`, `fee_asset`, `fee_discount`, `asset_fee`, `client_id` "
            "FROM `order_history_%u`", user_id % HISTORY_HASH_NUM);

    size_t market_len = strlen(market);
    if (account >= 0 && market_len > 0 && side) {
        sql = sdscat(sql, " use index(idx_user_account_market_side_time)");
    } else if (account >= 0 && market_len) {
        sql = sdscat(sql, " use index(idx_user_account_market_time)");
    } else if (market_len > 0 && side) {
        sql = sdscat(sql, " use index(idx_user_market_side_time)");
    } else if (market_len) {
        sql = sdscat(sql, " use index(idx_user_market_time)");
    }
    sql = sdscatprintf(sql, " where `user_id` = %u", user_id);

    if (account >= 0) {
        sql = sdscatprintf(sql, " AND `account` = %u", account);
    }
    if (market_len > 0) {
        char _market[2 * market_len + 1];
        mysql_real_escape_string(conn, _market, market, strlen(market));
        sql = sdscatprintf(sql, " AND `market` = '%s'", _market);
    }
    if (side) {
        sql = sdscatprintf(sql, " AND `side` = %d", side);
    }
    if (start_time) {
        sql = sdscatprintf(sql, " AND `create_time` >= %"PRIu64, start_time);
    }
    if (end_time) {
        sql = sdscatprintf(sql, " AND `create_time` < %"PRIu64, end_time);
    }

    sql = sdscatprintf(sql, " ORDER BY `create_time` DESC, `id` DESC");
    if (offset) {
        sql = sdscatprintf(sql, " LIMIT %zu, %zu", offset, limit);
    } else {
        sql = sdscatprintf(sql, " LIMIT %zu", limit);
    }

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *records = json_array();
    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);

        uint64_t order_id = strtoull(row[0], NULL, 0);
        json_object_set_new(record, "id", json_integer(order_id));
        double ctime = strtod(row[1], NULL);
        json_object_set_new(record, "ctime", json_real(ctime));
        double ftime = strtod(row[2], NULL);
        json_object_set_new(record, "ftime", json_real(ftime));
        uint32_t user_id = strtoul(row[3], NULL, 0);
        json_object_set_new(record, "user", json_integer(user_id));
        uint32_t account = strtoul(row[4], NULL, 0);
        json_object_set_new(record, "account", json_integer(account));
        uint32_t option = strtoul(row[5], NULL, 0);
        json_object_set_new(record, "option", json_integer(option));
        json_object_set_new(record, "market", json_string(row[6]));
        json_object_set_new(record, "source", json_string(row[7]));
        uint32_t type = atoi(row[8]);
        json_object_set_new(record, "type", json_integer(type));
        uint32_t side = atoi(row[9]);
        json_object_set_new(record, "side", json_integer(side));
        json_object_set_new(record, "price", json_string(rstripzero(row[10])));
        json_object_set_new(record, "amount", json_string(rstripzero(row[11])));
        json_object_set_new(record, "taker_fee", json_string(rstripzero(row[12])));
        json_object_set_new(record, "maker_fee", json_string(rstripzero(row[13])));
        json_object_set_new(record, "deal_stock", json_string(rstripzero(row[14])));
        json_object_set_new(record, "deal_money", json_string(rstripzero(row[15])));
        json_object_set_new(record, "money_fee", json_string(rstripzero(row[16])));
        json_object_set_new(record, "stock_fee", json_string(rstripzero(row[17])));
        json_object_set_new(record, "fee_asset", json_string(row[18]));
        json_object_set_new(record, "fee_discount", json_string(rstripzero(row[19])));
        json_object_set_new(record, "asset_fee", json_string(rstripzero(row[20])));
        json_object_set_new(record, "client_id", json_string(row[21]));

        json_array_append_new(records, record);
    }
    mysql_free_result(result);

    return records;
}

json_t *get_user_stop_history(MYSQL *conn, uint32_t user_id, int32_t account, 
        const char *market, int side, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `order_id`, `create_time`, `finish_time`, `user_id`, `account`, `option`, `market`, `source`, `t`, `side`, "
            "`stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`, `fee_asset`, `fee_discount`, `status`, `client_id` "
            "FROM `stop_history_%u`", user_id % HISTORY_HASH_NUM);

    size_t market_len = strlen(market);
    if (account >= 0 && market_len > 0 && side) {
        sql = sdscat(sql, " use index(idx_user_account_market_side_time)");
    } else if (account >= 0 && market_len) {
        sql = sdscat(sql, " use index(idx_user_account_market_time)");
    } else if (market_len > 0 && side) {
        sql = sdscat(sql, " use index(idx_user_market_side_time)");
    } else if (market_len) {
        sql = sdscat(sql, " use index(idx_user_market_time)");
    }
    sql = sdscatprintf(sql, " where `user_id` = %u", user_id);

    if (account >= 0) {
        sql = sdscatprintf(sql, " AND `account` = %u", account);
    }
    if (market_len > 0) {
        char _market[2 * market_len + 1];
        mysql_real_escape_string(conn, _market, market, strlen(market));
        sql = sdscatprintf(sql, " AND `market` = '%s'", _market);
    }
    if (side) {
        sql = sdscatprintf(sql, " AND `side` = %d", side);
    }
    
    sql = sdscatprintf(sql, " AND `status` != 3");

    if (start_time) {
        sql = sdscatprintf(sql, " AND `create_time` >= %"PRIu64, start_time);
    }
    if (end_time) {
        sql = sdscatprintf(sql, " AND `create_time` < %"PRIu64, end_time);
    }

    sql = sdscatprintf(sql, " ORDER BY `create_time` DESC, `id` DESC");
    if (offset) {
        sql = sdscatprintf(sql, " LIMIT %zu, %zu", offset, limit);
    } else {
        sql = sdscatprintf(sql, " LIMIT %zu", limit);
    }

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *records = json_array();
    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);

        uint64_t order_id = strtoull(row[0], NULL, 0);
        json_object_set_new(record, "id", json_integer(order_id));
        double ctime = strtod(row[1], NULL);
        json_object_set_new(record, "ctime", json_real(ctime));
        double ftime = strtod(row[2], NULL);
        json_object_set_new(record, "ftime", json_real(ftime));
        uint32_t user_id = strtoul(row[3], NULL, 0);
        json_object_set_new(record, "user", json_integer(user_id));
        uint32_t account = strtoul(row[4], NULL, 0);
        json_object_set_new(record, "account", json_integer(account));
        uint32_t option = strtoul(row[5], NULL, 0);
        json_object_set_new(record, "option", json_integer(option));
        json_object_set_new(record, "market", json_string(row[6]));
        json_object_set_new(record, "source", json_string(row[7]));
        uint32_t type = atoi(row[8]);
        json_object_set_new(record, "type", json_integer(type));
        uint32_t side = atoi(row[9]);
        json_object_set_new(record, "side", json_integer(side));
        json_object_set_new(record, "stop_price", json_string(rstripzero(row[10])));
        json_object_set_new(record, "price", json_string(rstripzero(row[11])));
        json_object_set_new(record, "amount", json_string(rstripzero(row[12])));
        json_object_set_new(record, "taker_fee", json_string(rstripzero(row[13])));
        json_object_set_new(record, "maker_fee", json_string(rstripzero(row[14])));
        json_object_set_new(record, "fee_asset", json_string(row[15]));
        json_object_set_new(record, "fee_discount", json_string(rstripzero(row[16])));
        uint32_t status = atoi(row[17]);
        json_object_set_new(record, "status", json_integer(status));
        json_object_set_new(record, "client_id", json_string(row[18]));

        json_array_append_new(records, record);
    }
    mysql_free_result(result);

    return records;
}

json_t *get_user_deal_history(MYSQL *conn, uint32_t user_id, int32_t account, 
        const char *market, int side, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `time`, `user_id`, `account`, `deal_user_id`, `deal_id`, `order_id`, `market`, `side`, `role`, `price`, `amount`, `deal`, `fee`, `fee_asset` "
            "FROM `user_deal_history_%u`", user_id % HISTORY_HASH_NUM);

    size_t market_len = strlen(market);
    if (account >= 0 && market_len > 0 && side) {
        sql = sdscat(sql, " use index(idx_user_account_market_side_time)");
    } else if (account >= 0 && market_len > 0) {
        sql = sdscat(sql, " use index(idx_user_account_market_time)");
    } else if (market_len > 0 && side) {
        sql = sdscat(sql, " use index(idx_user_market_side_time)");
    } else if (market_len) {
        sql = sdscat(sql, " use index(idx_user_market_time)");
    }
    sql = sdscatprintf(sql, " where `user_id` = %u", user_id);

    if (account >= 0) {
        sql = sdscatprintf(sql, " AND `account` = %u", account);
    }
    if (market_len > 0) {
        char _market[2 * market_len + 1];
        mysql_real_escape_string(conn, _market, market, strlen(market));
        sql = sdscatprintf(sql, " AND `market` = '%s'", _market);
    }
    if (side) {
        sql = sdscatprintf(sql, " AND `side` = %d", side);
    }
    if (start_time) {
        sql = sdscatprintf(sql, " AND `time` >= %"PRIu64, start_time);
    }
    if (end_time) {
        sql = sdscatprintf(sql, " AND `time` < %"PRIu64, end_time);
    }

    sql = sdscatprintf(sql, " ORDER BY `time` DESC, `id` DESC");
    if (offset) {
        sql = sdscatprintf(sql, " LIMIT %zu, %zu", offset, limit);
    } else {
        sql = sdscatprintf(sql, " LIMIT %zu", limit);
    }

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *records = json_array();
    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);

        double timestamp = strtod(row[0], NULL);
        json_object_set_new(record, "time", json_real(timestamp));
        uint32_t user_id = strtoul(row[1], NULL, 0);
        json_object_set_new(record, "user", json_integer(user_id));
        uint32_t account = strtoul(row[2], NULL, 0);
        json_object_set_new(record, "account", json_integer(account));
        uint32_t deal_user_id = strtoul(row[3], NULL, 0);
        json_object_set_new(record, "deal_user", json_integer(deal_user_id));
        uint64_t deal_id = strtoull(row[4], NULL, 0);
        json_object_set_new(record, "id", json_integer(deal_id));
        uint64_t order_id = strtoull(row[5], NULL, 0);
        json_object_set_new(record, "order_id", json_integer(order_id));
        json_object_set_new(record, "market", json_string(row[6]));
        int side = atoi(row[7]);
        json_object_set_new(record, "side", json_integer(side));
        int role = atoi(row[8]);
        json_object_set_new(record, "role", json_integer(role));
        json_object_set_new(record, "price", json_string(rstripzero(row[9])));
        json_object_set_new(record, "amount", json_string(rstripzero(row[10])));
        json_object_set_new(record, "deal", json_string(rstripzero(row[11])));
        json_object_set_new(record, "fee", json_string(rstripzero(row[12])));
        json_object_set_new(record, "fee_asset", json_string(row[13]));

        json_array_append_new(records, record);
    }
    mysql_free_result(result);

    return records;
}

json_t *get_order_detail(MYSQL *conn, uint32_t user_id, uint64_t order_id)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `order_id`, `create_time`, `finish_time`, `user_id`, `account`, `option`, `market`, `source`, `t`, `side`, `price`, `amount`, "
            "`taker_fee`, `maker_fee`, `deal_stock`, `deal_money`, `money_fee`, `stock_fee`, `fee_asset`, `fee_discount`, `asset_fee`, `client_id` "
            "FROM `order_history_%u` WHERE `user_id` = '%u' AND `order_id` = %"PRIu64"", user_id % HISTORY_HASH_NUM, user_id, order_id);

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    if (num_rows == 0) {
        mysql_free_result(result);
        return json_null();
    }

    MYSQL_ROW row = mysql_fetch_row(result);

    json_t *detail = json_object();
    json_object_set_new(detail, "id", json_integer(order_id));
    double ctime = strtod(row[1], NULL);
    json_object_set_new(detail, "ctime", json_real(ctime));
    double ftime = strtod(row[2], NULL);
    json_object_set_new(detail, "ftime", json_real(ftime));
    user_id = strtoul(row[3], NULL, 0);
    json_object_set_new(detail, "user", json_integer(user_id));
    uint32_t account = strtoul(row[4], NULL, 0);
    json_object_set_new(detail, "account", json_integer(account));
    uint32_t option = strtoul(row[5], NULL, 0);
    json_object_set_new(detail, "option", json_integer(option));
    json_object_set_new(detail, "market", json_string(row[6]));
    json_object_set_new(detail, "source", json_string(row[7]));
    uint32_t type = atoi(row[8]);
    json_object_set_new(detail, "type", json_integer(type));
    uint32_t side = atoi(row[9]);
    json_object_set_new(detail, "side", json_integer(side));
    json_object_set_new(detail, "price", json_string(rstripzero(row[10])));
    json_object_set_new(detail, "amount", json_string(rstripzero(row[11])));
    json_object_set_new(detail, "taker_fee", json_string(rstripzero(row[12])));
    json_object_set_new(detail, "maker_fee", json_string(rstripzero(row[13])));
    json_object_set_new(detail, "deal_stock", json_string(rstripzero(row[14])));
    json_object_set_new(detail, "deal_money", json_string(rstripzero(row[15])));
    json_object_set_new(detail, "money_fee", json_string(rstripzero(row[16])));
    json_object_set_new(detail, "stock_fee", json_string(rstripzero(row[17])));
    json_object_set_new(detail, "fee_asset", json_string(row[18]));
    json_object_set_new(detail, "fee_discount", json_string(rstripzero(row[19])));
    json_object_set_new(detail, "asset_fee", json_string(rstripzero(row[20])));
    json_object_set_new(detail, "client_id", json_string(row[21]));
    mysql_free_result(result);

    return detail;
}

json_t *get_order_deals(MYSQL *conn, uint32_t user_id, int32_t account, uint64_t order_id, size_t offset, size_t limit)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `time`, `user_id`, `account`, `deal_user_id`, `deal_id`, `role`, `price`, `amount`, `deal`, `fee`, `fee_asset`, `deal_order_id` "
            "FROM `user_deal_history_%u` where `user_id` = '%u' AND `order_id` = %"PRIu64"", user_id % HISTORY_HASH_NUM, user_id, order_id);

    if(account >= 0){
        sql = sdscatprintf(sql, " AND `account` = %u", account);
    }

    sql = sdscatprintf(sql, " ORDER BY `time` desc, `id` DESC");

    if (offset) {
        sql = sdscatprintf(sql, " LIMIT %zu, %zu", offset, limit);
    } else {
        sql = sdscatprintf(sql, " LIMIT %zu", limit);
    }

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *records = json_array();
    for (size_t i = 0; i < num_rows; ++i) {
        json_t *record = json_object();
        MYSQL_ROW row = mysql_fetch_row(result);

        double timestamp = strtod(row[0], NULL);
        json_object_set_new(record, "time", json_real(timestamp));
        uint32_t user_id = strtoul(row[1], NULL, 0);
        json_object_set_new(record, "user", json_integer(user_id));
        uint32_t account = strtoul(row[2], NULL, 0);
        json_object_set_new(record, "account", json_integer(account));
        uint32_t deal_user_id = strtoul(row[3], NULL, 0);
        json_object_set_new(record, "deal_user", json_integer(deal_user_id));
        uint64_t deal_id = strtoull(row[4], NULL, 0);
        json_object_set_new(record, "id", json_integer(deal_id));
        int role = atoi(row[5]);
        json_object_set_new(record, "role", json_integer(role));
        json_object_set_new(record, "price", json_string(rstripzero(row[6])));
        json_object_set_new(record, "amount", json_string(rstripzero(row[7])));
        json_object_set_new(record, "deal", json_string(rstripzero(row[8])));
        json_object_set_new(record, "fee", json_string(rstripzero(row[9])));
        json_object_set_new(record, "fee_asset", json_string(row[10]));
        uint64_t deal_order_id = strtoull(row[11], NULL, 0);
        json_object_set_new(record, "deal_order_id", json_integer(deal_order_id));

        json_array_append_new(records, record);
    }
    mysql_free_result(result);

    return records;
}

