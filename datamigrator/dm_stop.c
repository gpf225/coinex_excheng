/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# include "dm_stop.h"
# include "dm_common.h"
# include "dm_dbpool.h"

static int insert_into_new_db(uint32_t user_id, MYSQL_RES *result, size_t num_rows, long *last_order_id)
{
    const uint32_t hash = user_id % HISTORY_HASH_NUM;
    sds sql = sdsempty();
    MYSQL *new_conn = get_new_db_connection(user_id);
    long last_id = 0;

    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);

        if (sdslen(sql) == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `stop_history_%u` (`order_id`, `create_time`, `finish_time`, `user_id`, `market`, `source`, "
                    "`t`, `side`, `stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`, `fee_asset`, `fee_discount`, `status`) VALUES ", hash);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15]);
        if (i == num_rows - 1) {
            last_id = to_long(row[0]);
        }
    }

    int ret = mysql_real_query(new_conn, sql, sdslen(sql));
    sdsfree(sql);
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s, ret:%d", sql, mysql_errno(new_conn), mysql_error(new_conn), ret);
        return -1;
    }
    *last_order_id = last_id;
    return 0;
}

int stop_migrate(uint32_t user_id, double migrate_start_time, double migrate_end_time)
{
    double now = current_timestamp();
    log_info("stop migrate start, user_id:%u", user_id);
    MYSQL *conn = get_old_db_connection();

    uint32_t total = 0;
    long last_order_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `create_time`, `finish_time`, `user_id`, `market`, `source`, `t`, `side`, "
            "`stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`, `fee_asset`, `fee_discount`, `status`"
            "FROM `stop_history_%u` WHERE `user_id` = %u AND `id` > '%ld' AND `create_time` <= '%f' AND `create_time` > '%f' ORDER BY `id` ASC LIMIT %d",
             user_id % HISTORY_HASH_NUM, user_id, last_order_id, migrate_start_time, migrate_end_time, QUERY_LIMIT);

        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);
        
        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        if (num_rows == 0) {
            mysql_free_result(result);
            break;
        }
        
        log_info("stop migrating, user_id:%u total:%u", user_id, total);
        ret = insert_into_new_db(user_id, result, num_rows, &last_order_id);
        if (ret != 0) {
            mysql_free_result(result);
            return ret;
        }
        mysql_free_result(result);

        total += num_rows;
        if (num_rows < QUERY_LIMIT) {
            break;
        }
    }

    log_info("stop migrate completed, user_id:%u, total:%u cost:%f", user_id, total, current_timestamp() - now);
    return 0;
}
