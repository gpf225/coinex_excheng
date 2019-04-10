/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# include "dm_balance.h"
# include "dm_common.h"
# include "dm_dbpool.h"

static int insert_into_new_db(uint32_t user_id, MYSQL_RES *result, size_t num_rows, long *last_id)
{
    const uint32_t hash = user_id % HISTORY_HASH_NUM;
    sds sql = sdsempty();
    MYSQL *new_conn = get_new_db_connection(user_id);

    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);

        if (sdslen(sql) == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `balance_history_%u` (`time`, `user_id`, `asset`, `business`, `change`, `balance`, `detail`) VALUES ", hash);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "('%s', '%s', '%s', '%s', '%s', '%s'", row[1], row[2], row[3], row[4], row[5], row[6]);
        char buf[10 * 1024] = {0};
        mysql_real_escape_string(new_conn, buf, row[7], strlen(row[7]));
        sql = sdscatprintf(sql, ", '%s')", buf);
        if (i == num_rows - 1) {
            *last_id = to_long(row[0]);
        }
    }

    int ret = mysql_real_query(new_conn, sql, sdslen(sql));
    sdsfree(sql);
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s, ret:%d", sql, mysql_errno(new_conn), mysql_error(new_conn), ret);
        return -1;
    }
    return 0;
}

int balance_migrate(uint32_t user_id, double migrate_start_time, double migrate_end_time)
{
    double now = current_timestamp();
    log_info("balance migrate start, user_id:%u", user_id);
    MYSQL *conn = get_old_db_connection();

    uint32_t total = 0;
    long last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `time`, `user_id`, `asset`, `business`, `change`, `balance`, `detail` "
            "FROM `balance_history_%u` where `user_id` = %u AND `id` > '%ld' AND `time` <= '%f' AND `time` > '%f' ORDER BY `id` ASC LIMIT %d",
            user_id % HISTORY_HASH_NUM, user_id, last_id, migrate_start_time, migrate_end_time, QUERY_LIMIT);

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
        
        log_info("balance migrating, user_id:%u total:%u", user_id, total);
        ret = insert_into_new_db(user_id, result, num_rows, &last_id);
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

    log_info("balance migrate completed, user_id:%u, total:%u cost:%f", user_id, total, current_timestamp() - now);
    return 0;
}
