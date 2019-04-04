/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# include "dm_common.h"

long to_long(const char *str)
{
    if (str == NULL) {
        return 0;
    }
    return strtol(str, NULL, 10);
}

double to_double(const char *str)
{
    if (str == NULL) {
        return 0.0;
    }
    return strtod(str, NULL);
}

MYSQL_RES* execute_sql(MYSQL *conn, sds sql)
{
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("execute_sql failed, sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        return NULL;
    }

    MYSQL_RES *result = mysql_store_result(conn);
    if (result == NULL) {
        log_error("sql execute success, but get result failed, this is a very serious error!!!");
        return NULL;
    }

    return result;
}

uint64_t execute_sql_count(MYSQL *conn, sds sql)
{
    MYSQL_RES *result = execute_sql(conn, sql);
    if (result == NULL) {
        return 0;
    }

    size_t num_rows = mysql_num_rows(result);
    if (num_rows != 1) {
        log_error("invalid num_rows:%lu, at sql:%s", num_rows, sql);
        mysql_free_result(result);
        return 0;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    long total = to_long(row[0]);
    mysql_free_result(result);
    
    return total;
}