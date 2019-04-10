/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# ifndef _DM_COMMON_H_
# define _DM_COMMON_H_

# include "dm_config.h"

long to_long(const char *str);
double to_double(const char *str);

MYSQL_RES* execute_sql(MYSQL *conn, sds sql);

/*
 * 执行sql COUNT语句。返回一个非负整数表示查询到的结果。
 * 如果查询失败将返回0. */
uint64_t execute_sql_count(MYSQL *conn, sds sql);

# endif