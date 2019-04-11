/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "dr_config.h"
# include "dr_load.h"
# include "dr_message.h"

int load_operlog(MYSQL *conn)
{
	log_info("load_operlog start");
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    int total = 0;

    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `market`, `stock`, `ask_user_id`, `bid_user_id`, `taker_user_id`, `timestamp`, \
        	`amount`, `price`, `ask_fee_asset`, `bid_fee_asset`, `ask_fee`, `bid_fee`, `ask_fee_rate`, `bid_fee_rate` FROM `statistic_operlog` \
            WHERE `id` > %"PRIu64" ORDER BY id LIMIT %zu", last_id, query_limit);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        total += num_rows;

        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            char *market = row[1];
            char *stock = row[2];
            uint32_t ask_user_id = strtoul(row[3], NULL, 0);
            uint32_t bid_user_id = strtoul(row[4], NULL, 0);
            uint32_t taker_user_id = strtoul(row[5], NULL, 0);
            double  timestamp = strtod(row[6], NULL);
            char *amount = row[7];
            char *price = row[8];
            char *ask_fee_asset = row[9];
            char *bid_fee_asset = row[10];
            char *ask_fee = row[11];
            char *bid_fee = row[12];
            char *ask_fee_rate = row[13];
            char *bid_fee_rate = row[14];

            store_message(ask_user_id, bid_user_id, taker_user_id, timestamp, market, stock, amount, price, ask_fee_asset, bid_fee_asset, ask_fee, bid_fee, ask_fee_rate, bid_fee_rate);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

	log_info("load_operlog end, total: %d", total);

    return 0;
}

int init_load_db(void)
{
 	MYSQL *mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_history.charset) != 0)
        return -__LINE__;

    MYSQL *conn = mysql_connect(&settings.db_history);
    if (conn == NULL) {
        log_fatal("connect mysql fail");
        return -__LINE__;
    }

    if (load_operlog(conn) != 0) {
    	mysql_close(conn);
        return -__LINE__;
    }

    mysql_close(conn);
    return 0;
}

