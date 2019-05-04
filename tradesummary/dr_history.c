/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "dr_config.h"
# include "dr_history.h"
# include "dr_deal.h"
# include "dr_fee_rate.h"

static nw_timer timer;
static MYSQL    *mysql_conn;
static time_t   last_dump_time;

static int create_deal_table(const char *table, MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `statistic_deal_history_example`", table);

    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    return 0;
}

static int create_fee_table(const char *table, MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `statistic_fee_history_example`", table);

    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    return 0;
}

static int create_fee_rate_table(const char *table, MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `statistic_fee_rate_history_example`", table);

    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    return 0;
}

static char *get_deal_table_name(time_t time)
{ 
    static char str[64] = {0};
    struct tm *tm = localtime(&time);
    snprintf(str, sizeof(str), "statistic_deal_history_%04d%02d%02d", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);

    return str;
}

static char *get_fee_table_name(time_t time)
{ 
    static char str[64] = {0};
    struct tm *tm = localtime(&time);
    snprintf(str, sizeof(str), "statistic_fee_history_%04d%02d%02d", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);

    return str;
}

static char *get_fee_rate_table_name(time_t time)
{ 
    static char str[64] = {0};
    struct tm *tm = localtime(&time);
    snprintf(str, sizeof(str), "statistic_fee_rate_history_%04d%02d%02d", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);

    return str;
}

void dump_deals_to_db(list_t *list_deals, const char *market, const char *stock, time_t start)
{
    log_info("dump deals start, market: %s, start: %zd", market, start);
    MYSQL *conn = mysql_connect(&settings.db_history);
    if (conn == NULL) {
        log_fatal("connect mysql fail");
        return;  
    }

    char *table = get_deal_table_name(start);
    int ret = create_deal_table(table, conn);
    if (ret != 0) {
        log_error("create_deal_table fail");
        mysql_close(conn);
        return;    
    }

    size_t index = 0;
    list_node *node;
    sds sql = sdsempty();

    list_iter *iter = list_get_iterator(list_deals, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        struct deals_info *info = (struct deals_info*)node->value;
        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `market`, `stock`, `user_id`, `volume_bid`, `volume_ask`, `deal_bid`, `deal_ask`,  \
                    `volume_taker_bid`, `volume_taker_ask`, `num_taker_bid`, `num_taker_ask`, `num_total`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', '%s', %u, '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d)", 
                market, stock, info->user_id, info->volume_bid, info->volume_ask, info->deal_bid, info->deal_ask, 
                info->volume_taker_bid, info->volume_taker_ask, info->trade_num_taker_bid, info->trade_num_taker_ask, info->trade_num_total);
        index += 1;
        if (index == 1000) {
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                list_release_iterator(iter);
                sdsfree(sql);
                mysql_close(conn);
                return;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    list_release_iterator(iter);

    if (index > 0) {
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            mysql_close(conn);
            sdsfree(sql);
            return;
        }
    }

    mysql_close(conn);
    sdsfree(sql);
    log_info("dump deals end, market: %s", market);
}

void dump_fee_to_db(dict_t *dict_fee, time_t start)
{
    log_info("dump fee start, start: %zd", start);
    MYSQL *conn = mysql_connect(&settings.db_history);
    if (conn == NULL) {
        log_fatal("connect mysql fail");
        return;  
    }

    char *table = get_fee_table_name(start);
    int ret = create_fee_table(table, conn);
    if (ret != 0) {
        log_error("create_fee_table fail");
        mysql_close(conn);
        return;    
    }

    size_t index = 0;
    sds sql = sdsempty();

    dict_entry *entry;
    dict_iterator *iter_fee = dict_get_iterator(dict_fee);
    while ((entry = dict_next(iter_fee)) != NULL) {
        char *fee_asset = entry->key;
        char *fee_str = entry->val;

        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `fee_asset`, `fee`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', '%s')", fee_asset, fee_str);
        index += 1;
        if (index == 1000) {
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter_fee);
                sdsfree(sql);
                mysql_close(conn);
                return;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    dict_release_iterator(iter_fee);

    if (index > 0) {
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            mysql_close(conn);
            sdsfree(sql);
            return;
        }
    }

    mysql_close(conn);
    sdsfree(sql);
    log_info("dump fee end, market");
}

static sds sql_append_mpd(sds sql, int count, ...)
{
    static char buf[2048] = {0};
    size_t len = 0;
    va_list ap;
    va_start(ap, count);
    for(int i = 0; i < count; i++)
    {
        if (len >= sizeof(buf)) {
            log_fatal("sql_append_mpd fail, len: %zd", len);
            break;
        }

        char *str;
        mpd_t *mpd_val = va_arg(ap, mpd_t*);
        if (mpd_cmp(mpd_val, mpd_zero, &mpd_ctx) == 0) {
            str = mpd_to_sci(mpd_zero, 0);
        } else {
            str = mpd_to_sci(mpd_val, 0);
        }

        if (i < count - 1) {
            len += snprintf(buf + len, sizeof(buf) - len, "'%s', ", str);
        } else {
            len += snprintf(buf + len, sizeof(buf) - len, "'%s')", str);
        }

        free(str);
    }
    va_end(ap);

    sql = sdscat(sql, buf);
    return sql;
}

static void dump_fee_rate_to_db(dict_t *dict_fee_rate, time_t start)
{
    log_info("dump fee_rate start, start: %zd", start);
    MYSQL *conn = mysql_connect(&settings.db_history);
    if (conn == NULL) {
        log_fatal("connect mysql fail");
        return;  
    }

    char *table = get_fee_rate_table_name(start);
    int ret = create_fee_rate_table(table, conn);
    if (ret != 0) {
        log_error("create_fee_rate_table fail");
        mysql_close(conn);
        return;    
    }

    size_t index = 0;
    sds sql = sdsempty();

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_fee_rate);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_fee_rate_key *fee_rate_key = entry->key;
        struct dict_fee_rate_val *fee_rate_val = entry->val;

        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `market`, `stock`, `gear0`, `gear1`, `gear2`, `gear3`, `gear4`, `gear5`,  \
                    `gear6`, `gear7`, `gear8`, `gear9`, `gear10`, `gear11`, `gear12`, `gear13`, `gear14`, `gear15`, `gear16`, `gear17`, \
                    `gear18`, `gear19`, `gear20`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, '%s', '%s', ",  fee_rate_key->market, fee_rate_key->stock);
        sql = sql_append_mpd(sql, 21, fee_rate_val->volume_gear[0], fee_rate_val->volume_gear[1], fee_rate_val->volume_gear[2], fee_rate_val->volume_gear[3], \
                fee_rate_val->volume_gear[4], fee_rate_val->volume_gear[5], fee_rate_val->volume_gear[6], fee_rate_val->volume_gear[7], fee_rate_val->volume_gear[8], \
                fee_rate_val->volume_gear[9], fee_rate_val->volume_gear[10], fee_rate_val->volume_gear[11], fee_rate_val->volume_gear[12], fee_rate_val->volume_gear[13], \
                fee_rate_val->volume_gear[14], fee_rate_val->volume_gear[15], fee_rate_val->volume_gear[16], fee_rate_val->volume_gear[17], fee_rate_val->volume_gear[18], \
                fee_rate_val->volume_gear[18], fee_rate_val->volume_gear[20]);

        index += 1;
        if (index == 1000) {
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret != 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter); 
                sdsfree(sql);
                mysql_close(conn);      
                return;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    dict_release_iterator(iter); 

    if (index > 0) {
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            mysql_close(conn);
            sdsfree(sql); 
            return;
        }
    }

    mysql_close(conn);
    sdsfree(sql);
    log_info("dump fee_rate end");
}

static int dump_to_history(time_t start)
{
    dict_t *dict_fee_rate = get_fee_rate_dict();


    dlog_flush_all();
    int pid = fork();
    if (pid < 0) {
        log_fatal("fork fail: %d", pid);
        return -__LINE__;
    } else if (pid > 0) {
        dict_clear(dict_fee_rate);
        clear_fee_dict();
        return 0;
    }

    log_info("dump_fee_rate_to_db start: %zd", start);
    dump_fee_rate_to_db(dict_fee_rate, start);
    log_info("dump_fee_rate_to_db end");

    log_info("dump_deal_and_fee start: %zd", start);
    int ret = dump_deal_and_fee(start);
    if (ret !=0 ) {
        log_error("get_one_day_deals fail");
        exit(0);
        return 0;  
    }
    log_info("dump_deal_and_fee end");

    return 0;
}

static void on_timer(nw_timer *t, void *privdata)
{
    time_t d_now = time(NULL);
    time_t d_start = get_day_start(d_now - 20);

    if ((d_now - d_start) >= 86400 && (d_now - last_dump_time) >= 60) {
        dump_to_history(d_start);
        last_dump_time = d_now;
    }
}

int fini_history(void)
{
    on_timer(NULL, NULL);

    return 0;
}

int init_history(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_history.charset) != 0)
        return -__LINE__;

    nw_timer_set(&timer, 3, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

