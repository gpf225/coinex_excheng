/*
 * Description: 
 *     History: ouxiangyang@viabtc.com, 2018/10/15, create
 */

# include "mi_config.h"
# include "mi_index.h"
# include "mi_history.h"

static MYSQL *mysql_conn;
static nw_job *job_context;

static void *on_job_init(void)
{
    return mysql_connect(&settings.db_log);
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    sds sql = entry->request;
    while (true) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            usleep(1000 * 1000);
            continue;
        }
        break;
    }
}

static void on_job_cleanup(nw_job_entry *entry)
{
    sdsfree(entry->request);
}

static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

int init_history(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_log.charset) != 0)
        return -__LINE__;

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_init    = on_job_init;
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;
    jt.on_release = on_job_release;

    job_context = nw_job_create(&jt, 1);
    if (job_context == NULL)
        return -__LINE__;

    return 0;
}

int fini_history(void)
{
    nw_job_release(job_context);
    return 0;
}

sds history_status(sds reply)
{
    return sdscatprintf(reply, "history pending %d\n", job_context->request_count);
}

static sds sql_append_mpd(sds sql, const mpd_t *val, bool comma)
{
    char *str = mpd_to_sci(val, 0);
    sql = sdscatprintf(sql, "'%s'", str);
    if (comma) {
        sql = sdscatprintf(sql, ", ");
    }
    free(str);
    return sql;
}

int append_index_history(const char *market, const mpd_t *price, const char *detail)
{
    static sds table_last;
    if (table_last == NULL) {
        table_last = sdsempty();
    }

    double now = current_timestamp();
    time_t now_sec = (time_t)now;
    struct tm *tm = localtime(&now_sec);
    sds table = sdsempty();
    table = sdscatprintf(table, "indexlog_%04d%02d%02d", 1900 + tm->tm_year, 1 + tm->tm_mon, tm->tm_mday);

    if (sdscmp(table_last, table) != 0) {
        sds create_table_sql = sdsempty();
        create_table_sql = sdscatprintf(create_table_sql, "CREATE TABLE IF NOT EXISTS `%s` like `indexlog_example`", table);
        nw_job_add(job_context, 0, create_table_sql);
        table_last = sdscpy(table_last, table);
    }
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `time`, `market`, `price`, `detail`) VALUES ", table);
    sdsfree(table);

    sql = sdscatprintf(sql, "(NULL, %f, '%s', ", now, market);
    sql = sql_append_mpd(sql, price, true);

    uint32_t detail_len = strlen(detail);
    char buf[detail_len * 2 + 1];
    mysql_real_escape_string(mysql_conn, buf, detail, detail_len);
    sql = sdscatprintf(sql, "'%s')", buf);

    nw_job_add(job_context, 0, sql);
    profile_inc("flush_indexlog", 1);

    return 0;
}

