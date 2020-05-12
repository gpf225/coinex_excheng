/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/16, create
 */

# include <curl/curl.h>

# include <sys/time.h>

# include "ut_mysql.h"
# include "mp_config.h"
# include "mp_history.h"
# include "mp_message.h"

# define MAX_PENDING_HISTORY  1000

static MYSQL *mysql_conn;
static nw_job *job;

static void *on_job_init(void)
{
    return mysql_connect(&settings.db_log);
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    sds sql = entry->request;
    log_trace("exec sql: %s", sql);
    while (true) {
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_fatal("append count: %d, exec sql: %s fail: %d %s", job->request_count, sql, mysql_errno(conn), mysql_error(conn));
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

    nw_job_type type;
    memset(&type, 0, sizeof(type));
    type.on_init    = on_job_init;
    type.on_job     = on_job;
    type.on_cleanup = on_job_cleanup;
    type.on_release = on_job_release;

    job = nw_job_create(&type, 1);
    if (job == NULL)
        return -__LINE__;

    return 0;
}

int fini_history(void)
{
    while (job->request_count != 0) {
        usleep(100 * 1000);
        log_info("job->request_count: %d", job->request_count);
        continue;
    }
    nw_job_release(job);

    return 0;
}

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

int append_kline_history(const char *market, int type, time_t timestamp, struct kline_info *kinfo)
{
    if (job->request_count >= MAX_PENDING_HISTORY) {
        log_fatal("history append too big: %d", job->request_count);
    }

    static char *table_last;
    if (table_last == NULL) {
        table_last = strdup("");
    }

    char table[64];
    time_t now = time(NULL);
    struct tm *tm = localtime(&now);
    snprintf(table, sizeof(table), "kline_history_%04d%02d", 1900 + tm->tm_year, 1 + tm->tm_mon);

    if (strcmp(table_last, table) != 0) {
        sds create_table_sql = sdsempty();
        create_table_sql = sdscatprintf(create_table_sql, "CREATE TABLE IF NOT EXISTS `%s` like `kline_history_example`", table);
        nw_job_add(job, 0, create_table_sql);
        free(table_last);
        table_last = strdup(table);
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `%s` (`market`, `t`, `timestamp`, `open`, `close`, `high`, `low`, `volume`, `deal`) VALUES ", table);
    sql = sdscatprintf(sql, "('%s', %d, %ld, ", market, type, timestamp);
    sql = sql_append_mpd(sql, kinfo->open, true);
    sql = sql_append_mpd(sql, kinfo->close, true);
    sql = sql_append_mpd(sql, kinfo->high, true);
    sql = sql_append_mpd(sql, kinfo->low, true);
    sql = sql_append_mpd(sql, kinfo->volume, true);
    sql = sql_append_mpd(sql, kinfo->deal, false);
    sql = sdscatprintf(sql, ")");

    nw_job_add(job, 0, sql);
    log_trace("add history: %s", sql);
    profile_inc("history", 1);

    return 0;
}
