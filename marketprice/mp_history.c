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
    return mysql_connect(&settings.db_history);
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
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_history.charset) != 0)
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

static char *get_utc_date_from_time(time_t timestamp, const char *format)
{
    static char str[512];
    struct tm *timeinfo = gmtime(&timestamp);
    strftime(str, sizeof(str), format, timeinfo);
    return str;
}

int append_kline_history(const char *market, int type, time_t timestamp, mpd_t *open, mpd_t *close, mpd_t *high, mpd_t *low, mpd_t *volume, mpd_t *deal)
{
    if (job->request_count >= MAX_PENDING_HISTORY) {
        log_fatal("history append too big: %d", job->request_count);
    }

    static char *table_last;
    if (table_last == NULL) {
        table_last = strdup("");
    }

    time_t now = time(NULL);
    char table[64];
    snprintf(table, sizeof(table), "kline_history_%s", get_utc_date_from_time(now, "%Y%m"));

    if (strcmp(table_last, table) != 0) {
        sds create_table_sql = sdsempty();
        create_table_sql = sdscatprintf(create_table_sql, "CREATE TABLE IF NOT EXISTS `%s` like `kline_history_example`", table);
        nw_job_add(job, 0, create_table_sql);
        free(table_last);
        table_last = strdup(table);
    }

    char buf[512];
    char *open_str = mpd_format(open, "f", &mpd_ctx);
    char *close_str = mpd_format(close, "f", &mpd_ctx);
    char *high_str = mpd_format(high, "f", &mpd_ctx);
    char *low_str = mpd_format(low, "f", &mpd_ctx);
    char *volume_str = mpd_format(volume, "f", &mpd_ctx);
    char *deal_str = mpd_format(deal, "f", &mpd_ctx);

    snprintf(buf, sizeof(buf), "('%s', %d, %ld, '%s', '%s', '%s', '%s', '%s', '%s')", market, type, timestamp, open_str, close_str, high_str, low_str, volume_str, deal_str);
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `%s` (`market`, `t`, `timestamp`, `open`, `close`, `high`, `low`, `volume`, `deal`) VALUES %s", table, buf);
    nw_job_add(job, 0, sql);

    free(open_str);
    free(close_str);
    free(high_str);
    free(low_str);
    free(volume_str);
    free(deal_str);

    log_trace("add history: %s", sql);
    profile_inc("history", 1);

    return 0;
}
