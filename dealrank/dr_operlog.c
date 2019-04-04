/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "dr_config.h"
# include "dr_operlog.h"


static MYSQL *mysql_conn;
static nw_job *job;
static dict_t *dict_sql;
static nw_timer timer;

#define SQL_KEY_INSERT      1
#define SQL_KEY_DEL_TABLE   2
#define MAX_SQL_LEN         10240

struct dict_sql_key {
    uint32_t key;
};

struct dict_sql_val {
    sds      sql;
    char     type; 
};

static uint32_t dict_sql_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_sql_key));
}

static void *dict_sql_key_dup(const void *key)
{
    struct dict_sql_key *obj = malloc(sizeof(struct dict_sql_key));
    memcpy(obj, key, sizeof(struct dict_sql_key));
    return obj;
}

static int dict_sql_key_compare(const void *key1, const void *key2)
{
    const struct dict_sql_key *sql_key1 = key1;
    const struct dict_sql_key *sql_key2 = key2;

    return sql_key1->key - sql_key2->key;
}

static void dict_sql_key_free(void *key)
{
    free(key);
}

static void dict_sql_val_free(struct dict_sql_val *val)
{
    if (val->sql) {
        sdsfree(val->sql);
    }
    free(val);
}

static void *on_job_init(void)
{
    return mysql_connect(&settings.db_history);
}

static void delete_operlog_table(MYSQL *conn)
{
    char sql[64] = "truncate table statistic_operlog";
    int ret = mysql_real_query(conn, sql, strlen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
    }

    return;
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    struct dict_sql_val *val = entry->request;

    if (sdslen(val->sql) == 0) {
        delete_operlog_table(conn);
        return;
    }

    while (true) {
        int ret = mysql_real_query(conn, val->sql, sdslen(val->sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_fatal("exec sql: %s fail: %d %s", val->sql, mysql_errno(conn), mysql_error(conn));
            usleep(1000 * 1000);
            continue;
        }
        break;
    }
}

static void on_job_cleanup(nw_job_entry *entry)
{
    dict_sql_val_free(entry->request);
}

static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

static void flush_operlog_to_db(void)
{
    dict_iterator *iter = dict_get_iterator(dict_sql);
    dict_entry *entry;

    while ((entry = dict_next(iter)) != NULL) {
        nw_job_add(job, 0, entry->val);
        dict_delete(dict_sql, entry->key);
    }
    dict_release_iterator(iter);
}

static void on_timer(nw_timer *t, void *privdata)
{
    flush_operlog_to_db();   
}

static struct dict_sql_val *get_sql(struct dict_sql_key *key)
{
    dict_entry *entry = dict_find(dict_sql, key);
    if (!entry) {
        struct dict_sql_val *val = (struct dict_sql_val *)malloc(sizeof(struct dict_sql_val));
        memset(val, 0, sizeof(struct dict_sql_val));

        val->sql = sdsempty();
        entry = dict_add(dict_sql, key, val);
        if (entry == NULL) {
            dict_sql_val_free(val);
            return NULL;
        }
    }
    return entry->val;
}

sds history_status(sds reply)
{
    return sdscatprintf(reply, "history pending %d\n", job->request_count);
}

int append_deal_operlog(bool del_operlog, uint32_t ask_user_id, uint32_t bid_user_id, uint32_t taker_user_id, double timestamp, const char *market, const char *stock, const char *amount, 
        const char *price, const char *ask_fee_asset, const char *bid_fee_asset, const char *ask_fee, const char *bid_fee, const char *ask_fee_rate, const char *bid_fee_rate)
{
    if (del_operlog) {
        flush_operlog_to_db();

        //delte table
        struct dict_sql_val *val = (struct dict_sql_val *)malloc(sizeof(struct dict_sql_val));
        memset(val, 0, sizeof(struct dict_sql_val));
        val->sql = sdsempty();

        struct dict_sql_key sql_key;
        sql_key.key = SQL_KEY_DEL_TABLE;
        dict_add(dict_sql, &sql_key, val);
        flush_operlog_to_db();
    }

    struct dict_sql_key sql_key;
    sql_key.key = SQL_KEY_INSERT;
    struct dict_sql_val *val = get_sql(&sql_key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) >= MAX_SQL_LEN) {
        flush_operlog_to_db();
        val = get_sql(&sql_key);
        sql = val->sql;
    }

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `statistic_operlog` (`id`, `market`, `stock`, `ask_user_id`, `bid_user_id`, `taker_user_id`, "
                "`timestamp`, `amount`, `price`, `ask_fee_asset`, `bid_fee_asset`, `ask_fee`, `bid_fee`, `ask_fee_rate`, `bid_fee_rate`) VALUES ");
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(NULL, '%s', '%s', %u, %u, %u, %f, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", market, stock, ask_user_id, \
            bid_user_id, taker_user_id, timestamp, amount, price, ask_fee_asset, bid_fee_asset, ask_fee, bid_fee, ask_fee_rate, bid_fee_rate);

    val->sql = sql;
    return 0;
}

int init_operlog(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_history.charset) != 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_sql_hash_function;
    dt.key_compare    = dict_sql_key_compare;
    dt.key_dup        = dict_sql_key_dup;
    dt.key_destructor = dict_sql_key_free;

    dict_sql = dict_create(&dt, 2);
    if (dict_sql == 0) {
        return -__LINE__;
    }

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_init    = on_job_init;
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;
    jt.on_release = on_job_release;

    job = nw_job_create(&jt, 1);
    if (job == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.5, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int fini_operlog(void)
{
    on_timer(NULL, NULL);

    usleep(100 * 1000);
    nw_job_release(job);

    return 0;
}



