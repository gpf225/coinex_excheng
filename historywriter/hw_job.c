/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_job.h"
# include "hw_dump.h"
# include "ut_mysql.h"

# include "unistd.h"

static int going_to_quit = 0;

# define ON_CONNECT_MYSQL(n)                                             \
    MYSQL *mysql =  mysql_connect(&settings.db_histories.configs[n]);    \
    if (mysql == NULL) {                                                 \
        log_fatal("could not connect db %d", n);                         \
        abort();                                                         \
    }                                                                    \
    return mysql;

typedef void *(*on_init_callback)(void);

static void *on_job_init_0(void)
{
    ON_CONNECT_MYSQL(0)
}

static void *on_job_init_1(void)
{
    ON_CONNECT_MYSQL(1)
}

static void *on_job_init_2(void)
{
    ON_CONNECT_MYSQL(2)
    return mysql;
}

static void *on_job_init_3(void)
{
    ON_CONNECT_MYSQL(3)
}

static void *on_job_init_4(void)
{
    ON_CONNECT_MYSQL(4)
}

static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

static on_init_callback get_init_callback(int db)
{
    switch (db) {
        case 0:
            return on_job_init_0;
        case 1:
            return on_job_init_1;
        case 2:
            return on_job_init_2;
        case 3:
            return on_job_init_3;
        case 4:
            return on_job_init_4;
        default:
            log_error("invalid db:%d", db);
            abort();
    }
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    struct job_val *val = entry->request;
    log_trace("db:%d type:%d exec sql: %s", val->db, val->type, val->sql);
    while (true) {
        int ret = mysql_real_query(conn, val->sql, sdslen(val->sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_fatal("exec sql: %s fail: %d %s", val->sql, mysql_errno(conn), mysql_error(conn));
            usleep(1000 * 1000);

            if (going_to_quit) {
                log_info("going to quit, save the uncompleted sql.");
                dump_pending_sql(val);
                break;
            }
            continue;
        }
        break;
    }
}

static void on_job_cleanup(nw_job_entry *entry)
{
    job_val_free(entry->request);
}

history_job_t *create_history_job(int db)
{
    if (db < 0 || db > 4) {
        log_error("invalid db, db should bewteen [0, 4]");
        return NULL;
    }

    history_job_t *his_job = malloc(sizeof(history_job_t));
    if (his_job == NULL) {
        return NULL;
    }
    memset(his_job, 0, sizeof(history_job_t));
    his_job->db = db;

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_init    = get_init_callback(db);
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;
    jt.on_release = on_job_release;

    his_job->job = nw_job_create(&jt, settings.worker_per_db);
    if (his_job->job == NULL) {
        free(his_job);
        return NULL;
    }

    return his_job;
}

int submit_job(history_job_t *job, struct job_val *val)
{
    assert(job->db == val->db);
    nw_job_add(job->job, 0, val);
    return 0;
}

void job_val_free(struct job_val *val)
{
    if (val->sql) {
        sdsfree(val->sql);
    }
    free(val);
}

void job_quit(int flag)
{
    log_info("going to quit job");
    going_to_quit = flag;
}