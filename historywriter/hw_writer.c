/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "hw_config.h"
# include "hw_writer.h"
# include "hw_message.h"

# define PENDING_JOB_MAX 1000

static bool message_suspended;
static nw_job *job_context;
static nw_timer timer;

struct job_request {
    int db;
    sds sql;
};

static void *on_job_init(void)
{
    MYSQL **conn_array = calloc(sizeof(void *), settings.db_history_count);
    for (int i = 0; i < settings.db_history_count; ++i) {
        conn_array[i] = mysql_connect(&settings.db_histories[i]);
        if (conn_array[i] == NULL)
            return NULL;
    }

    return conn_array;
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    struct job_request *req = entry->request;
    if (req->db >= settings.db_history_count)
        return;
    MYSQL **conn_array = privdata;
    MYSQL *conn = conn_array[req->db];

    log_trace("db: %d exec sql: %s", req->db, req->sql);
    profile_inc("sql_exec", 1);
    while (true) {
        int ret = mysql_real_query(conn, req->sql, sdslen(req->sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_error("exec sql: %s fail: %d %s", req->sql, mysql_errno(conn), mysql_error(conn));
            usleep(1000 * 1000);
            continue;
        }
        break;
    }
}

static void on_job_cleanup(nw_job_entry *entry)
{
    struct job_request *req = entry->request;
    sdsfree(req->sql);
    free(req);
}

static void on_job_release(void *privdata)
{
    MYSQL **conn_array = privdata;
    for (int i = 0; i < settings.db_history_count; ++i) {
        mysql_close(conn_array[i]);
    }
}

static void on_timer(nw_timer *timer, void *privdata)
{
    profile_set("sql_pending", job_context->request_count);
    if (!message_suspended) {
        if (job_context->request_count >= PENDING_JOB_MAX) {
            suspend_message();
            message_suspended = true;
        }
    } else {
        if (job_context->request_count == 0) {
            resume_message();
            message_suspended = false;
        }
    }
}

int init_writer(void)
{
    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_init    = on_job_init;
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;
    jt.on_release = on_job_release;

    job_context = nw_job_create(&jt, settings.worker_num);
    if (job_context == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.5, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

void fini_writer(void)
{
    while (job_context->request_count != 0) {
        usleep(100 * 1000);
        continue;
    }

    nw_job_release(job_context);
}

int submit_job(int db, sds sql)
{
    struct job_request *req = malloc(sizeof(struct job_request));
    req->db = db;
    req->sql = sql;
    return nw_job_add(job_context, 0, req);
}

sds writer_status(sds reply)
{
    reply = sdscatprintf(reply, "pending job: %d\n", job_context->request_count);
    return reply;
}

