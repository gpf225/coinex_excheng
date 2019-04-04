/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_dump.h"
# include "hw_append_file.h"

static pthread_mutex_t pending_lock = PTHREAD_MUTEX_INITIALIZER;

static append_file_t *create_uncompleted_file(uint32_t db)
{
    char buf[128] = {0};
    snprintf(buf, sizeof(buf), "data/db%u_uncompleted.sql", db);
    return append_file_create(buf);
}

static append_file_t *create_pending_file(uint32_t db)
{
    char buf[128] = {0};
    snprintf(buf, sizeof(buf), "data/db%u_pending.sql", db);
    return append_file_create(buf);
}

static int dump_uncompleted_sqls(append_file_t *file, dict_t *dict_sql)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_sql);
    while ((entry = dict_next(iter)) != NULL) {
        struct job_val *job = entry->val;
        append_file_append(file, job->sql, sdslen(job->sql));
        append_file_append(file, "\r\n", sizeof("\r\n"));
    }
    return 0;
}

static int dump_uncompleted_jobs(append_file_t *file, nw_job *job)
{
    while (1) {
        nw_job_entry *entry = job->request_head;
        if (entry == NULL) {
            break;
        }
        job->request_head = entry->next;
        if (job->request_head) {
            job->request_head->prev = NULL;
        } else {
            job->request_tail = NULL;
        }
        job->request_count -= 1;
        struct job_val *job = entry->request;
        append_file_append(file, job->sql, sdslen(job->sql));
        append_file_append(file, "\r\n", sizeof("\r\n"));
    }   
    assert(job->request_count == 0);
    return 0;
}

static int dump_db_uncompleted_history(uint32_t db, nw_job *job, dict_t *dict_sql)
{
    if (job->request_count == 0 && dict_size(dict_sql) == 0) {
        log_info("db:%d has no sqls need to dump", db);
        return 0;
    }

    append_file_t *file = create_uncompleted_file(db);
    if (file == NULL) {
        log_error("create dump file failed at db:%u", db);
        return -__LINE__;
    }

    dump_uncompleted_jobs(file, job);
    dump_uncompleted_sqls(file, dict_sql);

    append_file_flush(file);
    append_file_release(file);
    return 0;
}

int dump_uncompleted_history(dict_t **dict_sqls, history_job_t **jobs)
{
    int db_count = settings.db_histories.count;
    for (int i = 0; i < db_count; ++i) {
        log_info("dump db:%d", i);
        history_job_t *his_job = jobs[i];
        dump_db_uncompleted_history(his_job->db, his_job->job, dict_sqls[i]);
    }
    log_info("dump uncompleted history success");
    return 0;
}

int dump_pending_sql(struct job_val *val)
{
    pthread_mutex_lock(&pending_lock);

    append_file_t *file = create_pending_file(val->db);
    if (file == NULL) {
        log_error("create dump file failed.");
        return -__LINE__;
    }

    append_file_append(file, val->sql, sdslen(val->sql));
    append_file_flush(file);
    append_file_release(file);

    pthread_mutex_unlock(&pending_lock);
    log_info("dump db[%d] pending history success", val->db);
    return 0;
}
