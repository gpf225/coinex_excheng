/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# ifndef _HW_JOB_H_
# define _HW_JOB_H_

# include "hw_config.h"
# include "nw_job.h"

struct job_val {
    uint32_t db;
    uint32_t type;
    sds     sql;
};

enum {
    HISTORY_USER_BALANCE,
    HISTORY_USER_ORDER,
    HISTORY_USER_STOP,
    HISTORY_USER_DEAL,
};

typedef struct history_job_t {
    int db;
    nw_job *job;
}history_job_t;

history_job_t *create_history_job(int db);
int submit_job(history_job_t *job, struct job_val *val);
void job_val_free(struct job_val *val);
void job_quit(int flag);

# endif