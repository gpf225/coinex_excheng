/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# ifndef _HW_DUMP_H_
# define _HW_DUMP_H_

# include "hw_job.h"

int dump_uncompleted_history(dict_t **dict_sqls, history_job_t **jobs);
int dump_pending_sql(struct job_val *val);

# endif