/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/19, create
 */

# ifndef _HW_CONFIG_H_
# define _HW_CONFIG_H_

# include <math.h>
# include <stdio.h>
# include <error.h>
# include <errno.h>
# include <ctype.h>
# include <string.h>
# include <stdlib.h>
# include <unistd.h>
# include <assert.h>
# include <inttypes.h>

# include "nw_svr.h"
# include "nw_clt.h"
# include "nw_job.h"
# include "nw_timer.h"
# include "nw_state.h"

# include "ut_log.h"
# include "ut_sds.h"
# include "ut_cli.h"
# include "ut_misc.h"
# include "ut_list.h"
# include "ut_kafka.h"
# include "ut_mysql.h"
# include "ut_signal.h"
# include "ut_config.h"
# include "ut_define.h"
# include "ut_decimal.h"
# include "ut_profile.h"

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    cli_svr_cfg         cli;
    char                *brokers;
    
    kafka_consumer_cfg  deals;
    kafka_consumer_cfg  stops;
    kafka_consumer_cfg  orders;
    kafka_consumer_cfg  balances;
    
    redis_cfg           redis;
    mysql_cfg           *db_histories;
    int                 db_history_count;
    int                 worker_num;
};

extern struct settings settings;
int init_config(const char *path);

# endif
