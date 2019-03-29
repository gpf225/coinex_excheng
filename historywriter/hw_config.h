/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/19, create
 */

# ifndef _HW_CONFIG_H_
# define _HW_CONFIG_H_

# include "ut_config.h"
# include "ut_misc.h"
# include "ut_signal.h"
# include "ut_log.h"
# include "ut_profile.h"
# include "ut_title.h"

# include <unistd.h>

# define MAX_PENDING_HISTORY    50000

typedef struct ut_mysql_slice {
    uint32_t count;
    mysql_cfg *configs;
}ut_mysql_slice;

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    cli_svr_cfg         cli;

    kafka_consumer_cfg  orders;
    kafka_consumer_cfg  stops;
    kafka_consumer_cfg  deals;
    kafka_consumer_cfg  balances;
    
    redis_cfg           redis;
    ut_mysql_slice      db_histories;

    int                 worker_per_db;
    double              flush_his_interval;
};

extern struct settings settings;
int init_config(const char *path);

# endif