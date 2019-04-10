/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# ifndef _DM_CONFIG_H_
# define _DM_CONFIG_H_

# include "ut_config.h"
# include "ut_misc.h"
# include "ut_signal.h"
# include "ut_log.h"
# include "ut_profile.h"
# include "ut_title.h"
# include "ut_mysql.h"

# include <unistd.h>

# define QUERY_LIMIT          1000
# define HISTORY_HASH_NUM     100
# define MIGRATE_MODE_FULL    1     
# define MIGRATE_MODE_PART    2    

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
    
    mysql_cfg           db_user;
    mysql_cfg           db_history;
    ut_mysql_slice      db_histories;
    
    int                 least_day_per_user;
    int                 max_order_per_user;
    int                 migrate_mode;  // 为1表示全量迁移，2表示增量迁移，全量迁移会采用数据过滤规则，增量迁移会迁移指定时间段内的所有数据
    uint32_t            last_user_id;
    double              migirate_start_time;
    double              migirate_end_time;
};

extern struct settings settings;
int init_config(const char *path);

# endif