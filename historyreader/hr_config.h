/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/24, create
 */

# ifndef _HR_CONFIG_H_
# define _HR_CONFIG_H_

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
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_json_rpc.h"

# define QUERY_LIMIT    1001

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;

    mysql_cfg           *db_histories;
    int                 db_history_count;
    int                 worker_num;
};

extern struct settings settings;
int init_config(const char *path);

# endif


#ifdef __APPLE__
extern int error(int status, void* error, char* format,int ret);
#endif

