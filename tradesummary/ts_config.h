# ifndef _TS_CONFIG_H_
# define _TS_CONFIG_H_

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
# include "nw_state.h"
# include "nw_timer.h"

# include "ut_log.h"
# include "ut_sds.h"
# include "ut_cli.h"
# include "ut_misc.h"
# include "ut_list.h"
# include "ut_kafka.h"
# include "ut_mysql.h"
# include "ut_title.h"
# include "ut_signal.h"
# include "ut_config.h"
# include "ut_define.h"
# include "ut_profile.h"
# include "ut_decimal.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_skiplist.h"
# include "ut_comm_dict.h"
# include "ut_json_rpc.h"

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;
    mysql_cfg           db_summary;
    redis_cfg           redis;
    int                 keep_days;
    char                *accesshttp;
    char                *brokers;
    char                **client_ids;
    size_t              client_id_count;
};

extern struct settings settings;
int init_config(const char *path);

# endif


#ifdef __APPLE__
extern int error(int status, int error, char* format, ...);
#endif