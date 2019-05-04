# ifndef _MP_CONFIG_H_
# define _MP_CONFIG_H_

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

# define MARKET_NAME_MAX_LEN   16
# define STOCK_NAME_MAX_LEN    8


struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;
    cli_svr_cfg         cli;
    mysql_cfg           db_history;
    kafka_consumer_cfg  deals;
    int                 keep_day;
    int                 prec;
    int                 interval_minute;
};

extern struct settings settings;
int init_config(const char *path);

# endif

