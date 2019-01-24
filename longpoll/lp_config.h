/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# ifndef _LP_CONFIG_H_
# define _LP_CONFIG_H_

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
# include <assert.h>

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
# include "ut_define.h"
# include "ut_signal.h"
# include "ut_config.h"
# include "ut_profile.h"
# include "ut_decimal.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_rpc_reply.h"
# include "ut_comm_dict.h"

# define MARKET_NAME_MAX_LEN    16
# define INTERVAL_MAX_LEN       16

struct settings{
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;

    rpc_clt_cfg         matchengine;
    rpc_clt_cfg         marketprice;

    double poll_depth_interval;
    double poll_state_interval;
    double poll_market_interval;
    double statistic_interval;
    double backend_timeout;
    bool debug;
};

extern struct settings settings;

int init_config(const char *path);

# endif