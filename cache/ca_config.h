/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# ifndef _CA_CONFIG_H_
# define _CA_CONFIG_H_

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
# include "nw_state.h"
# include "nw_timer.h"

# include "ut_log.h"
# include "ut_sds.h"
# include "ut_misc.h"
# include "ut_list.h"
# include "ut_title.h"
# include "ut_signal.h"
# include "ut_config.h"
# include "ut_define.h"
# include "ut_profile.h"
# include "ut_decimal.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_rpc_reply.h"

# define MARKET_NAME_MAX_LEN    16
# define INTERVAL_MAX_LEN       16
# define DEPTH_LIMIT_MAX_LEN    101

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;
    int                 worker_num;
    rpc_clt_cfg         matchengine;
    
    double              backend_timeout;
    double              poll_depth_interval;
    int                 cache_timeout;
};

extern struct settings settings;
int init_config(const char *path);

# endif

