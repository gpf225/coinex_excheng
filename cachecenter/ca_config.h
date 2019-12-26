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
# include "ut_comm_dict.h"
# include "ut_json_rpc.h"

typedef struct depth_merge_cfg {
    int    count;
    char   **interval;
} depth_interval_cfg;

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;
    rpc_svr_cfg         deals_svr;
    rpc_svr_cfg         state_svr;
    rpc_clt_cfg         matchengine;
    rpc_clt_cfg         marketprice;
    rpc_clt_cfg         marketindex;
    
    double              backend_timeout;
    double              market_interval;
    double              index_interval;
    depth_interval_cfg  depth_interval;

    double              interval_time;
    double              deals_interval;
    double              status_interval;
    double              depth_interval_time;
    int                 depth_limit_max;
    int                 deal_max;
    int                 worker_num;
};

extern struct settings settings;
int init_config(const char *path);

# endif

