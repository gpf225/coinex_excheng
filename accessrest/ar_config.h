/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/21, create
 */

# ifndef _AR_CONFIG_H_
# define _AR_CONFIG_H_

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
# include "ut_signal.h"
# include "ut_config.h"
# include "ut_profile.h"
# include "ut_decimal.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_http_svr.h"
# include "ut_rpc_reply.h"

# define MARKET_NAME_MAX_LEN    16
# define INTERVAL_MAX_LEN       16
# define AR_LISTENER_BIND       "seqpacket@/tmp/accessrest_listener.sock"

typedef struct depth_limit_cfg {
    int     count;
    int     *limit;
} depth_limit_cfg;

typedef struct depth_merge_cfg {
    int     count;
    mpd_t   **limit;
} depth_merge_cfg;

struct settings {
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    http_svr_cfg        svr;

    rpc_clt_cfg         matchengine;
    rpc_clt_cfg         marketprice;
    rpc_clt_cfg         cache;
    rpc_clt_cfg         longpoll;

    int                 worker_num;
    int                 cache_worker_num;
    int                 kline_max;
    int                 kline_default;
    int                 deal_default;
    int                 depth_limit_max;
    int                 depth_limit_default;
    double              backend_timeout;
    double              cache_timeout;
    double              state_interval;
    double              market_interval;

    depth_limit_cfg     depth_limit;
    depth_merge_cfg     depth_merge;
    
    char                *market_url;
    bool                debug;
};

extern struct settings settings;
int init_config(const char *path);

# endif

