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
# include "ut_comm_dict.h"
# include "ut_json_rpc.h"

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
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    http_svr_cfg        svr;
    int                 deal_max;
    int                 kline_max;

    rpc_clt_cfg         marketprice;
    rpc_clt_cfg         marketindex;

    rpc_clt_cfg         cache_deals;
    rpc_clt_cfg         cache_state;

    char               *cachecenter_host;
    int                 cachecenter_port;
    int                 cachecenter_worker_num;

    int                 worker_num;
    double              backend_timeout;
    double              cache_timeout;
    double              market_interval;
    char                *market_url;
    depth_limit_cfg     depth_limit;
    depth_merge_cfg     depth_merge;
};

extern struct settings settings;
int init_config(const char *path);

# endif

