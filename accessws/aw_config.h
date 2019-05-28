/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/21, create
 */

# ifndef _AW_CONFIG_H_
# define _AW_CONFIG_H_

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
# include "ut_define.h"
# include "ut_signal.h"
# include "ut_config.h"
# include "ut_profile.h"
# include "ut_decimal.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_ws_svr.h"
# include "ut_comm_dict.h"

# define ASSET_NAME_MAX_LEN     16
# define MARKET_NAME_MAX_LEN    16
# define SOURCE_MAX_LEN         32
# define INTERVAL_MAX_LEN       16

# define AW_LISTENER_BIND   "seqpacket@/tmp/accessws_listener.sock"

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
    ws_svr_cfg          svr;

    rpc_clt_cfg         matchengine;
    rpc_clt_cfg         marketprice;
    rpc_clt_cfg         readhistory;

    rpc_clt_cfg         cache_deals;
    rpc_clt_cfg         cache_state;

    char               *cachecenter_host;
    int                 cachecenter_port;
    int                 cachecenter_worker_num;

    kafka_consumer_cfg  deals;
    kafka_consumer_cfg  stops;
    kafka_consumer_cfg  orders;
    kafka_consumer_cfg  indexs;
    kafka_consumer_cfg  balances;

    int                 worker_num;
    int                 depth_limit_default;
    char                *auth_url;
    char                *auth_sub_url;
    char                *sign_url;
    char                *accesshttp;
    double              backend_timeout;
    double              kline_interval;

    depth_limit_cfg     depth_limit;
    depth_merge_cfg     depth_merge;
    int                 deal_max;
};

extern struct settings settings;
int init_config(const char *path);

# endif

