/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# ifndef _ME_CONFIG_H_
# define _ME_CONFIG_H_

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

# include "ut_log.h"
# include "ut_sds.h"
# include "ut_title.h"
# include "ut_cli.h"
# include "ut_misc.h"
# include "ut_list.h"
# include "ut_mysql.h"
# include "ut_signal.h"
# include "ut_define.h"
# include "ut_config.h"
# include "ut_profile.h"
# include "ut_decimal.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_skiplist.h"
# include "ut_json_rpc.h"
# include "ut_comm_dict.h"

# define ORDER_BOOK_MAX_LEN     101
# define ORDER_LIST_MAX_LEN     101

# define MAX_PENDING_OPERLOG    1000
# define MAX_PENDING_MESSAGE    10000
# define MAX_PENDING_HISTORY    100000

# define QUEUE_MEM_SIZE         500000
# define QUEUE_MEM_MIN          100000
# define QUEUE_SHMKEY_START     0x16120802
# define QUEUE_NAME             "matchengine_queue"
# define QUEUE_PIPE_PATH        "/tmp/matchengine_queue_pipe"

# define SYSTEM_FEE_TOKEN       "CET"

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;
    cli_svr_cfg         cli;
    mysql_cfg           db_log;

    char                *asset_url;
    json_t              *asset_cfg;

    char                *market_url;
    json_t              *market_cfg;

    char                *brokers;
    int                 slice_interval;
    int                 slice_keeptime;
    int                 depth_merge_max;
    int                 min_save_prec;
    int                 discount_prec;

    int                 fee_prec;
    int                 reader_num;
    double              call_auction_calc_interval;
    double              cache_timeout;
    double              worker_timeout;
    double              order_fini_keeptime;

    dict_t              *convert_fee_dict;
};

struct convert_fee {
    char                *money;
    mpd_t               *price;
};

extern struct settings settings;

int init_config(const char *path);

int update_asset_config(void);
int update_market_config(void);

# endif

