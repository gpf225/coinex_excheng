# ifndef MI_CONFIG_H
# define MI_CONFIG_H

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
# include "ut_list.h"
# include "ut_mysql.h"
# include "ut_misc.h"
# include "ut_signal.h"
# include "ut_define.h"
# include "ut_config.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"
# include "ut_http_svr.h"
# include "ut_profile.h"
# include "ut_comm_dict.h"
# include "ut_json_rpc.h"

# define REQUEST_THREAD_COUNT   100

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;
    mysql_cfg           db_log;

    char                *brokers;
    char                *index_url;
    json_t              *index_cfg;

    mpd_t               *protect_rate;
    int                 protect_interval;

    int                 update_interval;
    int                 expire_interval;
    double              request_timeout;
};

extern struct settings settings;

int init_config(const char *path);
int update_index_config(void);

# endif


#ifdef __APPLE__
extern int error(int status, int* error, char* format, ...);
#endif