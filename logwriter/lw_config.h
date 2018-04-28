/*
 * Description: 
 *     History: yang@haipo.me, 2016/12/03, create
 */

# ifndef _LW_CONFIG_H_
# define _LW_CONFIG_H_

# include <stdio.h>
# include <error.h>
# include <errno.h>
# include <ctype.h>
# include <unistd.h>
# include <assert.h>

# include "nw_svr.h"
# include "ut_log.h"
# include "ut_sds.h"
# include "ut_misc.h"
# include "ut_config.h"

struct instance_cfg {
    int                 port;
    log_cfg             log;
};

struct settings {
    int                 instance_count;
    struct instance_cfg *instances;
};

extern struct settings settings;
int load_config(const char *path);

# endif

