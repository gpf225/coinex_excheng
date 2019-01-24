/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# include "ca_cli.h"
# include "ca_config.h"
# include "ca_statistic.h"

static cli_svr *svr;

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = stat_status(reply);
    return reply;
}

int init_cli(void)
{
    svr = cli_svr_create(&settings.cli);
    if (svr == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svr, "status", on_cmd_status);

    return 0;
}

