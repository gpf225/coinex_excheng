/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/04/01, create
 */

# include "dm_cli.h"
# include "dm_migrate.h"

static cli_svr *svr;

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = migrate_status(reply);
    return reply;
}

static sds on_cmd_cancel(const char *cmd, int argc, sds *argv)
{
    migrate_cancel();
    return sdsnew("OK\n");
}


int init_cli(void)
{
    svr = cli_svr_create(&settings.cli);
    if (svr == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svr, "status", on_cmd_status);
    cli_svr_add_cmd(svr, "cancel", on_cmd_cancel);

    return 0;
}