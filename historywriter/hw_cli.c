/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_cli.h"
# include "hw_dispatcher.h"
# include "hw_message.h"

static cli_svr *svr;

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = history_status(reply);
    reply = message_status(reply);
    return reply;
}

static sds on_cmd_stop(const char *cmd, int argc, sds *argv)
{
    message_stop(1);
    return sdsnew("OK\n");
}

static sds on_cmd_dump(const char *cmd, int argc, sds *argv)
{
    dump_hisotry();
    return sdsnew("OK\n");
}

int init_cli(void)
{
    svr = cli_svr_create(&settings.cli);
    if (svr == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svr, "status", on_cmd_status);
    cli_svr_add_cmd(svr, "stop", on_cmd_stop);
    cli_svr_add_cmd(svr, "dump", on_cmd_dump);

    return 0;
}