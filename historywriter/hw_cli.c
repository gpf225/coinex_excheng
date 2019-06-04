/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_cli.h"
# include "hw_writer.h"
# include "hw_message.h"

static cli_svr *svr;

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = writer_status(reply);
    reply = message_status(reply);
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
