/*
 * Description: 
 *     History: yang@haipo.mp, 2017/03/17, create
 */

# include "mp_cli.h"
# include "mp_config.h"
# include "mp_message.h"

static cli_svr *svr;

static sds on_cmd_update_market_list(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    int ret = update_market_list();
    if (ret == 0) {
        reply = sdscatprintf(reply, "update market list success\n");
    } else {
        reply = sdscatprintf(reply, "update market list fail: %d\n", ret);
    }

    return reply;
}

int init_cli(void)
{
    svr = cli_svr_create(&settings.cli);
    if (svr == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svr, "update_market_list", on_cmd_update_market_list);

    return 0;
}

