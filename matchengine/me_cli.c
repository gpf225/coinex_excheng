/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/17, create
 */

# include "me_cli.h"
# include "me_config.h"
# include "me_market.h"
# include "me_persist.h"
# include "me_operlog.h"
# include "me_history.h"
# include "me_message.h"
# include "me_balance.h"

static cli_svr *svr;

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    reply = market_status(reply);
    reply = operlog_status(reply);
    reply = history_status(reply);
    reply = message_status(reply);
    return reply;
}

static sds on_cmd_makeslice(const char *cmd, int argc, sds *argv)
{
    time_t now = time(NULL);
    make_slice(now);
    return sdsnew("OK\n");
}

static sds on_cmd_unfreeze(const char *cmd, int argc, sds *argv)
{
    if (argc != 3) {
        sds reply = sdsempty();
        return sdscatprintf(reply, "usage: %s user_id asset amount\n", cmd);
    }

    uint32_t user_id = strtoul(argv[0], NULL, 0);
    if (user_id <= 0) {
        return sdsnew("failed, user_id error\n");
    }

    char *asset = strdup(argv[1]);
    if (!asset) {
        return sdsnew("failed, asset error\n");
    }
    if (!asset_exist(asset)) {
        free(asset);
        return sdsnew("failed, asset not exist\n");
    }

    mpd_t *amount = decimal(argv[2], asset_prec(asset));
    if (!amount) {
        free(asset);
        return sdsnew("failed, amount error\n");
    }

    mpd_t *frozen = balance_unfreeze(user_id, BALANCE_TYPE_FROZEN, asset, amount);
    if (!frozen) {
        free(asset);
        mpd_del(amount);
        sds reply = sdsempty();
        return sdscatprintf(reply, "unfreeze failed, user_id: %d\n", user_id);
    }

    free(asset);
    mpd_del(amount);
    sds reply = sdsempty();
    return sdscatprintf(reply, "unfreeze success, user_id: %d\n", user_id);
}

static sds on_cmd_history_control(const char *cmd, int argc, sds *argv)
{
    if (argc != 1) {
        sds reply = sdsempty();
        return sdscatprintf(reply, "usage: %s history_mode[1, 2, 3]\n", cmd);
    }

    uint32_t mode = strtoul(argv[0], NULL, 0);
    if (mode != HISTORY_MODE_DIRECT && mode != HISTORY_MODE_KAFKA && mode != HISTORY_MODE_DOUBLE) {
        return sdsnew("failed, mode should be 1, 2 or 3\n");
    }

    settings.history_mode = mode;
    sds reply = sdsempty();
    return sdscatprintf(reply, "change history mode success: %d\n", mode);
}

int init_cli(void)
{
    svr = cli_svr_create(&settings.cli);
    if (svr == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svr, "status", on_cmd_status);
    cli_svr_add_cmd(svr, "makeslice", on_cmd_makeslice);
    cli_svr_add_cmd(svr, "unfreeze", on_cmd_unfreeze);
    cli_svr_add_cmd(svr, "hiscontrol", on_cmd_history_control);

    return 0;
}

