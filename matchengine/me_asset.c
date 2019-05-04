/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# include "me_asset.h"
# include "me_dump.h"

static int asset_backup(sds table)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    log_info("backup asset to: %s", table);
    int ret = dump_balance(conn, table);
    if (ret < 0) {
        log_error("backup asset to %s fail: %d", table, ret);
        mysql_close(conn);
        return -__LINE__;
    }

    log_info("backup asset success");
    mysql_close(conn);
    return 0;
}

int make_asset_backup(json_t *params)
{
    time_t t = time(NULL);
    sds table = sdsempty();
    table = sdscatprintf(table, "backup_balance_%ld", t);

    dlog_flush_all();
    int pid = fork();
    if (pid < 0) {
        log_fatal("fork fail: %d", pid);
        sdsfree(table);
        return -__LINE__;
    } else if (pid > 0) {
        json_object_set_new(params, "table", json_string(table));
        json_object_set_new(params, "time", json_integer(t));
        sdsfree(table);
        return 0;
    }

    asset_backup(table);
    sdsfree(table);
    exit(0);
    return 0;
}
