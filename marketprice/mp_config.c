/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/16, create
 */

# include "mp_config.h"
# include "mp_message.h"

#ifdef __APPLE__
int error(int status, int error, char* format, ...)
{
    va_list ap;
    va_start(ap,format);
    vprintf(format,ap);
    va_end(ap);
    return 0;
}
#endif

struct settings settings;

static int read_config_from_json(json_t *root)
{
    int ret;
    ret = read_cfg_bool(root, "debug", &settings.debug, false, false);
    if (ret < 0) {
        printf("read debug config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_process(root, "process", &settings.process);
    if (ret < 0) {
        printf("load process config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_log(root, "log", &settings.log);
    if (ret < 0) {
        printf("load log config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_alert(root, "alert", &settings.alert);
    if (ret < 0) {
        printf("load alert config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_rpc_svr(root, "svr", &settings.svr);
    if (ret < 0) {
        printf("load svr config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_redis(root, "redis", &settings.redis);
    if (ret < 0) {
        printf("load redis deals config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_mysql(root, "db_log", &settings.db_log);
    if (ret < 0) {
        printf("load history db config fail: %d\n", ret);
        return -__LINE__;
    }

    ERR_RET_LN(read_cfg_str(root, "brokers", &settings.brokers, NULL));
    ERR_RET_LN(read_cfg_int(root, "sec_max", &settings.sec_max, false, 86400 * 3));
    ERR_RET_LN(read_cfg_int(root, "min_max", &settings.min_max, false, 60 * 24 * 30));
    ERR_RET_LN(read_cfg_int(root, "hour_max", &settings.hour_max, false, 24 * 365 * 3));
    ERR_RET_LN(read_cfg_int(root, "deal_summary_max", &settings.deal_summary_max, false, 1000));
    ERR_RET_LN(read_cfg_int(root, "pipeline_len_max", &settings.pipeline_len_max, false, 1000));
    ERR_RET_LN(read_cfg_int(root, "kline_max", &settings.kline_max, false, 1000));
    ERR_RET_LN(read_cfg_int(root, "worker_num", &settings.worker_num, false, 4));
    ERR_RET_LN(read_cfg_real(root, "cache_timeout", &settings.cache_timeout, false, 0.4));
    ERR_RET_LN(read_cfg_real(root, "worker_timeout", &settings.worker_timeout, false, 0.5));
    ERR_RET_LN(read_cfg_str(root, "accesshttp", &settings.accesshttp, NULL));

    if (settings.deal_summary_max <= 0 || settings.deal_summary_max > MARKET_DEALS_MAX)
        return -__LINE__;
    return 0;
}

int init_config(const char *path)
{
    json_error_t error;
    json_t *root = json_load_file(path, 0, &error);
    if (root == NULL) {
        printf("json_load_file from: %s fail: %s in line: %d\n", path, error.text, error.line);
        return -__LINE__;
    }
    if (!json_is_object(root)) {
        json_decref(root);
        return -__LINE__;
    }
    int ret = read_config_from_json(root);
    if (ret < 0) {
        json_decref(root);
        return ret;
    }
    json_decref(root);

    return 0;
}

