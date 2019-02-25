/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# include "lp_config.h"

struct settings settings;

static int read_config_from_json(json_t *root)
{
    assert(root != NULL);

    int ret = load_cfg_process(root, "process", &settings.process);
    if (ret < 0) {
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
    ret = load_cfg_rpc_clt(root, "matchengine", &settings.matchengine);
    if (ret < 0) {
        printf("load matchengine clt config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_rpc_clt(root, "marketprice", &settings.marketprice);
    if (ret < 0) {
        printf("load marketprice clt config fail: %d\n", ret);
        return -__LINE__;
    }

    ERR_RET(read_cfg_real(root, "poll_depth_interval", &settings.poll_depth_interval, false, 0.5));
    ERR_RET(read_cfg_real(root, "poll_state_interval", &settings.poll_state_interval, false, 0.5));
    ERR_RET(read_cfg_real(root, "poll_market_interval", &settings.poll_market_interval, false, 60.0));
    ERR_RET(read_cfg_real(root, "statistic_interval", &settings.statistic_interval, false, 10.0));

    ERR_RET(read_cfg_real(root, "backend_timeout", &settings.backend_timeout, false, 5));
    ERR_RET(read_cfg_bool(root, "debug", &settings.debug, false, true));
    ERR_RET(read_cfg_int(root, "depth_limit_max", &settings.depth_limit_max, false, 50));
    
    log_stderr("poll_depth_interval:%f poll_state_interval:%f", settings.poll_depth_interval, settings.poll_state_interval);
    return 0;
}

int init_config(const char *path)
{
    assert(path != NULL);
    printf("config path:%s\n", path);

    json_error_t error;
    json_t *root = json_load_file(path, 0, &error);
    if (root == NULL) {
        printf("json_load_file from: %s fail: %s in line: %d.\n", path, error.text, error.line);
        return -__LINE__;
    }

    if (!json_is_object(root)) {
        printf("config file:%s does not valid, please check its content.\n", path);
        json_decref(root);
        return -__LINE__;
    }
    
    int ret = read_config_from_json(root);
    if (ret < 0) {
        json_decref(root);
        return ret;
    }
    
    return 0;
}