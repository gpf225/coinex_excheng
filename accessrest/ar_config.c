/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/21, create
 */

# include "ar_config.h"

struct settings settings;

static int read_depth_limit_cfg(json_t *root, const char *key)
{
    json_t *obj = json_object_get(root, key);
    if (obj == NULL || !json_is_array(obj))
        return -__LINE__;

    settings.depth_limit.count = json_array_size(obj);
    settings.depth_limit.limit = malloc(sizeof(int) * settings.depth_limit.count);

    for (int i = 0; i < settings.depth_limit.count; ++i) {
        settings.depth_limit.limit[i] = json_integer_value(json_array_get(obj, i));
        if (settings.depth_limit.limit[i] == 0)
            return -__LINE__;
    }

    return 0;
}

static int read_depth_merge_cfg(json_t *root, const char *key)
{
    json_t *obj = json_object_get(root, key);
    if (obj == NULL || !json_is_array(obj))
        return -__LINE__;

    settings.depth_merge.count = json_array_size(obj);
    settings.depth_merge.limit = malloc(sizeof(mpd_t *) * settings.depth_merge.count);

    for (int i = 0; i < settings.depth_merge.count; ++i) {
        settings.depth_merge.limit[i] = decimal(json_string_value(json_array_get(obj, i)), 0);
        if (settings.depth_merge.limit[i] == NULL)
            return -__LINE__;
    }

    return 0;
}

static int read_config_from_json(json_t *root)
{
    int ret;
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
    ret = load_cfg_http_svr(root, "svr", &settings.svr);
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
    ret = load_cfg_rpc_clt(root, "cache", &settings.cache);
    if (ret < 0) {
        printf("load cache clt config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_rpc_clt(root, "cache_deals", &settings.cache_deals);
    if (ret < 0) {
        printf("load cache_deals clt config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_rpc_clt(root, "cache_state", &settings.cache_state);
    if (ret < 0) {
        printf("load cache_state clt config fail: %d\n", ret);
        return -__LINE__;
    }

    ERR_RET(read_cfg_int(root, "worker_num", &settings.worker_num, false, 1));
    ERR_RET(read_cfg_real(root, "backend_timeout", &settings.backend_timeout, false, 1.0));
    ERR_RET(read_cfg_real(root, "cache_timeout", &settings.cache_timeout, false, 0.5));
    ERR_RET(read_cfg_real(root, "market_interval", &settings.market_interval, false, 10));
    ERR_RET(read_cfg_str(root, "market_url", &settings.market_url, NULL));
    ERR_RET(read_cfg_int(root, "deal_max", &settings.deal_max, false, 1000));
    ERR_RET(read_cfg_int(root, "kline_max", &settings.kline_max, false, 1000));
    
    ERR_RET(read_depth_limit_cfg(root, "depth_limit"));
    ERR_RET(read_depth_merge_cfg(root, "depth_merge"));

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

