/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# include "ca_config.h"

struct settings settings;

// ses key dict
uint32_t dict_ses_hash_func(const void *key)
{
    return (uintptr_t)key;
}

int dict_ses_hash_compare(const void *key1, const void *key2)
{
    return (uintptr_t)key1 == (uintptr_t)key2 ? 0 : 1;
}

//str key dict
uint32_t dict_str_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

int dict_str_compare(const void *value1, const void *value2)
{
    return strcmp(value1, value2);
}

void *dict_str_dup(const void *value)
{
    return strdup(value);
}

void dict_str_free(void *value)
{
    free(value);
}

static int read_depth_interval_cfg(json_t *root, const char *key)
{
    json_t *obj = json_object_get(root, key);
    if (obj == NULL || !json_is_array(obj))
        return -__LINE__;

    settings.depth_interval.count = json_array_size(obj);
    settings.depth_interval.interval = malloc(sizeof(char *) * settings.depth_interval.count);

    for (int i = 0; i < settings.depth_interval.count; ++i) {
        settings.depth_interval.interval[i] = strdup(json_string_value(json_array_get(obj, i)));
        if (settings.depth_interval.interval[i] == NULL)
            return -__LINE__;
    }

    return 0;
}

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
    ret = load_cfg_rpc_svr(root, "deals_svr", &settings.deals_svr);
    if (ret < 0) {
        printf("load deals_svr config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_rpc_svr(root, "state_svr", &settings.state_svr);
    if (ret < 0) {
        printf("load state_svr config fail: %d\n", ret);
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

    ERR_RET_LN(read_cfg_real(root, "interval_time", &settings.interval_time, false, 1.0));
    ERR_RET_LN(read_cfg_real(root, "backend_timeout", &settings.backend_timeout, false, 1.0));
    ERR_RET_LN(read_cfg_real(root, "market_interval", &settings.market_interval, false, 10));
    ERR_RET_LN(read_cfg_int(root, "depth_limit_max", &settings.depth_limit_max, false, 50));
    ERR_RET(read_cfg_int(root, "deal_max", &settings.deal_max, false, 1000));
    
    ERR_RET(read_depth_interval_cfg(root, "depth_merge"));

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

