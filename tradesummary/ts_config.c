/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "ts_config.h"

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

static bool is_client_id_valid(const char *client_id)
{
    if (!client_id) {
        return false;
    }

    size_t len = strlen(client_id);
    if (len > CLIENT_ID_MAX_LEN) {
        return false;
    }

    bool is_valid = true;
    for (int i = 0; i < len; ++i) {
        if (client_id[i] == '-' || client_id[i] == '_' || isalnum(client_id[i])) {
            continue;
        }
        is_valid = false;
        break;
    }
    return is_valid;
}

static int load_client_ids(json_t *node)
{
    if (node == NULL || !json_is_array(node) || json_array_size(node) == 0) {
        return 0;
    }

    settings.client_id_count = json_array_size(node);
    settings.client_ids = malloc(sizeof(char *) * settings.client_id_count);
    for (size_t i = 0; i < settings.client_id_count; i++) {
        if (!json_is_string(json_array_get(node, i)) || !is_client_id_valid(json_string_value(json_array_get(node, i)))) {
            return __LINE__;
        }
        settings.client_ids[i] = strdup(json_string_value(json_array_get(node, i)));
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
    ret = load_cfg_mysql(root, "db_summary", &settings.db_summary);
    if (ret < 0) {
        printf("load history db config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_redis(root, "redis", &settings.redis);
    if (ret < 0) {
        printf("load redis config fail: %d\n", ret);
        return -__LINE__;
    }

    ERR_RET_LN(read_cfg_str(root, "brokers", &settings.brokers, NULL));
    ERR_RET_LN(read_cfg_int(root, "keep_days", &settings.keep_days, false, 3));
    ERR_RET_LN(read_cfg_str(root, "accesshttp", &settings.accesshttp, NULL));
    ERR_RET_LN(load_client_ids(json_object_get(root, "client_ids")));
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

