/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/24, create
 */

# include "hr_config.h"

#ifdef __APPLE__
int error(int status, int* error, char* format, ...)
{
    va_list ap;
    va_start(ap,format);
    vprintf(format,ap);
    va_end(ap);
    return 0;
}
#endif

struct settings settings;

static int load_db_histories(json_t *root, const char *key)
{
    json_t *node = json_object_get(root, key);
    if (node == NULL || !json_is_array(node)) {
        return -__LINE__;
    }
    
    settings.db_history_count = json_array_size(node);
    settings.db_histories = calloc(sizeof(struct mysql_cfg), settings.db_history_count);
    for (size_t i = 0; i < settings.db_history_count; ++i) {
        json_t *item = json_array_get(node, i);
        if (item == NULL || !json_is_object(item)) {
            return -__LINE__;
        }
        int ret = load_cfg_mysql_node(item, &settings.db_histories[i]);
        if (ret < 0) {
            return -__LINE__;
        }
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
    ret = load_db_histories(root, "db_history");
    if (ret < 0) {
        printf("load db histories fail: %d\n", ret);
        return -__LINE__;
    }

    ERR_RET_LN(read_cfg_int(root, "worker_num", &settings.worker_num, false, 10));

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

