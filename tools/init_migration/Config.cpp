
#include "Config.h"

struct settings settings;


static int load_db_histories(json_t *root, const char *key)
{
    json_t *node = json_object_get(root, key);
    if (node == NULL || !json_is_array(node)) {
        return -__LINE__;
    }

    settings.db_histories = (mysql_cfg*)calloc(sizeof(struct mysql_cfg), DB_HISTORY_COUNT);
    for (size_t i = 0; i < DB_HISTORY_COUNT; ++i) {
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
    ret = load_cfg_log(root, "log", &settings.log);
    if (ret < 0) {
        printf("load log config fail: %d\n", ret);
        return -__LINE__;
    }

    ret = load_cfg_mysql(root, "src_db", &settings.db_src);
    if (ret < 0) {
        printf("load src db config fail: %d\n", ret);
        return -__LINE__;
    }

    ret = load_db_histories(root, "db_history");
    if (ret < 0) {
        printf("load histories db fail: %d\n", ret);
        return -__LINE__;
    }

    ret = load_cfg_mysql(root,"db_log",&settings.db_log);
    if (ret<0) {
        printf("load log db config fail: %d\n", ret);
        return -__LINE__;
    }


//    ret = load_cfg_mysql(root,"db_summary",&settings.db_summary);
//    if (ret<0) {
//        printf("load db_summary db config fail: %d\n", ret);
//        return -__LINE__;
//    }

    ret = load_cfg_redis(root, "redis", &settings.redis);
    if (ret < 0) {
        printf("load redis deals config fail: %d\n", ret);
        return -__LINE__;
    }

    ERR_RET_LN(read_cfg_int(root, "row_limit", &settings.row_limit, false, 1000));
    ERR_RET_LN(read_cfg_int(root,"mode", &settings.mode,false,7));

    ERR_RET_LN(read_cfg_int(root, "pipeline_len_max", &settings.pipeline_len_max, false, 1000));

    ERR_RET_LN(read_cfg_int(root, "min_max", &settings.min_max, false, 60 * 24 * 30));
    ERR_RET_LN(read_cfg_int(root, "hour_max", &settings.hour_max, false, 24 * 365 * 3));

    ERR_RET_LN(read_cfg_int(root, "deal_summary_max", &settings.deal_summary_max, false, 10000));

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
        printf("json is not object\n");
        return -__LINE__;
    }

    int ret = read_config_from_json(root);
    if (ret < 0) {
        json_decref(root);
        printf("read config error\n");
        return ret;
    }

    json_decref(root);
    return 0;
}
