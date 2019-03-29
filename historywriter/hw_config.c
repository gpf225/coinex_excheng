/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/19, create
 */

# include "hw_config.h"
# include "ut_misc.h"

struct settings settings;

static int load_db_histories(json_t *root, const char *key, ut_mysql_slice *db_histories)
{
    json_t *db_array = json_object_get(root, key);
    if (db_array == NULL || !json_is_array(db_array)) {
        log_stderr("history mysql config invalid, it must be a json array", key);
        return -__LINE__;
    }
    
    db_histories->count = json_array_size(db_array);
    db_histories->configs = malloc(sizeof(struct mysql_cfg) * db_histories->count);
    memset(db_histories->configs, 0, sizeof(struct ut_mysql_slice) * db_histories->count);
    for (size_t i = 0; i < db_histories->count; ++i) {
        json_t *node = json_array_get(db_array, i);
        if (node == NULL || !json_is_object(node)) {
            log_stderr("invalid history db config, please check your config file.");
            return -__LINE__;
        }
        
        int ret = load_cfg_mysql1(node, &db_histories->configs[i]);
        if (ret < 0) {
            log_stderr("load history db config fail: %d at: %zu", ret, i);
            return -__LINE__;
        }
        if (db_histories->configs[i].db != i) {
            log_stderr("please check your db config, it maybe wrong");
            return -__LINE__;
        }
    }

    for (size_t i = 0; i < db_histories->count; ++i) {
        mysql_cfg *cfg = &db_histories->configs[i];
        log_stderr("host[%s] user[%s] pass[%s] name[%s] db[%d]", cfg->host, cfg->user, cfg->pass, cfg->name, cfg->db);
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
    ret = load_cfg_cli_svr(root, "cli", &settings.cli);
    if (ret < 0) {
        printf("load cli config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_kafka_consumer(root, "his_orders", &settings.orders);
    if (ret < 0) {
        printf("load kafka orders config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_kafka_consumer(root, "his_stops", &settings.stops);
    if (ret < 0) {
        printf("load kafka stop_orders config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_kafka_consumer(root, "his_balances", &settings.balances);
    if (ret < 0) {
        printf("load kafka balances config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_kafka_consumer(root, "his_deals", &settings.deals);
    if (ret < 0) {
        printf("load kafka deals config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_redis(root, "redis", &settings.redis);
    if (ret < 0) {
        printf("load redis config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_db_histories(root, "db_history", &settings.db_histories);
    if (ret < 0) {
        printf("load db histories fail: %d\n", ret);
        return -__LINE__;
    }
    ERR_RET_LN(read_cfg_int(root, "worker_per_db", &settings.worker_per_db, false, 2));
    ERR_RET_LN(read_cfg_real(root, "flush_his_interval", &settings.flush_his_interval, false, 0.5));

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

