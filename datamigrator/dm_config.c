/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# include "dm_config.h"
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
    ret = load_db_histories(root, "db_histories", &settings.db_histories);
    if (ret < 0) {
        printf("load db histories fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_mysql(root, "db_history", &settings.db_history);
    if (ret < 0) {
        printf("load history db config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_mysql(root, "db_user", &settings.db_user);
    if (ret < 0) {
        printf("load user db config fail: %d\n", ret);
        return -__LINE__;
    }

    ERR_RET(read_cfg_int(root, "least_day_per_user", &settings.least_day_per_user, false, 7));
    ERR_RET(read_cfg_int(root, "max_order_per_user", &settings.max_order_per_user, false, 50000));
    ERR_RET(read_cfg_int(root, "migrate_mode", &settings.migrate_mode, false, 1));
    ERR_RET(read_cfg_uint32(root, "last_user_id", &settings.last_user_id, false, 0));
    ERR_RET(read_cfg_real(root, "migrate_start_time", &settings.migirate_start_time, true, 1.0));
    ERR_RET(read_cfg_real(root, "migrate_end_time", &settings.migirate_end_time, false, 1.0));
    
    if (settings.migirate_end_time > 10.0) {
        if (settings.migrate_mode == MIGRATE_MODE_FULL) {
            log_stderr("when migirate_end_time has been setted, the migrate mode may be part migration");
            return -__LINE__;
        }
    }

    if (settings.migirate_end_time < 10.0) {
        if (settings.migrate_mode == MIGRATE_MODE_PART) {
            log_stderr("when migirate_end_time does not setted, the migrate mode may be full migration");
            return -__LINE__;
        }
    }

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

