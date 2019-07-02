/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# include "me_asset.h"
# include "me_dump.h"
# include "ut_comm_dict.h"

dict_t *dict_asset;

static void *asset_dict_val_dup(const void *val)
{
    struct asset_type *obj = malloc(sizeof(struct asset_type));
    memcpy(obj, val, sizeof(struct asset_type));
    return obj;
}

static void asset_dict_val_free(void *val)
{
    struct asset_type *obj = val;
    mpd_del(obj->min);
    free(obj);
}

static void dict_free(void *val)
{
    dict_release(val);
}

static int init_account(uint32_t account, json_t *assets)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_dup        = asset_dict_val_dup;
    dt.val_destructor = asset_dict_val_free;

    dict_t *dict = dict_create(&dt, 64);
    if (dict == NULL)
        return -__LINE__;
    if (dict_add(dict_asset, (void *)(uintptr_t)account, dict) == NULL)
        return -__LINE__;

    const char *key;
    json_t *asset;
    json_object_foreach(assets, key, asset) {
        struct asset_type at;
        ERR_RET_LN(read_cfg_int(asset, "prec_save", &at.prec_save, true, 0));
        ERR_RET_LN(read_cfg_int(asset, "prec_show", &at.prec_show, true, 0));

        size_t asset_len = strlen(key);
        if (asset_len == 0 || asset_len > ASSET_NAME_MAX_LEN) {
            log_stderr("init account: %u, asset: %s fail", account, key);
            return -__LINE__;
        }

        if (at.prec_save < settings.min_save_prec) {
            log_stderr("init account: %u, asset: %s, prec save: %d, min_save_prec: %d", account, key, at.prec_save, settings.min_save_prec);
            return -__LINE__;
        }

        at.min = mpd_new(&mpd_ctx);
        mpd_set_i32(at.min, -at.prec_show, &mpd_ctx);
        mpd_pow(at.min, mpd_ten, at.min, &mpd_ctx);
        log_stderr("init account: %u, asset: %s, prec save: %d, prec show: %d", account, key, at.prec_save, at.prec_show);
        if (dict_add(dict, (void *)key, &at) < 0) {
            return -__LINE__;
        }
    }

    return 0;
}

int init_asset(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func;
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_destructor = dict_free;

    dict_asset = dict_create(&dt, 64);
    if (dict_asset == NULL)
        return -__LINE__;

    const char *key;
    json_t *assets;
    json_object_foreach(settings.asset_cfg, key, assets) {
        uint32_t account = strtod(key, NULL);
        if (dict_find(dict_asset, (void *)(uintptr_t)account) != NULL) {
            log_error("duplicated account: %s", key);
            return -__LINE__;
        }
        int ret = init_account(account, assets);
        if (ret < 0) {
            log_error("init account: %s fail: %d", key, ret);
            return -__LINE__;
        }
    }

    return 0;
}

static int update_account(uint32_t account, dict_t *dict, json_t *assets)
{
    const char *key;
    json_t *asset;
    json_object_foreach(assets, key, asset) {
        dict_entry *entry = dict_find(dict, key);
        if (entry) {
            struct asset_type *type = entry->val;
            int prec_save;
            int prec_show;
            ERR_RET_LN(read_cfg_int(asset, "prec_save", &prec_save, false, type->prec_save));
            ERR_RET_LN(read_cfg_int(asset, "prec_show", &prec_show, false, type->prec_show));

            if (type->prec_save < prec_save)
                type->prec_save = prec_save;
            type->prec_show = prec_show;

            if (prec_save < settings.min_save_prec)
                log_fatal("update account fail, account: %u, asset: %s, prec save: %d, min_save_prec: %d", account, key, prec_save, settings.min_save_prec);
            continue;
        }

        size_t asset_len = strlen(key);
        if (asset_len == 0 || asset_len > ASSET_NAME_MAX_LEN) {
            log_fatal("update account: %u, asset: %s fail", account, key);
            continue;
        }

        struct asset_type at;
        ERR_RET_LN(read_cfg_int(asset, "prec_save", &at.prec_save, true, 0));
        ERR_RET_LN(read_cfg_int(asset, "prec_show", &at.prec_show, true, 0));

        if (at.prec_save < settings.min_save_prec) {
            log_fatal("update account fail, account: %u, asset: %s, prec save: %d, min_save_prec: %d", account, key, at.prec_save, settings.min_save_prec);
            return -__LINE__;
        }

        at.min = mpd_new(&mpd_ctx);
        mpd_set_i32(at.min, -at.prec_show, &mpd_ctx);
        mpd_pow(at.min, mpd_ten, at.min, &mpd_ctx);
        log_info("update account: %u, asset: %s, prec save: %d, prec show: %d", account, key, at.prec_save, at.prec_show);
        if (dict_add(dict, (void *)key, &at) < 0) {
            return -__LINE__;
        }
    }

    return 0;
}

int update_asset(void)
{
    const char *key;
    json_t *assets;
    json_object_foreach(settings.asset_cfg, key, assets) {
        uint32_t account = strtod(key, NULL);
        dict_entry *entry = dict_find(dict_asset, (void *)(uintptr_t)account);
        if (entry == NULL) {
            int ret = init_account(account, assets);
            if (ret < 0) {
                log_error("init account: %s fail: %d", key, ret);
                return -__LINE__;
            }
        } else {
            int ret = update_account(account, entry->val, assets);
            if (ret < 0) {
                log_error("update account: %s fail: %d", key, ret);
                return -__LINE__;
            }
        }
    }

    return 0;
}

bool account_exist(uint32_t account)
{
    dict_entry *entry = dict_find(dict_asset, (void *)(uintptr_t)account);
    if (entry == NULL)
        return false;
    return true;
}

bool asset_exist(uint32_t account, const char *asset)
{
    dict_entry *entry = dict_find(dict_asset, (void *)(uintptr_t)account);
    if (entry == NULL)
        return false;
    if (dict_find(entry->val, asset) == NULL)
        return false;
    return true;
}

struct asset_type *get_asset_type(uint32_t account, const char *asset)
{
    dict_entry *entry = dict_find(dict_asset, (void *)(uintptr_t)account);
    if (entry == NULL)
        return NULL;
    entry = dict_find(entry->val, asset);
    if (entry == NULL)
        return NULL;
    return entry->val;
}

int asset_prec_save(uint32_t account, const char *asset)
{
    struct asset_type *at = get_asset_type(account, asset);
    return at ? at->prec_save : -1;
}

int asset_prec_show(uint32_t account, const char *asset)
{
    struct asset_type *at = get_asset_type(account, asset);
    return at ? at->prec_show: -1;
}

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

json_t *get_asset_config(void)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_asset);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t account = (uintptr_t)entry->key;
        dict_t *dict_account = entry->val;

        json_t *account_info = json_object();
        dict_entry *entry_account;
        dict_iterator *iter_account = dict_get_iterator(dict_account);
        while ((entry_account = dict_next(iter_account)) != NULL) {
            const char *name = entry_account->key;
            struct asset_type *type = entry_account->val;

            json_t *info = json_object();
            json_object_set_new(info, "prec_show", json_integer(type->prec_show));
            json_object_set_new(info, "prec_save", json_integer(type->prec_save));
            json_object_set_new(account_info, name, info);
        }
        dict_release_iterator(iter_account);

        char account_str[20];
        snprintf(account_str, sizeof(account_str), "%u", account);
        json_object_set_new(result, account_str, account_info);
    }
    dict_release_iterator(iter);

    return result;
}

