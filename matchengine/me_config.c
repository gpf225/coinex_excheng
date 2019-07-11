/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include <curl/curl.h>
# include "me_config.h"

struct settings settings;

static void convert_fee_dict_val_free(void *val)
{
    struct convert_fee *obj = val;
    if (obj->money)
        free(obj->money);
    if (obj->price)
        mpd_del(obj->price);
    free(obj);
}

static size_t write_callback_func(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

static json_t *request_json(const char *url)
{
    json_t *reply  = NULL;
    json_t *result = NULL;

    CURL *curl = curl_easy_init();
    sds reply_str = sdsempty();

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback_func);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(1000));

    CURLcode ret = curl_easy_perform(curl);
    if (ret != CURLE_OK) {
        log_fatal("get %s fail: %s", url, curl_easy_strerror(ret));
        goto cleanup;
    }

    reply = json_loads(reply_str, 0, NULL);
    if (reply == NULL) {
        log_error("parse %s reply fail: %s", url, reply_str);
        goto cleanup;
    }
    int code = json_integer_value(json_object_get(reply, "code"));
    if (code != 0) {
        log_error("reply error: %s: %s", url, reply_str);
        goto cleanup;
    }
    result = json_object_get(reply, "data");
    json_incref(result);

cleanup:
    curl_easy_cleanup(curl);
    sdsfree(reply_str);
    if (reply)
        json_decref(reply);

    return result;
}

static int load_convert_fee(json_t *node)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_destructor = convert_fee_dict_val_free;

    settings.convert_fee_dict = dict_create(&dt, 8);
    if (node == NULL) {
        log_stderr("no convert fee config");
        return -__LINE__;
    }

    const char *key;
    json_t *val;
    json_object_foreach(node, key, val) {
        char *money;
        mpd_t *price;

        ERR_RET_LN(read_cfg_str(val, "money", &money, NULL));
        ERR_RET_LN(read_cfg_mpd(val, "price", &price, NULL));

        if (mpd_cmp(price, mpd_zero, &mpd_ctx) == 0) {
            return -__LINE__;
        }

        struct convert_fee *obj = malloc(sizeof(struct convert_fee));
        obj->money = money;
        obj->price = price;

        if (dict_add(settings.convert_fee_dict, (void *)key, obj) < 0) {
            return -__LINE__;
        }
        log_stderr("load convert fee: asset: %s, money: %s", key, money);
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
    ret = load_cfg_cli_svr(root, "cli", &settings.cli);
    if (ret < 0) {
        printf("load cli config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_cfg_mysql(root, "db_log", &settings.db_log);
    if (ret < 0) {
        printf("load log db config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = load_convert_fee(json_object_get(root, "convert_fee"));
    if (ret < 0) {
        printf("load load_convert_fee fail: %d", ret);
        return -__LINE__;
    }

    ERR_RET_LN(read_cfg_str(root, "asset_url", &settings.asset_url, NULL));
    ERR_RET_LN(read_cfg_str(root, "market_url", &settings.market_url, NULL));

    ERR_RET_LN(read_cfg_str(root, "brokers", &settings.brokers, NULL));
    ERR_RET_LN(read_cfg_int(root, "slice_interval", &settings.slice_interval, false, 86400));
    ERR_RET_LN(read_cfg_int(root, "slice_keeptime", &settings.slice_keeptime, false, 86400 * 3));
    ERR_RET_LN(read_cfg_int(root, "depth_merge_max", &settings.depth_merge_max, false, 1000));
    ERR_RET_LN(read_cfg_int(root, "min_save_prec", &settings.min_save_prec, false, 24));
    ERR_RET_LN(read_cfg_int(root, "discount_prec", &settings.discount_prec, false, 2));

    ERR_RET_LN(read_cfg_int(root, "reader_num", &settings.reader_num, false, 2));
    ERR_RET_LN(read_cfg_real(root, "cache_timeout", &settings.cache_timeout, false, 0.1));
    ERR_RET_LN(read_cfg_real(root, "order_fini_keeptime", &settings.order_fini_keeptime, false, 300.0));
    ERR_RET_LN(read_cfg_real(root, "worker_timeout", &settings.worker_timeout, false, 1.0));
    ERR_RET_LN(read_cfg_real(root, "call_auction_calc_interval", &settings.call_auction_calc_interval, false, 1.0));

    return 0;
}

int update_asset_config(void)
{
    json_t *data = request_json(settings.asset_url);
    if (data == NULL)
        return -__LINE__;
    if (settings.asset_cfg)
        json_decref(settings.asset_cfg);
    settings.asset_cfg = data;
    return 0;
}

int update_market_config(void)
{
    json_t *data = request_json(settings.market_url);
    if (data == NULL)
        return -__LINE__;
    if (settings.market_cfg)
        json_decref(settings.market_cfg);
    settings.market_cfg = data;
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

    ERR_RET(update_asset_config());
    ERR_RET(update_market_config());

    return 0;
}

