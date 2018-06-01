/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include <curl/curl.h>
# include "me_config.h"

struct settings settings;

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
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(3000));

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

static int load_assets(json_t *node)
{
    settings.asset_num = json_array_size(node);
    settings.assets = malloc(sizeof(struct asset) * settings.asset_num);
    for (size_t i = 0; i < settings.asset_num; ++i) {
        json_t *row = json_array_get(node, i);
        if (!json_is_object(row))
            return -__LINE__;
        ERR_RET_LN(read_cfg_str(row, "name", &settings.assets[i].name, NULL));
        ERR_RET_LN(read_cfg_int(row, "prec_save", &settings.assets[i].prec_save, true, 0));
        ERR_RET_LN(read_cfg_int(row, "prec_show", &settings.assets[i].prec_show, false, settings.assets[i].prec_save));
        if (strlen(settings.assets[i].name) > ASSET_NAME_MAX_LEN)
            return -__LINE__;
    }

    return 0;
}

static int load_markets(json_t *node)
{
    settings.market_num = json_array_size(node);
    settings.markets = malloc(sizeof(struct market) * settings.market_num);
    for (size_t i = 0; i < settings.market_num; ++i) {
        json_t *row = json_array_get(node, i);
        if (!json_is_object(row))
            return -__LINE__;
        ERR_RET_LN(read_cfg_str(row, "name", &settings.markets[i].name, NULL));
        ERR_RET_LN(read_cfg_int(row, "fee_prec", &settings.markets[i].fee_prec, false, 4));
        ERR_RET_LN(read_cfg_mpd(row, "min_amount", &settings.markets[i].min_amount, "0.01"));

        json_t *stock = json_object_get(row, "stock");
        if (!stock || !json_is_object(stock))
            return -__LINE__;
        ERR_RET_LN(read_cfg_str(stock, "name", &settings.markets[i].stock, NULL));
        ERR_RET_LN(read_cfg_int(stock, "prec", &settings.markets[i].stock_prec, true, 0));

        json_t *money = json_object_get(row, "money");
        if (!money || !json_is_object(money))
            return -__LINE__;
        ERR_RET_LN(read_cfg_str(money, "name", &settings.markets[i].money, NULL));
        ERR_RET_LN(read_cfg_int(money, "prec", &settings.markets[i].money_prec, true, 0));
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
    ret = load_cfg_rpc_clt(root, "monitor", &settings.monitor);
    if (ret < 0) {
        printf("load monitor clt config fail: %d\n", ret);
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
    ret = load_cfg_mysql(root, "db_history", &settings.db_history);
    if (ret < 0) {
        printf("load history db config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = read_cfg_str(root, "asset_url", &settings.asset_url, NULL);
    if (ret < 0) {
        printf("load asset url config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = read_cfg_str(root, "market_url", &settings.market_url, NULL);
    if (ret < 0) {
        printf("load market url config fail: %d\n", ret);
        return -__LINE__;
    }
    ret = read_cfg_str(root, "brokers", &settings.brokers, NULL);
    if (ret < 0) {
        printf("load brokers fail: %d\n", ret);
        return -__LINE__;
    }
    ret = read_cfg_int(root, "slice_interval", &settings.slice_interval, false, 86400);
    if (ret < 0) {
        printf("load slice_interval fail: %d", ret);
        return -__LINE__;
    }
    ret = read_cfg_int(root, "slice_keeptime", &settings.slice_keeptime, false, 86400 * 3);
    if (ret < 0) {
        printf("load slice_keeptime fail: %d", ret);
        return -__LINE__;
    }
    ret = read_cfg_int(root, "history_thread", &settings.history_thread, false, 10);
    if (ret < 0) {
        printf("load history_thread fail: %d", ret);
        return -__LINE__;
    }

    ERR_RET_LN(read_cfg_real(root, "cache_timeout", &settings.cache_timeout, false, 0.45));

    return 0;
}

int update_asset_config(void)
{
    json_t *data = request_json(settings.asset_url);
    if (data == NULL)
        return -__LINE__;

    if (settings.assets) {
        free(settings.assets);
        settings.asset_num = 0;
        settings.assets = NULL;
    }

    int ret = load_assets(data);
    json_decref(data);
    return ret;
}

int update_market_config(void)
{
    json_t *data = request_json(settings.market_url);
    if (data == NULL)
        return -__LINE__;

    if (settings.markets) {
        for (size_t i = 0; i < settings.market_num; ++i) {
            mpd_del(settings.markets[i].min_amount);
        }
        free(settings.markets);
        settings.market_num = 0;
        settings.markets = NULL;
    }

    int ret = load_markets(data);
    json_decref(data);
    return ret;
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

