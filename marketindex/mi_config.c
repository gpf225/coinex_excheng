/*
 * Description: 
 *     History: ouxiangyang@viabtc.com, 2018/10/15, create
 */

# include <curl/curl.h>
# include "mi_config.h"

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

int update_index_config(void)
{
    json_t *data = request_json(settings.index_url);
    if (data == NULL)
        return -__LINE__;
    if (settings.index_cfg)
        json_decref(settings.index_cfg);
    settings.index_cfg = data;
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
    ret = load_cfg_mysql(root, "db_log", &settings.db_log);
    if (ret < 0) {
        printf("load log db config fail: %d\n", ret);
        return -__LINE__;
    } 

    ERR_RET_LN(read_cfg_str(root, "brokers", &settings.brokers, NULL));
    ERR_RET_LN(read_cfg_str(root, "index_url", &settings.index_url, NULL));
    ERR_RET_LN(read_cfg_int(root, "update_interval", &settings.update_interval, false, 5));
    ERR_RET_LN(read_cfg_int(root, "expire_interval", &settings.expire_interval, false, 900));
    ERR_RET_LN(read_cfg_real(root, "request_timeout", &settings.request_timeout, false, 3.0));
    ERR_RET_LN(read_cfg_int(root, "protect_interval", &settings.protect_interval, false, 60));
    ERR_RET_LN(read_cfg_mpd(root, "protect_rate", &settings.protect_rate, "0.2"));

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

    ERR_RET_LN(update_index_config());

    return 0;
}

