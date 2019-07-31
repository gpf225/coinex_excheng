# include <curl/curl.h>
# include "me_config.h"
# include "me_request.h"

static nw_job *job;

struct request_context {
    char              *url;
    nw_ses            *ses;
    rpc_pkg           pkg;
    request_callback  callback;
};

static size_t write_callback_func(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

static json_t *http_request(const char *url, double timeout)
{
    json_t *reply  = NULL;
    json_t *result = NULL;

    CURL *curl = curl_easy_init();
    sds reply_str = sdsempty();

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback_func);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(timeout * 1000));

    CURLcode ret = curl_easy_perform(curl);
    if (ret != CURLE_OK) {
        log_fatal("get %s fail: %s", url, curl_easy_strerror(ret));
        goto cleanup;
    }

    reply = json_loads(reply_str, 0, NULL);
    if (reply == NULL) {
        log_fatal("parse %s reply fail: %s", url, reply_str);
        goto cleanup;
    }
    int code = json_integer_value(json_object_get(reply, "code"));
    if (code != 0) {
        log_fatal("reply error: %s: %s", url, reply_str);
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

static void on_job(nw_job_entry *entry, void *privdata)
{
    double start = current_timestamp();
    struct request_context *req = entry->request;
    entry->reply = http_request(req->url, settings.worker_timeout);
    double end = current_timestamp();
    log_info("request url: %s cost: %f", req->url, end - start);
}

static void on_job_finish(nw_job_entry *entry)
{
    struct request_context *req = entry->request;
    req->callback(entry->reply, req->ses, &req->pkg);
}

static void on_job_cleanup(nw_job_entry *entry)
{
    struct request_context *req = entry->request;
    free(req);
}

int init_asset_config(void)
{
    json_t *data = http_request(settings.asset_url, settings.worker_timeout);
    if (data == NULL)
        return -__LINE__;
    if (settings.asset_cfg)
        json_decref(settings.asset_cfg);
    settings.asset_cfg = data;
    return 0;
}

int init_market_config(void)
{
    json_t *data = http_request(settings.market_url, settings.worker_timeout);
    if (data == NULL)
        return -__LINE__;
    if (settings.market_cfg)
        json_decref(settings.market_cfg);
    settings.market_cfg = data;
    return 0;
}

int update_assert_config(nw_ses *ses, rpc_pkg *pkg, request_callback callback)
{    
    struct request_context *req = malloc(sizeof(struct request_context));
    memcpy(&req->pkg, pkg, sizeof(rpc_pkg));
    req->url        = settings.asset_url;
    req->ses        = ses;
    req->callback   = callback;
    return nw_job_add(job, 0, req);
}

int update_market_config(nw_ses *ses, rpc_pkg *pkg, request_callback callback)
{
    struct request_context *req = malloc(sizeof(struct request_context));
    memcpy(&req->pkg, pkg, sizeof(rpc_pkg));
    req->url        = settings.market_url;
    req->ses        = ses;
    req->callback   = callback;
    return nw_job_add(job, 0, req);
}

int init_request(void)
{
    nw_job_type type;
    memset(&type, 0, sizeof(type));
    type.on_job     = on_job;
    type.on_finish  = on_job_finish;
    type.on_cleanup = on_job_cleanup;

    job = nw_job_create(&type, 1);
    if (job == NULL)
        return -__LINE__;
    return 0;
}

