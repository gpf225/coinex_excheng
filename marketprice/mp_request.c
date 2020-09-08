/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2020/08/24, create
 */

# include <curl/curl.h>
# include "mp_config.h"
# include "mp_request.h"

static nw_job *job;

struct request_context {
    char              *method;
    request_callback  callback;
};

static size_t write_callback_func(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

static json_t *http_request(const char *method, json_t *params)
{
    json_t *reply  = NULL;
    json_t *error  = NULL;
    json_t *result = NULL;

    json_t *request = json_object();
    json_object_set_new(request, "method", json_string(method));
    if (params) {
        json_object_set(request, "params", params);
    } else {
        json_object_set_new(request, "params", json_array());
    }
    json_object_set_new(request, "id", json_integer(time(NULL)));
    char *request_data = json_dumps(request, 0);
    json_decref(request);

    CURL *curl = curl_easy_init();
    sds reply_str = sdsempty();

    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_URL, settings.accesshttp);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback_func);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(5000));
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_data);

    CURLcode ret = curl_easy_perform(curl);
    if (ret != CURLE_OK) {
        log_fatal("curl_easy_perform fail: %s", curl_easy_strerror(ret));
        goto cleanup;
    }

    reply = json_loads(reply_str, 0, NULL);
    if (reply == NULL)
        goto cleanup;
    error = json_object_get(reply, "error");
    if (!json_is_null(error)) {
        log_error("get market list fail: %s", reply_str);
        goto cleanup;
    }
    result = json_object_get(reply, "result");
    json_incref(result);

cleanup:
    free(request_data);
    sdsfree(reply_str);
    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);
    if (reply)
        json_decref(reply);

    return result;
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    double start = current_timestamp();
    struct request_context *req = entry->request;
    entry->reply = http_request(req->method, NULL);
    double end = current_timestamp();
    log_info("request method: %s cost: %f", req->method, end - start);
}

static void on_job_finish(nw_job_entry *entry)
{
    struct request_context *req = entry->request;
    req->callback(entry->reply);
}

static void on_job_cleanup(nw_job_entry *entry)
{
    struct request_context *req = entry->request;
    if (req->method) {
        free(req->method);
    }
    free(req);

    if (entry->reply)
        json_decref(entry->reply);
}

json_t *get_market_list()
{    
    return http_request("market.list", NULL);
}

json_t *get_index_list()
{    
    return http_request("index.list", NULL);
}

int add_request(const char *method, request_callback callback)
{
    struct request_context *req = malloc(sizeof(struct request_context));
    memset(req, 0, sizeof(struct request_context));
    req->method     = strdup(method);
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

    job = nw_job_create(&type, 2);
    if (job == NULL)
        return -__LINE__;
    return 0;
}

