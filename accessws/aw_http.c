/*
 * Description: 
 *     History: damonyang@tencent.com, 2018/01/28, create
 */

# include <curl/curl.h>
# include "aw_config.h"
# include "aw_http.h"

static nw_job *worker;

struct worker_request {
    char *method;
    json_t *params;
    result_callback callback;
};

static size_t post_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
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
    json_object_set    (request, "params", params);
    json_object_set_new(request, "id", json_integer(time(NULL)));
    char *request_data = json_dumps(request, 0);
    json_decref(request);

    CURL *curl = curl_easy_init();
    sds reply_str = sdsempty();

    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_URL, settings.accesshttp);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, post_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(1000));
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
        log_error("http request: %s fail: %s", method, reply_str);
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

static void on_worker(nw_job_entry *entry, void *privdata)
{
    struct worker_request *request = entry->request;
    entry->reply = http_request(request->method, request->params);
}

static void on_worker_finish(nw_job_entry *entry)
{
    if (!entry->reply)
        return;
    struct worker_request *request = entry->request;
    request->callback(entry->reply);
}

static void on_worker_cleanup(nw_job_entry *entry)
{
    struct worker_request *request = entry->request;
    free(request->method);
    json_decref(request->params);
    free(request);

    if (entry->reply) {
        json_decref(entry->reply);
    }
}

int init_http(void)
{
    nw_job_type type;
    memset(&type, 0, sizeof(type));
    type.on_job = on_worker;
    type.on_finish = on_worker_finish;
    type.on_cleanup = on_worker_cleanup;

    worker = nw_job_create(&type, 2);
    if (worker == NULL)
        return -__LINE__;

    return 0;
}

int send_http_request(const char *method, json_t *params, result_callback callback)
{
    struct worker_request *request = malloc(sizeof(struct worker_request));
    request->method = strdup(method);
    request->params = params;
    request->callback = callback;
    return nw_job_add(worker, 0, request);
}

