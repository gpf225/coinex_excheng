/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include <curl/curl.h>
# include <openssl/crypto.h>

# include "mi_config.h"
# include "mi_request.h"

static nw_job *job_context;
static pthread_mutex_t *lockarray;

struct request_context {
    uint32_t            id;
    char                *exchange;
    char                *url;
    double              timeout;
    request_callback    callback;
};

static void lock_callback(int mode, int type, const char *file, int line)
{
    if(mode & CRYPTO_LOCK) {
        pthread_mutex_lock(&(lockarray[type]));
    } else {
        pthread_mutex_unlock(&(lockarray[type]));
    }
}

static unsigned long thread_id(void)
{
    return pthread_self();
}

static void init_openssl_locks(void)
{
    lockarray = (pthread_mutex_t *)OPENSSL_malloc(CRYPTO_num_locks() * sizeof(pthread_mutex_t));
    for(int i = 0; i < CRYPTO_num_locks(); i++) {
        pthread_mutex_init(&(lockarray[i]), NULL);
    }

    CRYPTO_set_id_callback(thread_id);
    CRYPTO_set_locking_callback(lock_callback);
}

static size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

static json_t *http_request(const char *url, double timeout)
{
    json_t *reply= NULL;
    sds reply_str = sdsempty();
    CURL *curl = curl_easy_init();

    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(timeout * 1000));

    profile_inc("request", 1);
    double start = current_timestamp();
    CURLcode ret = curl_easy_perform(curl);
    if (ret != CURLE_OK) {
        log_error("curl_easy_perform fail: %s, url: %s, timeout: %f", curl_easy_strerror(ret), url, timeout);
        profile_inc("request_fail", 1);
        goto cleanup;
    }

    json_error_t error;
    reply = json_loads(reply_str, JSON_DECODE_INT_AS_REAL, &error);
    if (reply == NULL) {
        log_error("json_loads fail: %s, url: %s, reply: %s", error.text, url, reply_str);
        goto cleanup;
    }

    profile_inc("request_success", 1);
    log_trace("cost %f, url: %s, reply:\n%s", current_timestamp() - start, url, reply_str);
cleanup:
    sdsfree(reply_str);
    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);

    return reply;
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    struct request_context *req = entry->request;
    entry->reply = http_request(req->url, req->timeout);
}

static void on_job_finish(nw_job_entry *entry)
{
    struct request_context *req = entry->request;
    req->callback(req->id, req->exchange, entry->reply);
}

static void on_job_cleanup(nw_job_entry *entry)
{
    struct request_context *req = entry->request;
    free(req->exchange);
    free(req->url);
    free(req);
    if (entry->reply) {
        json_decref(entry->reply);
    }
}

int init_request(void)
{
    init_openssl_locks();
    curl_global_init(CURL_GLOBAL_ALL);

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_job = on_job;
    jt.on_finish = on_job_finish;
    jt.on_cleanup = on_job_cleanup;

    job_context = nw_job_create(&jt, REQUEST_THREAD_COUNT);
    if (job_context == NULL)
        return -__LINE__;

    return 0;
}

int fini_request(void)
{
    nw_job_release(job_context);
    return 0;
}

int send_request(uint32_t id, const char *exchange, const char *url, double timeout, request_callback callback)
{
    struct request_context *req = malloc(sizeof(struct request_context));
    req->id         = id;
    req->exchange   = strdup(exchange);
    req->url        = strdup(url);
    req->timeout    = timeout;
    req->callback   = callback;

    return nw_job_add(job_context, 0, req);
}

