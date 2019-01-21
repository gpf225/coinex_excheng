/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/01/18, create
 */

# include <curl/curl.h>

# include "aw_config.h"
# include "aw_server.h"
# include "aw_asset_sub.h"

static nw_job *job_context;
static nw_state *state_context;

struct state_data {
    nw_ses *ses;
    uint64_t ses_id;
    uint64_t request_id;
    struct clt_info *info;
};

typedef struct request_data_t {
    uint32_t user_id;
    json_t *sub_users;
}request_data_t;

static void request_data_free(request_data_t *request)
{
    if (request->sub_users != NULL) {
        json_decref(request->sub_users);
    }
}

static size_t post_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    request_data_t *data = entry->request;

    json_t *req_json = json_object();
    json_object_set_new(req_json, "user_id", json_integer(data->user_id));
    json_object_set    (req_json, "sub_users", data->sub_users);
    char *post_data = json_dumps(req_json, 0);
    json_decref(req_json);

    CURL *curl = curl_easy_init();
    sds reply = sdsempty();
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, "Accept-Language: en_US");
    chunk = curl_slist_append(chunk, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_URL, settings.auth_sub_url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data);   
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, post_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)(settings.backend_timeout * 1000));

    CURLcode ret = curl_easy_perform(curl);
    if (ret != CURLE_OK) {
        log_fatal("curl_easy_perform fail: %s", curl_easy_strerror(ret));
        goto cleanup;
    }

    json_t *result = json_loads(reply, 0, NULL);
    if (result == NULL) {
        goto cleanup;
    }
    entry->reply = result;

cleanup:
    free(post_data);
    curl_easy_cleanup(curl);
    sdsfree(reply);
    curl_slist_free_all(chunk);
}

static void on_result(struct state_data *state, request_data_t *data, json_t *result)
{
    if (state->ses->id != state->ses_id) {
        return;
    }
    if (result == NULL) {
        goto error;
    }

    json_t *code = json_object_get(result, "code");
    if (code == NULL) {
        goto error;
    }
    int error_code = json_integer_value(code);
    if (error_code != 0) {
        const char *message = json_string_value(json_object_get(result, "message"));
        if (message == NULL) {
            goto error;
        }
        char *sub_users = json_dumps(data->sub_users, 0);
        log_error("auth sub account fail, user_id: %u, code: %d, message: %s, sub users: %s", data->user_id, error_code, message, sub_users);
        free(sub_users);
        send_error(state->ses, state->request_id, 11, message);
        profile_inc("auth_sub_fail", 1);
        return;
    }

    json_t *users = json_object_get(result, "data");
    if (users == NULL || !json_is_array(users) || json_array_size(users) == 0) {
        goto error;
    }
    
    asset_unsubscribe_sub(state->ses);
    asset_subscribe_sub(state->ses, users);

    struct clt_info *info = state->info;
    log_info("auth sub user success, user_id: %u", info->user_id);
    send_success(state->ses, state->request_id);
    profile_inc("auth_sub_success", 1);

    return;

error:
    if (result) {
        char *reply = json_dumps(result, 0);
        log_fatal("invalid reply: %s", reply);
        free(reply);
    }
    send_error_internal_error(state->ses, state->request_id);
}

static void on_finish(nw_job_entry *entry)
{
    nw_state_entry *state = nw_state_get(state_context, entry->id);
    if (state == NULL) {
        return;
    }
    on_result(state->data, entry->request, entry->reply);
    nw_state_del(state_context, entry->id);
}

static void on_cleanup(nw_job_entry *entry)
{
    request_data_free(entry->request);
    if (entry->reply) {
        json_decref(entry->reply);
    }
}

static void on_timeout(nw_state_entry *entry)
{
    struct state_data *state = entry->data;
    if (state->ses->id == state->ses_id) {
        send_error_service_timeout(state->ses, state->request_id);
    }
}

int send_auth_sub_request(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params)
{
    if (json_array_size(params) != 1) {
        return send_error_invalid_argument(ses, id);
    }

    nw_state_entry *entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->request_id = id;
    state->info = info;

    log_trace("send auth_sub request, user_id:%u", info->user_id);

    request_data_t *data = malloc(sizeof(request_data_t));
    memset(data, 0, sizeof(request_data_t));
    data->user_id = info->user_id;
    data->sub_users = params;
    json_incref(data->sub_users);
    nw_job_add(job_context, entry->id, data);

    return 0;
}

int init_auth_sub(void)
{
    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_job = on_job;
    jt.on_finish = on_finish;
    jt.on_cleanup = on_cleanup;

    job_context = nw_job_create(&jt, 10);
    if (job_context == NULL)
        return -__LINE__;

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL)
        return -__LINE__;

    return 0;
}

size_t pending_auth_sub_request(void)
{
    return job_context->request_count;
}

