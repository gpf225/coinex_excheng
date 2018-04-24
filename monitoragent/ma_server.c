/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/06, create
 */

# include "ma_config.h"
# include "ma_server.h"

static rpc_clt *clt;
static http_svr *svr;
static dict_t *monitor;
static nw_timer timer;

struct monitor_key {
    char key[160];
};

struct monitor_val {
    uint64_t val;
};

static bool is_good_scope(const char *value)
{
    const size_t len = strlen(value);
    if (len == 0 || len > MAX_SCOPE_LENGTH)
        return false;
    for (size_t i = 0; i < len; ++i) {
        if (!(isalnum(value[i]) || value[i] == '.' || value[i] == '-' || value[i] == '_'))
            return false;
    }

    return true;
}

static bool is_good_key(const char *value)
{
    const size_t len = strlen(value);
    if (len == 0 || len > MAX_KEY_LENGTH)
        return false;
    for (size_t i = 0; i < len; ++i) {
        if (!(isalnum(value[i]) || value[i] == '.' || value[i] == '-' || value[i] == '_'))
            return false;
    }

    return true;
}

static bool is_good_host(const char *value)
{
    const size_t len = strlen(value);
    if (len == 0 || len > MAX_HOST_LENGTH)
        return false;
    for (size_t i = 0; i < len; ++i) {
        if (!(isalnum(value[i]) || value[i] == '.' || value[i] == '-' || value[i] == '_'))
            return false;
    }

    return true;
}

static int update_key_inc(struct monitor_key *mkey, uint64_t val)
{
    dict_entry *entry = dict_find(monitor, mkey);
    if (entry) {
        struct monitor_val *mval = entry->val;
        mval->val += val;
        return 0;
    }

    struct monitor_val mval = { .val = val };
    dict_add(monitor, mkey, &mval);

    return 0;
}

static int update_key_set(struct monitor_key *mkey, uint64_t val)
{
    dict_entry *entry = dict_find(monitor, mkey);
    if (entry) {
        struct monitor_val *mval = entry->val;
        mval->val = val;
        return 0;
    }

    struct monitor_val mval = { .val = val };
    dict_add(monitor, mkey, &mval);

    return 0;
}

static int reply_bad_request(nw_ses *ses)
{
    return send_http_response_simple(ses, 400, NULL, 0);
}

static int reply_internal_error(nw_ses *ses)
{
    return send_http_response_simple(ses, 500, NULL, 0);
}

static int reply_error(nw_ses *ses, int64_t id, int code, const char *message, uint32_t status)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));
    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(id));

    char *reply_str = json_dumps(reply, 0);
    int ret = send_http_response_simple(ses, status, reply_str, strlen(reply_str));
    free(reply_str);
    json_decref(reply);
    return ret;
}

static int reply_not_found(nw_ses *ses, int64_t id)
{
    return reply_error(ses, id, 4, "method not found", 404);
}

static int reply_result(nw_ses *ses, json_t *result, int64_t id)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(id));

    char *reply_str = json_dumps(reply, 0);
    int ret = send_http_response_simple(ses, 200, reply_str, strlen(reply_str));
    free(reply_str);
    json_decref(reply);
    return ret;
}

static int reply_success(nw_ses *ses, int64_t id)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, result, id);
    json_decref(result);
    return ret;
}

static int on_cmd_monitor_inc(nw_ses *ses, int64_t id, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_bad_request(ses);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_bad_request(ses);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_bad_request(ses);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return reply_bad_request(ses);
    uint64_t val = json_integer_value(json_array_get(params, 3));
    if (val > UINT32_MAX)
        return reply_bad_request(ses);

    struct monitor_key mkey;
    snprintf(mkey.key, sizeof(mkey.key), "%s:%s:%s", scope, key, host);

    int ret = update_key_inc(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return reply_internal_error(ses);
    }

    return reply_success(ses, id);
}

static int on_cmd_monitor_set(nw_ses *ses, int64_t id, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_bad_request(ses);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_bad_request(ses);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_bad_request(ses);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return reply_bad_request(ses);
    uint64_t val = json_integer_value(json_array_get(params, 3));
    if (val > UINT32_MAX)
        return reply_bad_request(ses);

    struct monitor_key mkey;
    snprintf(mkey.key, sizeof(mkey.key), "%s:%s:%s", scope, key, host);

    int ret = update_key_set(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return reply_internal_error(ses);
    }

    return reply_success(ses, id);
}

static int on_http_request(nw_ses *ses, http_request_t *request)
{
    log_trace("new http request, from: %s, url: %s, method: %u, body: %s",
            nw_sock_human_addr(&ses->peer_addr), request->url, request->method, request->body);
    if (request->method == HTTP_GET) {
        return send_http_response_simple(ses, 200, "ok\n", 3);
    } else {
        if (request->method != HTTP_POST || !request->body) {
            reply_bad_request(ses);
            return -__LINE__;
        }
    }

    json_t *body = json_loadb(request->body, sdslen(request->body), 0, NULL);
    if (body == NULL) {
        goto decode_error;
    }
    json_t *id = json_object_get(body, "id");
    if (!id || !json_is_integer(id)) {
        goto decode_error;
    }
    json_t *method = json_object_get(body, "method");
    if (!method || !json_is_string(method)) {
        goto decode_error;
    }
    json_t *params = json_object_get(body, "params");
    if (!params || !json_is_array(params)) {
        goto decode_error;
    }

    int64_t request_id = json_integer_value(id);
    const char *method_str = json_string_value(method);

    int ret;
    if (strcmp(method_str, "monitor.inc") == 0) {
        ret = on_cmd_monitor_inc(ses, request_id, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_inc fail: %d", ret);
        }
    } else if (strcmp(method_str, "monitor.set") == 0) {
        ret = on_cmd_monitor_set(ses, request_id, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_set fail: %d", ret);
        }
    } else {
        reply_not_found(ses, request_id);
    }

    json_decref(body);
    return 0;

decode_error:
    if (body)
        json_decref(body);
    sds hex = hexdump(request->body, sdslen(request->body));
    log_fatal("peer: %s, decode request fail, request body: \n%s", nw_sock_human_addr(&ses->peer_addr), hex);
    sdsfree(hex);
    reply_bad_request(ses);
    return -__LINE__;
}

int report_to_center(const char *key, uint64_t val)
{
    int token_count;
    sds *tokens = sdssplitlen(key, strlen(key), ":", 1, &token_count);
    if (token_count != 3)
        return -__LINE__;

    json_t *params = json_array();
    json_array_append_new(params, json_string(tokens[0]));
    json_array_append_new(params, json_string(tokens[1]));
    json_array_append_new(params, json_string(tokens[2]));
    json_array_append_new(params, json_integer(val));
    sdsfreesplitres(tokens, token_count);

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_MONITOR_INC;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    int ret = rpc_clt_send(clt, &pkg);
    free(pkg.body);

    return ret;
}

static int flush_dict(void)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(monitor);
    while ((entry = dict_next(iter)) != NULL) {
        struct monitor_key *k = entry->key;
        struct monitor_val *v = entry->val;
        if (v->val) {
            int ret = report_to_center(k->key, v->val);
            if (ret < 0) {
                log_error("report_to_center fail: %d, key: %s, val: %"PRIu64, ret, k->key, v->val);
            }
        }
        dict_delete(monitor, k);
    }
    dict_release_iterator(iter);

    return 0;
}

static int flush_data(void)
{
    double begin = current_timestamp();
    int ret = flush_dict();
    log_info("flush data success, cost time: %f, result: %d", current_timestamp() - begin, ret);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata)
{
    static time_t flush_last = 0;
    time_t now = time(NULL);
    if (now % 60 >= 55 && now - flush_last >= 60) {
        flush_last = now / 60 * 60 + 55;
        flush_data();
    }
}

static void on_clt_connect(nw_ses *ses, bool result)
{
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_clt_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("recv pkg from: %s, cmd: %u, body: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply);
    sdsfree(reply);
}

static uint32_t val_dict_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct monitor_key));
}

static int val_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct monitor_key));
}

static void *val_dict_key_dup(const void *key)
{
    struct monitor_key *obj = malloc(sizeof(struct monitor_key));
    memcpy(obj, key, sizeof(struct monitor_key));
    return obj;
}

static void val_dict_key_free(void *key)
{
    free(key);
}

static void *val_dict_val_dup(const void *val)
{
    struct monitor_val *obj = malloc(sizeof(struct monitor_val));
    memcpy(obj, val, sizeof(struct monitor_val));
    return obj;
}

static void val_dict_val_free(void *val)
{
    free(val);
}

int init_clt(void)
{
    rpc_clt_type type;
    memset(&type, 0, sizeof(type));
    type.on_connect = on_clt_connect;
    type.on_recv_pkg = on_clt_recv_pkg;

    clt = rpc_clt_create(&settings.center, &type);
    if (clt == NULL)
        return -__LINE__;
    if (rpc_clt_start(clt) < 0)
        return -__LINE__;

    return 0;
}

int init_svr(void)
{
    svr = http_svr_create(&settings.svr, on_http_request);
    if (svr == NULL)
        return -__LINE__;
    if (http_svr_start(svr) < 0)
        return -__LINE__;

    return 0;
}

int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = val_dict_hash_func;
    type.key_compare    = val_dict_key_compare;
    type.key_dup        = val_dict_key_dup;
    type.key_destructor = val_dict_key_free;
    type.val_dup        = val_dict_val_dup;
    type.val_destructor = val_dict_val_free;

    monitor = dict_create(&type, 64);
    if (monitor == NULL)
        return -__LINE__;

    return 0;
}

int init_server(void)
{
    ERR_RET(init_clt());
    ERR_RET(init_svr());
    ERR_RET(init_dict());

    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

void force_flush(void)
{
    flush_data();
}

