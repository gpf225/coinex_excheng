/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/06, create
 */

# include "ma_config.h"
# include "ma_server.h"

static nw_svr *svr;
static rpc_clt *clt;
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

static int on_cmd_monitor_inc(json_t *params)
{
    if (json_array_size(params) != 4)
        return -__LINE__;
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return -__LINE__;
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return -__LINE__;
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return -__LINE__;
    uint64_t val = json_integer_value(json_array_get(params, 3));
    if (val > UINT32_MAX)
        return -__LINE__;

    struct monitor_key mkey;
    snprintf(mkey.key, sizeof(mkey.key), "%s_%s_%s", scope, key, host);

    int ret = update_key_inc(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return -__LINE__;
    }

    return 0;
}

static int on_cmd_monitor_set(json_t *params)
{
    if (json_array_size(params) != 4)
        return -__LINE__;
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return -__LINE__;
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return -__LINE__;
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return -__LINE__;
    uint64_t val = json_integer_value(json_array_get(params, 3));
    if (val > UINT32_MAX)
        return -__LINE__;

    struct monitor_key mkey;
    snprintf(mkey.key, sizeof(mkey.key), "%s_%s_%s", scope, key, host);

    int ret = update_key_set(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return -__LINE__;
    }

    return 0;
}

static int on_svr_decode_pkg(nw_ses *ses, void *data, size_t max)
{
    char *s = data;
    for (size_t i = 0; i < max; ++i) {
        if (s[i] == '\n')
            return i + 1;
    }
    return 0;
}

static void on_svr_recv_pkg(nw_ses *ses, void *data, size_t size)
{
    sds message_str = sdsnewlen(data, size);

    json_t *message = json_loadb(data, size, 0, NULL);
    if (message == NULL) {
        goto decode_error;
    }
    json_t *method = json_object_get(message, "method");
    if (!method || !json_is_string(method)) {
        goto decode_error;
    }
    json_t *params = json_object_get(message, "params");
    if (!params || !json_is_array(params)) {
        goto decode_error;
    }

    log_trace("new request from: %s, message: %s", nw_sock_human_addr(&ses->peer_addr), message_str);
    const char *method_str = json_string_value(method);

    int ret;
    if (strcmp(method_str, "monitor.inc") == 0) {
        ret = on_cmd_monitor_inc(params);
        if (ret < 0) {
            log_error("on_cmd_monitor_inc fail: %d", ret);
        }
    } else if (strcmp(method_str, "monitor.set") == 0) {
        ret = on_cmd_monitor_set(params);
        if (ret < 0) {
            log_error("on_cmd_monitor_set fail: %d", ret);
        }
    } else {
        log_error("unknown request: %s", message_str);
    }

    json_decref(message);
    sdsfree(message_str);
    return;

decode_error:
    if (message)
        json_decref(message);
    sdsfree(message_str);
    sds hex = hexdump(data, size);
    log_fatal("peer: %s, decode request fail, request message: \n%s", nw_sock_human_addr(&ses->peer_addr), hex);
    sdsfree(hex);
    return;
}

static void on_svr_error_msg(nw_ses *ses, const char *msg)
{
    if (ses->ses_type == NW_SES_TYPE_COMMON) {
        log_error("connection: %"PRIu64":%s error: %s", ses->id, nw_sock_human_addr(&ses->peer_addr), msg);
    } else {
        log_error("connection: %"PRIu64":%s error: %s", ses->id, nw_sock_human_addr(ses->host_addr), msg);
    }
}

int report_to_center(const char *key, uint64_t val)
{
    int token_count;
    sds *tokens = sdssplitlen(key, strlen(key), "_", 1, &token_count);
    if (token_count != 3) {
        sdsfreesplitres(tokens, token_count);
        return -__LINE__;
    }

    json_t *params = json_array();
    json_array_append_new(params, json_string(tokens[0]));
    json_array_append_new(params, json_string(tokens[1]));
    json_array_append_new(params, json_string(tokens[2]));
    json_array_append_new(params, json_integer(val));
    sdsfreesplitres(tokens, token_count);

    int ret = rpc_request_json(clt, CMD_MONITOR_INC, 0, 0, params);
    json_decref(params);

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

int init_svr(void)
{
    nw_svr_type type;
    memset(&type, 0, sizeof(type));
    type.decode_pkg = on_svr_decode_pkg;
    type.on_recv_pkg = on_svr_recv_pkg;
    type.on_error_msg = on_svr_error_msg;

    svr = nw_svr_create(&settings.svr, &type, NULL);
    if (svr == NULL)
        return -__LINE__;
    if (nw_svr_start(svr) < 0)
        return -__LINE__;

    return 0;
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

