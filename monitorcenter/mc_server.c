/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/06, create
 */

# include "mc_config.h"
# include "mc_server.h"

static rpc_svr *svr;
static redis_sentinel_t *redis;
static redisContext *redis_store;
static dict_t *key_set;
static dict_t *key_val;

struct monitor_key {
    char key[100];
    time_t timestamp;
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

static int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_data;
    if (settings.debug) {
        message_data = json_dumps(json, JSON_INDENT(4));
    } else {
        message_data = json_dumps(json, 0);
    }
    if (message_data == NULL)
        return -__LINE__;
    log_trace("connection: %s send: %s", nw_sock_human_addr(&ses->peer_addr), message_data);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type = RPC_PKG_TYPE_REPLY;
    reply.body = message_data;
    reply.body_size = strlen(message_data);
    rpc_send(ses, &reply);
    free(message_data);

    return 0;
}

static int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static int reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 1, "invalid argument");
}

static int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 2, "internal error");
}

static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

void *redis_query(const char *format, ...)
{
    for (int i = 0; i < 2; ++i) {
        if (redis_store == NULL) {
            log_info("redis connection lost, try connect");
            redis_store = redis_sentinel_connect_master(redis);
            if (redis_store == NULL) {
                log_error("redis_sentinel_connect_master fail");
                break;
            }
        }

        va_list ap;
        va_start(ap, format);
        redisReply *reply = redisvCommand(redis_store, format, ap);
        va_end(ap);

        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply == NULL) {
                log_error("redisvCommand fail: %d, %s", redis_store->err, strerror(errno));
            } else {
                log_error("redisvCommand fail: %d, %s, %s", redis_store->err, strerror(errno), reply->str);
                freeReplyObject(reply);
            }
            redisFree(redis_store);
            redis_store = NULL;
            continue;
        }

        return reply;
    }

    return NULL;
}

static int update_key_list(char *full_key, const char *scope, const char *key, const char *host)
{
    dict_entry *entry = dict_find(key_set, full_key);
    if (entry) {
        return 0;
    }

    log_info("key: %s not exist", full_key);
    redisReply *reply;

    reply = redis_query("SADD m:scopes %s", scope);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);

    reply = redis_query("SADD m:%s:keys %s", scope, key);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);

    reply = redis_query("SADD m:%s:%s:hosts %s", scope, key, host);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);

    dict_add(key_set, full_key, NULL);

    return 0;
}

static int update_key_inc(struct monitor_key *mkey, uint64_t val)
{
    dict_entry *entry = dict_find(key_val, mkey);
    if (entry) {
        struct monitor_val *mval = entry->val;
        mval->val += val;
        return 0;
    }

    struct monitor_val mval = { .val = val };
    dict_add(key_val, mkey, &mval);

    return 0;
}

static int update_key_set(struct monitor_key *mkey, uint64_t val)
{
    dict_entry *entry = dict_find(key_val, mkey);
    if (entry) {
        struct monitor_val *mval = entry->val;
        mval->val = val;
        return 0;
    }

    struct monitor_val mval = { .val = val };
    dict_add(key_val, mkey, &mval);

    return 0;
}

static int on_cmd_monitor_inc(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t val = json_integer_value(json_array_get(params, 3));

    struct monitor_key mkey;
    memset(&mkey, 0, sizeof(mkey));
    snprintf(mkey.key, sizeof(mkey.key), "%s:%s:%s", scope, key, host);
    mkey.timestamp = time(NULL) / 60 * 60;

    int ret;
    ret = update_key_list(mkey.key, scope, key, host);
    if (ret < 0) {
        log_error("update_key_list fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }
    ret = update_key_inc(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    return reply_success(ses, pkg);
}

static int on_cmd_monitor_set(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t val = json_integer_value(json_array_get(params, 3));

    struct monitor_key mkey;
    memset(&mkey, 0, sizeof(mkey));
    snprintf(mkey.key, sizeof(mkey.key), "%s:%s:%s", scope, key, host);
    mkey.timestamp = time(NULL) / 60 * 60;

    int ret;
    ret = update_key_list(mkey.key, scope, key, host);
    if (ret < 0) {
        log_error("update_key_list fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }
    ret = update_key_set(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    return reply_success(ses, pkg);
}

static int on_cmd_monitor_list_scope(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    redisReply *reply = redis_query("SMEMBERS m:scopes");
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 1) {
        json_array_append_new(result, json_string(reply->element[i]->str));
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_list_key(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);

    redisReply *reply = redis_query("SMEMBERS m:%s:keys", scope);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 1) {
        json_array_append_new(result, json_string(reply->element[i]->str));
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_list_host(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);

    redisReply *reply = redis_query("SMEMBERS m:%s:%s:hosts", scope, key);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 1) {
        json_array_append_new(result, json_string(reply->element[i]->str));
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || (strlen(host) > 0 && !is_good_host(host)))
        return reply_error_invalid_argument(ses, pkg);
    size_t points = json_integer_value(json_array_get(params, 3));
    if (points == 0)
        return reply_error_invalid_argument(ses, pkg);

    sds cmd = sdsempty();
    time_t start = time(NULL) / 60 * 60 - 60 * points;
    cmd = sdscatprintf(cmd, "HMGET m:%s:%s:%s:m", scope, key, host);
    for (size_t i = 0; i < points; ++i) {
        cmd = sdscatprintf(cmd, " %ld", start + i * 60);
    }

    redisReply *reply = redis_query(cmd);
    sdsfree(cmd);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 2) {
        time_t timestamp = strtol(reply->element[i]->str, NULL, 0);
        uint64_t value = strtoull(reply->element[i + 1]->str, NULL, 0);
        json_t *unit = json_array();
        json_array_append_new(unit, json_integer(timestamp));
        json_array_append_new(unit, json_integer(value));
        json_array_append_new(result, unit);
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_daily(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || (strlen(host) > 0 && !is_good_host(host)))
        return reply_error_invalid_argument(ses, pkg);
    size_t points = json_integer_value(json_array_get(params, 3));
    if (points == 0)
        return reply_error_invalid_argument(ses, pkg);

    sds cmd = sdsempty();
    time_t now = time(NULL);
    time_t start = get_day_start(now) - points * 86400;
    cmd = sdscatprintf(cmd, "HMGET m:%s:%s:%s:d", scope, key, host);
    for (size_t i = 0; i < points; ++i) {
        cmd = sdscatprintf(cmd, " %ld", start + i * 86400);
    }

    redisReply *reply = redis_query(cmd);
    sdsfree(cmd);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 2) {
        time_t timestamp = strtol(reply->element[i]->str, NULL, 0);
        uint64_t value = strtoull(reply->element[i + 1]->str, NULL, 0);
        json_t *unit = json_array();
        json_array_append_new(unit, json_integer(timestamp));
        json_array_append_new(unit, json_integer(value));
        json_array_append_new(result, unit);
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_MONITOR_INC:
        log_debug("from: %s cmd monitor inc, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_inc(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_inc %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_SET:
        log_debug("from: %s cmd monitor set, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_set(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_set %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_SCOPE:
        log_debug("from: %s cmd monitor list scope, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_scope(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_scope %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_KEY:
        log_debug("from: %s cmd monitor list key, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_key(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_key %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_HOST:
        log_debug("from: %s cmd monitor list host, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_host(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_QUERY:
        log_debug("from: %s cmd monitor query minute, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_DAILY:
        log_debug("from: %s cmd monitor query daily, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_daily(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

    sdsfree(params_str);
    json_decref(params);
    return;

decode_error:
    if (params) {
        json_decref(params);
    }
    sds hex = hexdump(pkg->body, pkg->body_size);
    log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", \
            nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
    sdsfree(hex);
    rpc_svr_close_clt(svr, ses);

    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static uint32_t set_dict_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen((char *)key));
}

static int set_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp((char *)key1, (char *)key2);
}

static void *set_dict_key_dup(const void *key)
{
    return strdup((char *)key);
}

static void set_dict_key_free(void *key)
{
    free(key);
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

static int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = set_dict_hash_func;
    type.key_compare    = set_dict_key_compare;
    type.key_dup        = set_dict_key_dup;
    type.key_destructor = set_dict_key_free;

    key_set = dict_create(&type, 64);
    if (key_set == NULL)
        return -__LINE__;

    memset(&type, 0, sizeof(type));
    type.hash_function  = val_dict_hash_func;
    type.key_compare    = val_dict_key_compare;
    type.key_dup        = val_dict_key_dup;
    type.key_destructor = val_dict_key_free;
    type.val_dup        = val_dict_val_dup;
    type.val_destructor = val_dict_val_free;

    key_val = dict_create(&type, 64);
    if (key_val == NULL)
        return -__LINE__;

    return 0;
}

int init_server(void)
{
    ERR_RET(init_dict());

    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    redis = redis_sentinel_create(&settings.redis);
    if (redis == NULL)
    redis_store = redis_sentinel_connect_master(redis);
    if (redis_store == NULL)
        return -__LINE__;

    return 0;
}

