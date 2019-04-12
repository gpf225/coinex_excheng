/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/21, create
 */

# include "ah_config.h"
# include "ah_server.h"
# include "ah_cache.h"

static http_svr *svr;
static nw_state *state;
static dict_t *methods;
static rpc_clt *listener;

static rpc_clt *matchengine;
static rpc_clt *marketprice;
static rpc_clt *readhistory;
static rpc_clt *monitorcenter;
static rpc_clt *cache;

struct state_info {
    nw_ses  *ses;
    uint64_t ses_id;
    int64_t  request_id;
    sds      cache_key;
};

struct request_info {
    rpc_clt *clt;
    uint32_t cmd;
};

static void reply_error(nw_ses *ses, int64_t id, int code, const char *message, uint32_t status)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));
    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(id));

    char *reply_str = json_dumps(reply, 0);
    send_http_response_simple(ses, status, reply_str, strlen(reply_str));
    free(reply_str);
    json_decref(reply);
}

static void reply_bad_request(nw_ses *ses)
{
    profile_inc("error_bad_request", 1);
    send_http_response_simple(ses, 400, NULL, 0);
}

static void reply_internal_error(nw_ses *ses)
{
    profile_inc("error_interval_error", 1);
    send_http_response_simple(ses, 500, NULL, 0);
}

static void reply_not_found(nw_ses *ses, int64_t id)
{
    profile_inc("error_not_found", 1);
    reply_error(ses, id, 4, "method not found", 404);
}

static void reply_time_out(nw_ses *ses, int64_t id)
{
    profile_inc("error_time_out", 1);
    reply_error(ses, id, 5, "service timeout", 504);
}

static int on_http_request(nw_ses *ses, http_request_t *request)
{
    log_trace("new http request, url: %s, method: %u", request->url, request->method);
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
    log_trace("from: %s body: %s", nw_sock_human_addr(&ses->peer_addr), request->body);

    dict_entry *entry = dict_find(methods, json_string_value(method));
    if (entry == NULL) {
        profile_inc("method_not_found", 1);
        reply_not_found(ses, json_integer_value(id));
    } else {
        struct request_info *req = entry->val;
        rpc_clt *clt = req->clt;

        if (!rpc_clt_connected(clt)) {
            reply_internal_error(ses);
            json_decref(body);
            return 0;
        }

        profile_inc(json_string_value(method), 1);
        sds key = sdsempty();

        if (req->cmd == CMD_CACHE_KLINE || req->cmd == CMD_CACHE_DEALS ||
                req->cmd == CMD_CACHE_STATUS || req->cmd == CMD_CACHE_DEPTH) {
            char *params_str = json_dumps(params, 0);
            key = sdscatprintf(key, "%u-%s", req->cmd, params_str);
            free(params_str);

            int ret;
            ret = check_cache(ses, json_integer_value(id), key, req->cmd, params);
            if (ret > 0) {
                sdsfree(key);
                json_decref(body);
                return 0;
            }
        }

        nw_state_entry *entry = nw_state_add(state, settings.timeout, 0);
        struct state_info *info = entry->data;
        info->ses = ses;
        info->ses_id = ses->id;
        info->request_id = json_integer_value(id);

        if (req->cmd == CMD_CACHE_KLINE || req->cmd == CMD_CACHE_DEALS ||
                req->cmd == CMD_CACHE_STATUS || req->cmd == CMD_CACHE_DEPTH) {
            info->cache_key = key;
        }

        rpc_pkg pkg;
        memset(&pkg, 0, sizeof(pkg));
        pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
        pkg.command   = req->cmd;
        pkg.sequence  = entry->id;
        pkg.req_id    = json_integer_value(id);
        pkg.body      = json_dumps(params, 0);
        pkg.body_size = strlen(pkg.body);

        rpc_clt_send(clt, &pkg);
        log_debug("send request to %s, cmd: %u, sequence: %u",
                nw_sock_human_addr(rpc_clt_peer_addr(clt)), pkg.command, pkg.sequence);
        free(pkg.body);
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

static uint32_t dict_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *dict_key_dup(const void *key)
{
    return strdup(key);
}

static void dict_key_free(void *key)
{
    free(key);
}

static void *dict_val_dup(const void *val)
{
    struct request_info *obj = malloc(sizeof(struct request_info));
    memcpy(obj, val, sizeof(struct request_info));
    return obj;
}

static void dict_val_free(void *val)
{
    free(val);
}

static void on_state_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
    struct state_info *info = entry->data;
    if (info->ses->id == info->ses_id) {
        reply_time_out(info->ses, info->request_id);
    }
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    log_debug("recv pkg from: %s, cmd: %u, sequence: %u",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence);
    nw_state_entry *entry = nw_state_get(state, pkg->sequence);
    if (entry) {
        struct state_info *info = entry->data;
        if (info->ses->id == info->ses_id) {
            log_trace("send response to: %s", nw_sock_human_addr(&info->ses->peer_addr));
            if (pkg->command == CMD_CACHE_KLINE || pkg->command == CMD_CACHE_DEALS ||
                    pkg->command == CMD_CACHE_STATUS || pkg->command == CMD_CACHE_DEPTH) {
                json_t *reply_json = json_loadb(pkg->body, pkg->body_size, 0, NULL);
                json_t *cache_result = json_object_get(reply_json, "cache_result");

                if (reply_json == NULL || cache_result == NULL) {
                    log_error("cache_result null");
                    reply_internal_error(ses);
                    nw_state_del(state, pkg->sequence);
                    if (reply_json)
                        json_decref(reply_json);
                    return;
                }

                char *reply_str = json_dumps(cache_result, 0);
                send_http_response_simple(info->ses, 200, reply_str, strlen(reply_str));
                profile_inc("success", 1);
                free(reply_str);

                if (info->cache_key) {
                    json_t *error = json_object_get(cache_result, "error");
                    json_t *result = json_object_get(cache_result, "result");

                    if (error && json_is_null(error) && result && !json_is_null(result)) {
                        int ttl = json_integer_value(json_object_get(reply_json, "ttl"));
                        struct cache_val val;
                        val.time_exp = current_millis() + ttl;
                        val.result = result;
                        json_incref(result);
                        dict_replace_cache(info->cache_key, &val);
                    }
                }
                json_decref(reply_json);
            } else {
                send_http_response_simple(info->ses, 200, pkg->body, pkg->body_size);
                profile_inc("success", 1);
            }
        }
        nw_state_del(state, pkg->sequence);
    }
}

static void on_listener_connect(nw_ses *ses, bool result)
{
    if (result) {
        log_info("connect listener success");
    } else {
        log_info("connect listener fail");
    }
}

static void on_listener_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    return;
}

static void on_listener_recv_fd(nw_ses *ses, int fd)
{
    if (nw_svr_add_clt_fd(svr->raw_svr, fd) < 0) {
        log_error("nw_svr_add_clt_fd: %d fail: %s", fd, strerror(errno));
        close(fd);
        profile_inc("new_connection_fail", 1);
    } else {
        profile_inc("new_connection_success", 1);
    }
}

static int init_listener_clt(void)
{
    rpc_clt_cfg cfg;
    nw_addr_t addr;
    memset(&cfg, 0, sizeof(cfg));
    cfg.name = strdup("listener");
    cfg.addr_count = 1;
    cfg.addr_arr = &addr;
    if (nw_sock_cfg_parse(AH_LISTENER_BIND, &addr, &cfg.sock_type) < 0)
        return -__LINE__;
    cfg.max_pkg_size = 1024;

    rpc_clt_type type;
    memset(&type, 0, sizeof(type));
    type.on_connect  = on_listener_connect;
    type.on_recv_pkg = on_listener_recv_pkg;
    type.on_recv_fd  = on_listener_recv_fd;

    listener = rpc_clt_create(&cfg, &type);
    if (listener == NULL)
        return -__LINE__;
    if (rpc_clt_start(listener) < 0)
        return -__LINE__;

    return 0;
}

static int add_handler(char *method, rpc_clt *clt, uint32_t cmd)
{
    struct request_info info = { .clt = clt, .cmd = cmd };
    if (dict_add(methods, method, &info) == NULL)
        return __LINE__;
    return 0;
}

static int init_methods_handler(void)
{
    ERR_RET_LN(add_handler("asset.list", matchengine, CMD_ASSET_LIST));
    ERR_RET_LN(add_handler("asset.query", matchengine, CMD_ASSET_QUERY));
    ERR_RET_LN(add_handler("asset.update", matchengine, CMD_ASSET_UPDATE));
    ERR_RET_LN(add_handler("asset.history", readhistory, CMD_ASSET_HISTORY));
    ERR_RET_LN(add_handler("asset.lock", matchengine, CMD_ASSET_LOCK));
    ERR_RET_LN(add_handler("asset.unlock", matchengine, CMD_ASSET_UNLOCK));
    ERR_RET_LN(add_handler("asset.query_lock", matchengine, CMD_ASSET_QUERY_LOCK));
    ERR_RET_LN(add_handler("asset.backup", matchengine, CMD_ASSET_BACKUP));

    ERR_RET_LN(add_handler("order.put_limit", matchengine, CMD_ORDER_PUT_LIMIT));
    ERR_RET_LN(add_handler("order.put_market", matchengine, CMD_ORDER_PUT_MARKET));
    ERR_RET_LN(add_handler("order.cancel", matchengine, CMD_ORDER_CANCEL));
    ERR_RET_LN(add_handler("order.book", matchengine, CMD_ORDER_BOOK));
    ERR_RET_LN(add_handler("order.depth", cache, CMD_CACHE_DEPTH));
    ERR_RET_LN(add_handler("order.pending", matchengine, CMD_ORDER_PENDING));
    ERR_RET_LN(add_handler("order.pending_detail", matchengine, CMD_ORDER_PENDING_DETAIL));
    ERR_RET_LN(add_handler("order.deals", readhistory, CMD_ORDER_DEALS));
    ERR_RET_LN(add_handler("order.finished", readhistory, CMD_ORDER_FINISHED));
    ERR_RET_LN(add_handler("order.finished_detail", readhistory, CMD_ORDER_FINISHED_DETAIL));
    ERR_RET_LN(add_handler("order.put_stop_limit", matchengine, CMD_ORDER_PUT_STOP_LIMIT));
    ERR_RET_LN(add_handler("order.put_stop_market", matchengine, CMD_ORDER_PUT_STOP_MARKET));
    ERR_RET_LN(add_handler("order.cancel_stop", matchengine, CMD_ORDER_CANCEL_STOP));
    ERR_RET_LN(add_handler("order.pending_stop", matchengine, CMD_ORDER_PENDING_STOP));
    ERR_RET_LN(add_handler("order.finished_stop", readhistory, CMD_ORDER_FINISHED_STOP));
    ERR_RET_LN(add_handler("order.stop_book", matchengine, CMD_ORDER_STOP_BOOK));

    ERR_RET_LN(add_handler("market.list", matchengine, CMD_MARKET_LIST));
    ERR_RET_LN(add_handler("market.last", marketprice, CMD_MARKET_LAST));
    ERR_RET_LN(add_handler("market.kline", cache, CMD_CACHE_KLINE));
    ERR_RET_LN(add_handler("market.status", cache, CMD_CACHE_STATUS));
    ERR_RET_LN(add_handler("market.deals", cache, CMD_CACHE_DEALS));
    ERR_RET_LN(add_handler("market.deals_ext", marketprice, CMD_MARKET_DEALS_EXT));
    ERR_RET_LN(add_handler("market.user_deals", readhistory, CMD_MARKET_USER_DEALS));

    ERR_RET_LN(add_handler("monitor.inc", monitorcenter, CMD_MONITOR_INC));
    ERR_RET_LN(add_handler("monitor.set", monitorcenter, CMD_MONITOR_SET));
    ERR_RET_LN(add_handler("monitor.list_scope", monitorcenter, CMD_MONITOR_LIST_SCOPE));
    ERR_RET_LN(add_handler("monitor.list_key", monitorcenter, CMD_MONITOR_LIST_KEY));
    ERR_RET_LN(add_handler("monitor.list_host", monitorcenter, CMD_MONITOR_LIST_HOST));
    ERR_RET_LN(add_handler("monitor.query_minute", monitorcenter, CMD_MONITOR_QUERY));
    ERR_RET_LN(add_handler("monitor.query_daily", monitorcenter, CMD_MONITOR_DAILY));

    ERR_RET_LN(add_handler("config.update_asset", matchengine, CMD_CONFIG_UPDATE_ASSET));
    ERR_RET_LN(add_handler("config.update_market", matchengine, CMD_CONFIG_UPDATE_MARKET));

    return 0;
}

static void on_release(nw_state_entry *entry)
{
    struct state_info *state = entry->data;
    if (state->cache_key)
        sdsfree(state->cache_key);
}

int init_server(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_hash_func;
    dt.key_compare = dict_key_compare;
    dt.key_dup = dict_key_dup;
    dt.val_dup = dict_val_dup;
    dt.key_destructor = dict_key_free;
    dt.val_destructor = dict_val_free;
    methods = dict_create(&dt, 64);
    if (methods == NULL)
        return -__LINE__;

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_state_timeout;
    st.on_release = on_release;
    state = nw_state_create(&st, sizeof(struct state_info));
    if (state == NULL)
        return -__LINE__;

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;
    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL)
        return -__LINE__;
    if (rpc_clt_start(matchengine) < 0)
        return -__LINE__;

    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL)
        return -__LINE__;
    if (rpc_clt_start(marketprice) < 0)
        return -__LINE__;

    readhistory = rpc_clt_create(&settings.readhistory, &ct);
    if (readhistory == NULL)
        return -__LINE__;
    if (rpc_clt_start(readhistory) < 0)
        return -__LINE__;

    monitorcenter = rpc_clt_create(&settings.monitorcenter, &ct);
    if (monitorcenter == NULL)
        return -__LINE__;
    if (rpc_clt_start(monitorcenter) < 0)
        return -__LINE__;

    cache = rpc_clt_create(&settings.cache, &ct);
    if (cache == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache) < 0)
        return -__LINE__;

    svr = http_svr_create(&settings.svr, on_http_request);
    if (svr == NULL)
        return -__LINE__;

    ERR_RET(init_methods_handler());
    ERR_RET(init_listener_clt());

    return 0;
}

