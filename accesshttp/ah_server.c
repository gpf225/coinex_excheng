/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/21, create
 */

# include "ah_deals.h"
# include "ah_state.h"
# include "ah_cache.h"
# include "ah_config.h"
# include "ah_server.h"
# include "ah_message.h"

static http_svr *svr;
static nw_state *state;
static dict_t *methods;
static rpc_clt *listener;

static rpc_clt *matchengine;
static rpc_clt *marketprice;
static rpc_clt *marketindex;
static rpc_clt *tradesummary; 
static rpc_clt *readhistory;
static rpc_clt *monitorcenter;
static rpc_clt **cachecenter_clt_arr;

struct state_info {
    nw_ses  *ses;
    uint64_t ses_id;
    int64_t  request_id;
    sds      cache_key;
    int      depth_limit;
    uint32_t cmd;
};

struct request_info {
    rpc_clt *clt;
    uint32_t cmd;
};

static int check_depth_param(json_t *params)
{
    if (json_array_size(params) != 3)
        return -__LINE__;

    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL)
        return -__LINE__;

    const char *interval = json_string_value(json_array_get(params, 2));
    if (interval == NULL)
        return -__LINE__;

    return 0;
}

static rpc_clt *get_cache_clt(const char *market)
{
    uint32_t hash = dict_generic_hash_function(market, strlen(market));
    return cachecenter_clt_arr[hash % settings.cachecenter_worker_num];
}

static int on_http_request(nw_ses *ses, http_request_t *request)
{
    log_trace("new http request, url: %s, method: %u", request->url, request->method);
    if (request->method == HTTP_GET) {
        return send_http_response_simple(ses, 200, "ok\n", 3);
    } else {
        if (request->method != HTTP_POST || !request->body) {
            http_reply_error_bad_request(ses);
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
        http_reply_error_not_found(ses, json_integer_value(id));
    } else {
        struct request_info *req = entry->val;
        sds key = NULL;

        if (req->cmd == CMD_CACHE_DEPTH) {
            if (check_depth_param(params) != 0) {
                http_reply_error_invalid_argument(ses, json_integer_value(id));
                json_decref(body);
                return 0;
            }

            const char *market = json_string_value(json_array_get(params, 0));
            uint32_t limit = json_integer_value(json_array_get(params, 1));
            const char *interval = json_string_value(json_array_get(params, 2));

            key = sdsempty();
            key = sdscatprintf(key, "%u-%s-%s", req->cmd, market, interval);

            int ret = check_depth_cache(ses, json_integer_value(id), key, limit);
            if (ret > 0) {
                sdsfree(key);
                json_decref(body);
                return 0;
            }

            req->clt = get_cache_clt(market);
        } else if (req->cmd == CMD_MARKET_DEALS) {
            direct_deals_reply(ses, params, json_integer_value(id));
            json_decref(body);
            return 0;
        } else if (req->cmd == CMD_MARKET_STATUS && judege_state_period_is_day(params)) {
            direct_state_reply(ses, params, json_integer_value(id));
            json_decref(body);
            return 0;
        } else if (req->cmd == CMD_NOTICE_USER_MESSAGE) {
            json_t *message = json_array_get(params, 0);
            if (!message || !json_object_get(message, "user_id")) {
                http_reply_error_invalid_argument(ses, json_integer_value(id));
                json_decref(body);
                return 0;
            }

            push_notify_message(message);
            http_reply_success(ses, json_integer_value(id));
            json_decref(body);
            return 0;
        }

        if (!rpc_clt_connected(req->clt)) {
            http_reply_error_internal_error(ses, json_integer_value(id));
            json_decref(body);
            return 0;
        }

        profile_inc(json_string_value(method), 1);
        nw_state_entry *entry = nw_state_add(state, settings.timeout, 0);
        struct state_info *info = entry->data;
        info->ses = ses;
        info->ses_id = ses->id;
        info->request_id = json_integer_value(id);
        info->cache_key = key;
        info->cmd = req->cmd;
        if (req->cmd == CMD_CACHE_DEPTH) {
            info->depth_limit = json_integer_value(json_array_get(params, 1));
        }

        rpc_request_json(req->clt, req->cmd, entry->id, json_integer_value(id), params);
    }

    json_decref(body);
    return 0;

decode_error:
    if (body)
        json_decref(body);
    sds hex = hexdump(request->body, sdslen(request->body));
    log_fatal("peer: %s, decode request fail, request body: \n%s", nw_sock_human_addr(&ses->peer_addr), hex);
    sdsfree(hex);
    http_reply_error_bad_request(ses);
    return -__LINE__;
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

static int send_depth_http_response(struct state_info *state, rpc_pkg *pkg)
{
    int ret = 0;
    json_t *reply_json = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (!reply_json) {
        http_reply_error_internal_error(state->ses, state->request_id);
        goto clean;
    }
    json_t *result = json_object_get(reply_json, "result");
    if (!result) {
        http_reply_error_internal_error(state->ses, state->request_id);
        goto clean;
    }

    json_t *data = pack_depth_result(result, state->depth_limit);
    ret = http_reply_result(state->ses, state->request_id, data);
    json_decref(data);

clean:
    if (reply_json)
        json_decref(reply_json);
    return ret;
}

static void on_state_timeout(nw_state_entry *entry)
{
    struct state_info *info = entry->data;
    char buf[100];
    snprintf(buf, sizeof(buf), "on_timeout_cmd_%u", info->cmd);
    profile_inc(buf, 1);

    log_error("query timeout, state id: %u, command: %u", entry->id, info->cmd);
    if (info->ses->id == info->ses_id) {
        http_reply_error_service_timeout(info->ses, info->request_id);
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
    if (entry == NULL)
        return;

    struct state_info *info = entry->data;
    if (info->ses->id != info->ses_id) {
        nw_state_del(state, pkg->sequence);
        return;
    }

    log_trace("send response to: %s", nw_sock_human_addr(&info->ses->peer_addr));
    profile_inc("success", 1);

    if (pkg->command == CMD_CACHE_DEPTH) {
        send_depth_http_response(info, pkg);
    } else {
        send_http_response_simple(info->ses, 200, pkg->body, pkg->body_size);
    }

    json_t *reply = NULL;
    if (info->cache_key) {
        reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
        if (reply == NULL)
            goto end;
        json_t *result = json_object_get(reply, "result");
        if (!result || json_is_null(result))
            goto end;
        uint64_t ttl = json_integer_value(json_object_get(reply, "ttl"));
        if (ttl == 0)
            goto end;

        struct cache_val val;
        val.time_cache = current_millisecond() + ttl;
        val.result = result;
        json_incref(result);
        replace_cache(info->cache_key, &val);
    }

end:
    if (reply)
        json_decref(reply);
    nw_state_del(state, pkg->sequence);
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
    ERR_RET_LN(add_handler("asset.query_users", matchengine, CMD_ASSET_QUERY_USERS));
    ERR_RET_LN(add_handler("asset.query_all", matchengine, CMD_ASSET_QUERY_ALL));
    ERR_RET_LN(add_handler("asset.update", matchengine, CMD_ASSET_UPDATE));
    ERR_RET_LN(add_handler("asset.summary", matchengine, CMD_ASSET_SUMMARY));
    ERR_RET_LN(add_handler("asset.history", readhistory, CMD_ASSET_HISTORY));
    ERR_RET_LN(add_handler("asset.lock", matchengine, CMD_ASSET_LOCK));
    ERR_RET_LN(add_handler("asset.unlock", matchengine, CMD_ASSET_UNLOCK));
    ERR_RET_LN(add_handler("asset.query_lock", matchengine, CMD_ASSET_QUERY_LOCK));
    ERR_RET_LN(add_handler("asset.backup", matchengine, CMD_ASSET_BACKUP));

    ERR_RET_LN(add_handler("order.put_limit", matchengine, CMD_ORDER_PUT_LIMIT));
    ERR_RET_LN(add_handler("order.put_market", matchengine, CMD_ORDER_PUT_MARKET));
    ERR_RET_LN(add_handler("order.cancel", matchengine, CMD_ORDER_CANCEL));
    ERR_RET_LN(add_handler("order.cancel_all", matchengine, CMD_ORDER_CANCEL_ALL));
    ERR_RET_LN(add_handler("order.book", matchengine, CMD_ORDER_BOOK));
    ERR_RET_LN(add_handler("order.depth", NULL, CMD_CACHE_DEPTH));
    ERR_RET_LN(add_handler("order.pending", matchengine, CMD_ORDER_PENDING));
    ERR_RET_LN(add_handler("order.pending_detail", matchengine, CMD_ORDER_PENDING_DETAIL));
    ERR_RET_LN(add_handler("order.deals", readhistory, CMD_ORDER_DEALS));
    ERR_RET_LN(add_handler("order.finished", readhistory, CMD_ORDER_FINISHED));
    ERR_RET_LN(add_handler("order.finished_detail", readhistory, CMD_ORDER_FINISHED_DETAIL));
    ERR_RET_LN(add_handler("order.put_stop_limit", matchengine, CMD_ORDER_PUT_STOP_LIMIT));
    ERR_RET_LN(add_handler("order.put_stop_market", matchengine, CMD_ORDER_PUT_STOP_MARKET));
    ERR_RET_LN(add_handler("order.cancel_stop", matchengine, CMD_ORDER_CANCEL_STOP));
    ERR_RET_LN(add_handler("order.cancel_stop_all", matchengine, CMD_ORDER_CANCEL_STOP_ALL));
    ERR_RET_LN(add_handler("order.pending_stop", matchengine, CMD_ORDER_PENDING_STOP));
    ERR_RET_LN(add_handler("order.finished_stop", readhistory, CMD_ORDER_FINISHED_STOP));
    ERR_RET_LN(add_handler("order.stop_book", matchengine, CMD_ORDER_STOP_BOOK));
    ERR_RET_LN(add_handler("call.start", matchengine, CMD_CALL_AUCTION_START));
    ERR_RET_LN(add_handler("call.execute", matchengine, CMD_CALL_AUCTION_EXECUTE));

    ERR_RET_LN(add_handler("market.list", matchengine, CMD_MARKET_LIST));
    ERR_RET_LN(add_handler("market.summary", matchengine, CMD_MARKET_SUMMARY));
    ERR_RET_LN(add_handler("market.last", marketprice, CMD_MARKET_LAST));
    ERR_RET_LN(add_handler("market.kline", marketprice, CMD_MARKET_KLINE));
    ERR_RET_LN(add_handler("market.status", marketprice, CMD_MARKET_STATUS));
    ERR_RET_LN(add_handler("market.deals", marketprice, CMD_MARKET_DEALS));
    ERR_RET_LN(add_handler("market.deals_ext", marketprice, CMD_MARKET_DEALS_EXT));
    ERR_RET_LN(add_handler("market.user_deals", readhistory, CMD_MARKET_USER_DEALS));
    ERR_RET_LN(add_handler("market.self_deal", matchengine, CMD_MARKET_SELF_DEAL));

    ERR_RET_LN(add_handler("index.list", marketindex, CMD_INDEX_LIST));
    ERR_RET_LN(add_handler("index.query", marketindex, CMD_INDEX_QUERY));

    ERR_RET_LN(add_handler("monitor.inc", monitorcenter, CMD_MONITOR_INC));
    ERR_RET_LN(add_handler("monitor.set", monitorcenter, CMD_MONITOR_SET));
    ERR_RET_LN(add_handler("monitor.list_scope", monitorcenter, CMD_MONITOR_LIST_SCOPE));
    ERR_RET_LN(add_handler("monitor.list_key", monitorcenter, CMD_MONITOR_LIST_KEY));
    ERR_RET_LN(add_handler("monitor.list_host", monitorcenter, CMD_MONITOR_LIST_HOST));
    ERR_RET_LN(add_handler("monitor.query_minute", monitorcenter, CMD_MONITOR_QUERY));
    ERR_RET_LN(add_handler("monitor.query_daily", monitorcenter, CMD_MONITOR_DAILY));

    ERR_RET_LN(add_handler("config.update_asset", matchengine, CMD_CONFIG_UPDATE_ASSET));
    ERR_RET_LN(add_handler("config.update_market", matchengine, CMD_CONFIG_UPDATE_MARKET));
    ERR_RET_LN(add_handler("config.update_index", marketindex, CMD_CONFIG_UPDATE_INDEX));

    ERR_RET_LN(add_handler("trade.net_rank", tradesummary, CMD_TRADE_NET_RANK));
    ERR_RET_LN(add_handler("trade.amount_rank", tradesummary, CMD_TRADE_AMOUNT_RANK));

    ERR_RET_LN(add_handler("notice.user_message", NULL, CMD_NOTICE_USER_MESSAGE));
    return 0;
}

static void on_release(nw_state_entry *entry)
{
    struct state_info *state = entry->data;
    if (state->cache_key)
        sdsfree(state->cache_key);
}

static int init_cache_backend(rpc_clt_type *ct)
{
    cachecenter_clt_arr = malloc(sizeof(void *) * settings.cachecenter_worker_num);
    for (int i = 0; i < settings.cachecenter_worker_num; ++i) {
        char clt_name[100];
        snprintf(clt_name, sizeof(clt_name), "cachecenter_%d", i);
        char clt_addr[100];
        snprintf(clt_addr, sizeof(clt_addr), "tcp@%s:%d", settings.cachecenter_host, settings.cachecenter_port + i);

        rpc_clt_cfg cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.name = clt_name;
        cfg.addr_count = 1;
        cfg.addr_arr = malloc(sizeof(nw_addr_t));
        if (nw_sock_cfg_parse(clt_addr, &cfg.addr_arr[0], &cfg.sock_type) < 0)
            return -__LINE__;
        cfg.max_pkg_size = 1024 * 1024;

        cachecenter_clt_arr[i] = rpc_clt_create(&cfg, ct);
        if (cachecenter_clt_arr[i] == NULL)
            return -__LINE__;
        if (rpc_clt_start(cachecenter_clt_arr[i]) < 0)
            return -__LINE__;
    }

    return 0;
}

int init_server(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.val_dup        = dict_val_dup;
    dt.key_destructor = str_dict_key_free;
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

    marketindex = rpc_clt_create(&settings.marketindex, &ct);
    if (marketindex == NULL)
        return -__LINE__;
    if (rpc_clt_start(marketindex) < 0)
        return -__LINE__;

    tradesummary = rpc_clt_create(&settings.tradesummary, &ct);
    if (tradesummary == NULL)
        return -__LINE__;
    if (rpc_clt_start(tradesummary) < 0)
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

    if (init_cache_backend(&ct) < 0)
        return -__LINE__;

    svr = http_svr_create(&settings.svr, on_http_request);
    if (svr == NULL)
        return -__LINE__;

    ERR_RET(init_methods_handler());
    ERR_RET(init_listener_clt());

    return 0;
}

