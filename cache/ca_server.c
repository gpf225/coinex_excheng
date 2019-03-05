/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# include "ca_config.h"
# include "ca_depth_cache.h"
# include "ca_depth_update.h"
# include "ca_depth_sub.h"
# include "ca_common.h"

static rpc_svr *svr = NULL;

static int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_data;
    if (settings.debug) {
        message_data = json_dumps(json, JSON_INDENT(4));
    } else {
        message_data = json_dumps(json, 0);
    }
    if (message_data == NULL) {
        return -__LINE__;
    }
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

int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
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
    profile_inc("error_invalid_argument", 1);
    return reply_error(ses, pkg, 1, "invalid argument");
}

int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 2, "internal error");
}

int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

int notify_message(nw_ses *ses, int command, json_t *message)
{
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.command = command;

    return reply_result(ses, &pkg, message);
}

static int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result);
    json_decref(result);

    return ret;
}

static int on_cmd_order_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3) {
        return reply_error_internal_error(ses, pkg);
    }
    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL) {
        return reply_error_invalid_argument(ses, pkg);
    }
    uint32_t limit = json_integer_value(json_array_get(params, 1));
    if (limit == 0) {
        return reply_error_invalid_argument(ses, pkg);
    }
    const char *interval = json_string_value(json_array_get(params, 2));
    if (interval == NULL) {
        return reply_error_invalid_argument(ses, pkg);
    }
   
    struct depth_cache_val *cache_val = depth_cache_get(market, interval, limit);
    if (cache_val != NULL) {
        json_t *reply_json = depth_get_result_rest(cache_val->data, cache_val->limit, limit, cache_val->ttl);
        reply_result(ses, pkg, reply_json);
        json_decref(reply_json);
        profile_inc("depth_cache", 1);
        return 0;
    }
    
    limit = depth_cache_get_update_limit(market, interval, limit);
    depth_update(ses, pkg, market, interval, limit);
    return 0;
}

static int on_method_depth_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t params_len = json_array_size(params);
    if (params_len < 1) {
        return reply_error_invalid_argument(ses, pkg);
    }

    for (size_t i = 0; i < params_len; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        const char *interval = json_string_value(json_array_get(item, 1));
        const int limit = json_integer_value(json_array_get(item, 2));

        if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || limit <= 0 || interval == NULL || strlen(interval) >= INTERVAL_MAX_LEN) {
            log_warn("market[%s] interval[%s] limit[%d] not valid, can not been subscribed", market, interval, limit);
            return reply_error_invalid_argument(ses, pkg);
        }
    }

    for (size_t i = 0; i < params_len; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        const char *interval = json_string_value(json_array_get(item, 1));
        int limit = json_integer_value(json_array_get(item, 2));
        int ret = depth_subscribe(ses, market, interval, limit);
        if (ret != 0) {
            log_warn("subscribe %s-%s-%d failed.", market, interval, limit);
            continue;  // 忽略该错误，继续执行
        }
    }

    return reply_success(ses, pkg);
}

static int on_method_depth_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t params_len = json_array_size(params);
    if (params_len < 1) {
        depth_unsubscribe_all(ses);
        return reply_success(ses, pkg);
    }

    for (size_t i = 0; i < params_len; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        const char *interval = json_string_value(json_array_get(item, 1));
        const int limit = json_integer_value(json_array_get(item, 2));

        if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || limit <= 0 || interval == NULL || strlen(interval) >= INTERVAL_MAX_LEN) {
            log_warn("market[%s] interval[%s] limit[%d] not valid, can not been subscribed", market, interval, limit);
            return reply_error_invalid_argument(ses, pkg);
        }
    }

    for (size_t i = 0; i < params_len; ++i) {
        json_t *item = json_array_get(params, i);
        const char *market = json_string_value(json_array_get(item, 0));
        const char *interval = json_string_value(json_array_get(item, 1));
        const int limit = json_integer_value(json_array_get(item, 2));

        int ret = depth_unsubscribe(ses, market, interval, limit);
        if (ret != 0) {
            log_warn("unsubscribe %s-%s-%d failed.", market, interval, limit);
            continue;  // 忽略该错误，继续执行
        }
    }

    return reply_success(ses, pkg);
}


static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        return ;
    }

    sds params_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("from: %s cmd: %u, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, params_str);

    int ret;
    switch (pkg->command) {
    case CMD_ORDER_DEPTH:
        profile_inc("on_cmd_order_depth", 1);
        ret = on_cmd_order_depth(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_depth %s fail: %d", params_str, ret);
        }
        break;
    case CMD_LP_DEPTH_SUBSCRIBE:    
        ret = on_method_depth_subscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_depth_subscribe fail: %d", ret);
        }
        break;  
    case CMD_LP_DEPTH_UNSUBSCRIBE:    
        ret = on_method_depth_unsubscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_depth_unsubscribe fail: %d", ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

    sdsfree(params_str);
    json_decref(params);

    if (ret != 0) {
        rpc_svr_close_clt(svr, ses); 
    }
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

int init_server(void)
{
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL) {
        return -__LINE__;
    }
    if (rpc_svr_start(svr) < 0) {
        return -__LINE__;
    }

    return 0;
}