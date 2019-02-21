/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# include "ca_config.h"
# include "ca_depth_cache.h"
# include "ca_depth_update.h"

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

static json_t* get_json(struct depth_cache_val *cache_val, size_t limit)
{
    json_t *depth_data = cache_val->data;
    json_t *new_data = json_array();
    int size = json_array_size(depth_data) > limit ? limit : json_array_size(depth_data);
    for (int i = 0; i < size; ++i) {
        json_t *unit = json_array_get(depth_data, i);
        json_array_append(new_data, unit);
    }

    json_object_set(new_data, "last", json_object_get(depth_data, "last"));
    json_object_set(new_data, "time", json_object_get(depth_data, "time"));

    return new_data;
}

static int on_cmd_order_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3) {
        return reply_error_internal_error(ses, pkg);
    }
    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL) {
        return reply_error_internal_error(ses, pkg);
    }
    size_t limit = json_integer_value(json_array_get(params, 1));
    if (limit == 0) {
        return reply_error_internal_error(ses, pkg);
    }
    const char *interval = json_string_value(json_array_get(params, 2));
    if (interval == NULL) {
        return reply_error_internal_error(ses, pkg);
    }
   
    struct depth_cache_val *cache_val = depth_cache_get(market, interval, limit);
    if (cache_val == NULL) {
        depth_update(ses, pkg, market, interval, limit, true);
        return 0;
    }
    
    double now = current_timestamp();
    if ((now - cache_val->time) < (settings.cache_timeout * 2)) {
        json_t *reply_json = get_json(cache_val, limit);
        reply_result(ses, pkg, reply_json);
        json_decref(reply_json);
        profile_inc("depth_cache", 1);

        if ((now - cache_val->time) > settings.cache_timeout) {
            cache_val->time = now;
            limit = depth_cache_get_update_limit(cache_val, limit);
            depth_update(ses, pkg, market, interval, limit, false);
            profile_inc("depth_update", 1);
        }
    } else { 
        limit = depth_cache_get_update_limit(cache_val, limit);
        depth_update(ses, pkg, market, interval, limit, true);
        profile_inc("depth_update", 1);
        return 0;
    }

    return 0;
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

int init_server(int worker_id)
{
    if (settings.svr.bind_count != 1) {
        return -__LINE__;
    }

    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET) {
        return -__LINE__;
    }
    bind_arr->addr.in.sin_port = htons(ntohs(bind_arr->addr.in.sin_port) + worker_id);

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