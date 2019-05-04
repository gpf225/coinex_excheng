/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_depth.h"
# include "ca_market.h"
# include "ca_deals.h"
# include "ca_status.h"
# include "ca_server.h"
# include "ca_filter.h"

static rpc_svr *svr;
static nw_timer timer;

int push_data(nw_ses *ses, uint32_t command, char *data, size_t len)
{
    rpc_pkg reply;
    memset(&reply, 0, sizeof(reply));
    reply.command = command;
    reply.pkg_type = RPC_PKG_TYPE_PUSH;
    reply.body = data;
    reply.body_size = len;
    return rpc_send(ses, &reply);
}

int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_data;
    if (settings.debug) {
        message_data = json_dumps(json, JSON_INDENT(4));
    } else {
        message_data = json_dumps(json, 0);
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

int reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg)
{
    profile_inc("error_invalid_argument", 1);
    return reply_error(ses, pkg, 1, "invalid argument");
}

int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    profile_inc("error_internal_error", 1);
    return reply_error(ses, pkg, 2, "internal error");
}

int reply_time_out(nw_ses *ses, rpc_pkg *pkg)
{
    profile_inc("error_time_out", 1);
    return reply_error(ses, pkg, 3, "service timeout");
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

static int on_method_order_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    sds recv_str = NULL;
    if (json_array_size(params) != 3) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }
    if (!market_exist(market)) {
        log_error("market not exist, market: %s", market);
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    const char *interval = json_string_value(json_array_get(params, 2));
    if (interval == NULL) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    depth_request(ses, pkg, market, interval);
    return 0;

error:
    log_error("parameter error, recv_str: %s", recv_str);
    sdsfree(recv_str);
    return reply_error_invalid_argument(ses, pkg);
}

static int on_method_depth_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    sds recv_str = NULL;
    if (json_array_size(params) != 2) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    const char *interval = json_string_value(json_array_get(params, 1));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || interval == NULL || strlen(interval) >= INTERVAL_MAX_LEN) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    if (!market_exist(market)) {
        log_error("market not exist, market: %s", market);
    }

    depth_subscribe(ses, market, interval);
    return 0;

error:
    log_error("parameter error, recv_str: %s", recv_str);
    sdsfree(recv_str);
    return reply_error_invalid_argument(ses, pkg);
}

static int on_method_depth_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    sds recv_str = NULL;
    if (json_array_size(params) != 2) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    const char *interval = json_string_value(json_array_get(params, 1));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || interval == NULL || strlen(interval) >= INTERVAL_MAX_LEN) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    depth_unsubscribe(ses, market, interval);
    return 0;

error:
    log_error("parameter error, recv_str: %s", recv_str);
    sdsfree(recv_str);
    return reply_error_invalid_argument(ses, pkg);
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("connection: %s, cmd: %u decode params fail, params data: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        return ;
    }

    sds params_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("from: %s cmd: %u, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, params_str);

    int ret;
    switch (pkg->command) {
    case CMD_CACHE_DEPTH:
        profile_inc("cmd_cache_depth", 1);
        ret = on_method_order_depth(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_order_depth fail: %d", ret);
        }
        break;
    case CMD_CACHE_DEPTH_SUBSCRIBE:
        profile_inc("cmd_cache_depth_sub", 1);
        ret = on_method_depth_subscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_depth_subscribe fail: %d", ret);
        }
        break;
    case CMD_CACHE_DEPTH_UNSUBSCRIBE:
        profile_inc("cmd_cache_depth_unsub", 1);
        ret = on_method_depth_unsubscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_depth_unsubscribe fail: %d", ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

    json_decref(params);
    if (ret != 0)
        log_info("from: %s cmd: %u, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, params_str);
    sdsfree(params_str);

    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_info("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_info("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
    depth_unsubscribe_all(ses);
    clear_ses_filter(ses);
}

static void on_timer(nw_timer *timer, void *privdata)
{
    size_t count = depth_subscribe_number();
    profile_set("subscribe_depth", count);
    log_info("depth subscribe count: %zu", count);
}

int init_server(int worker_id)
{
    if (settings.svr.bind_count != 1)
        return -__LINE__;
    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET)
        return -__LINE__;
    bind_arr->addr.in.sin_port = htons(ntohs(bind_arr->addr.in.sin_port) + worker_id);

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

    nw_timer_set(&timer, 60, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

