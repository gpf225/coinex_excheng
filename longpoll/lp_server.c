/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# include "lp_server.h"
# include "lp_config.h"
# include "lp_market.h"
# include "lp_state.h"

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
        log_error("json_dumps failed, json maybe null, please check your code.");
        return -__LINE__;
    }
    log_trace("connection: %s send: %s", nw_sock_human_addr(&ses->peer_addr), message_data);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type = RPC_PKG_TYPE_REPLY;
    reply.body = message_data;
    reply.body_size = strlen(message_data);
    int ret = rpc_send(ses, &reply);
    free(message_data);

    return ret;
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
    profile_inc("error_invalid_argument", 1);
    return reply_error(ses, pkg, 1, "invalid argument");
}

//static int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
//{
//    profile_inc("error_internal_error", 1);
//    return reply_error(ses, pkg, 2, "internal error");
//}

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

int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result);
    json_decref(result);

    return ret;
}

int notify_message(nw_ses *ses, int command, json_t *message)
{
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.command = command;

    return reply_result(ses, &pkg, message);
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_info("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_info("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
    market_unsubscribe(ses);
    state_unsubscribe(ses);
}

static int on_method_market_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    market_subscribe(ses);
    int ret = reply_success(ses, pkg);
    if (ret != 0) {
        return ret;
    }
    return market_send_last(ses);
}

static int on_method_market_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    market_unsubscribe(ses);
    return reply_success(ses, pkg);
}

static int on_method_state_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{ 
    state_subscribe(ses);
    int ret = reply_success(ses, pkg);
    if (ret != 0) {
        return ret;
    }

    return state_send_last(ses);
}

static int on_method_state_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    state_unsubscribe(ses);
    return reply_success(ses, pkg);
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL) {
        reply_error_invalid_argument(ses, pkg);
        rpc_svr_close_clt(svr, ses);
        return ;
    }
    
    int ret = 0;
    switch (pkg->command) {
        case CMD_LP_MARKET_SUBSCRIBE:    
            ret = on_method_market_subscribe(ses, pkg, params);
            if (ret < 0) {
                log_error("on_method_market_subscribe fail: %d", ret);
            }
            break;   
        case CMD_LP_MARKET_UNSUBSCRIBE:    
            ret = on_method_market_unsubscribe(ses, pkg, params);
            if (ret < 0) {
                log_error("on_method_market_unsubscribe fail: %d", ret);
            }
            break;

        case CMD_LP_STATE_SUBSCRIBE:    
            ret = on_method_state_subscribe(ses, pkg, params);
            if (ret < 0) {
                log_error("on_method_state_subscribe fail: %d", ret);
            }
            break;   
        case CMD_LP_STATE_UNSUBSCRIBE:    
            ret = on_method_state_unsubscribe(ses, pkg, params);
            if (ret < 0) {
                log_error("on_method_state_unsubscribe fail: %d", ret);
            }
            break;

        default:
            log_error("command:%d not supported.", pkg->command);
            break;
    }

    if (ret != 0) {
        log_warn("handle failed, ret:%d", ret);
        rpc_svr_close_clt(svr, ses);
    }
    
    json_decref(params);
    return ;
}

int init_server(void)
{
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    assert(svr == NULL);
    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL) {
        printf("rpc_svr_create failed\n");
        return -__LINE__;
    }

    if (rpc_svr_start(svr) < 0) {
        printf("rpc_svr_start failed\n");
        return -__LINE__;
    }

    return 0;
}