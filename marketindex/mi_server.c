/*
 * Description: 
 *     History: ouxiangyang@viabtc.com, 2018/10/15, create
 */

# include "mi_server.h"
# include "mi_index.h"

static rpc_svr *svr;

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

static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result, double ttl)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));
    if (ttl > 0) {
        json_object_set_new(reply, "ttl", json_integer((int)(ttl * 1000)));
    }

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

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

static int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    profile_inc("error_internal_error", 1);
    return reply_error(ses, pkg, 2, "internal error");
}

int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    profile_inc("success", 1);
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result, 0);
    json_decref(result);
    return ret;
}

static int on_cmd_index_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = get_market_list();
    if (result == NULL)
        return reply_error_internal_error(ses, pkg);

    int ret = reply_result(ses, pkg, result, 1);
    json_decref(result);
    return ret;
}

static int on_cmd_index_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    if (!market_exists(market_name))
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = get_market_index(market_name);
    if (result == NULL)
        return reply_error_internal_error(ses, pkg);

    int ret = reply_result(ses, pkg, result, 1);
    json_decref(result);
    return ret;
}

static int on_cmd_update_config(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    int ret;
    ret = update_index_config();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);
    ret = reload_index_config();
    if (ret < 0)
        return reply_error_internal_error(ses, pkg);

    return reply_success(ses, pkg);
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("from: %s cmd: %u, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, params_str);

    int ret;
    switch (pkg->command) {
    case CMD_INDEX_LIST:
        profile_inc("index_list", 1);
        ret = on_cmd_index_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_index_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_INDEX_QUERY:
        profile_inc("index_query", 1);
        ret = on_cmd_index_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_index_query %s fail: %d", params_str, ret);
        }
        break;
    case CMD_CONFIG_UPDATE_INDEX:
        profile_inc("index_config", 1);
        ret = on_cmd_update_config(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_update_config %s fail: %d", params_str, ret);
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

int init_server(void)
{
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

    return 0;
}

