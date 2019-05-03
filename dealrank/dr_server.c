/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "dr_config.h"
# include "dr_server.h"
# include "dr_message.h"
# include "dr_deal.h"

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

static int on_cmd_deal_rank_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 5)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    time_t start = json_integer_value(json_array_get(params, 1));

    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    time_t end = json_integer_value(json_array_get(params, 2));

    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t data_type = json_integer_value(json_array_get(params, 3));

    if (!json_is_integer(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t top_num = json_integer_value(json_array_get(params, 4));

    json_t *result = NULL; 
    int ret = deal_top_market(&result, start, end, market, data_type, top_num);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "time invalid");
    } else if (ret < 0) {
        log_fatal("deal_top_data fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
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
    case CMD_DEAL_RANK_MARKET:
        profile_inc("cmd_deal_rank_market", 1);
        ret = on_cmd_deal_rank_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_deal_rank_market %s fail: %d", params_str, ret);
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
