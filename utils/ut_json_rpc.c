/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ut_log.h"
# include "ut_rpc.h"
# include "ut_rpc_clt.h"
# include "ut_ws_svr.h"
# include "ut_http_svr.h"
# include "ut_json_rpc.h"

int rpc_request_json(rpc_clt *clt, uint32_t command, uint32_t sequence, uint64_t request_id, const json_t *params)
{
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type    = RPC_PKG_TYPE_REQUEST;
    pkg.command     = command;
    pkg.sequence    = sequence;
    pkg.req_id      = request_id;
    pkg.body        = json_dumps(params, 0);
    pkg.body_size   = strlen(pkg.body);

    int ret = rpc_clt_send(clt, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(clt)), command, sequence, (char *)pkg.body);
    free(pkg.body);
    return ret;
}

int rpc_push_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_str = json_dumps(json, 0);
    if (message_str == NULL)
        return -__LINE__;
    log_trace("connection: %s size: %zu, send: %s", nw_sock_human_addr(&ses->peer_addr), strlen(message_str), message_str);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type  = RPC_PKG_TYPE_PUSH;
    reply.body      = message_str;
    reply.body_size = strlen(message_str);

    int ret = rpc_send(ses, &reply);
    free(message_str);
    return ret;
}

int rpc_push_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *data = json_object();
    json_object_set_new(data, "error", error);
    json_object_set_new(data, "result", json_null());
    json_object_set_new(data, "id", json_null());

    int ret = rpc_push_json(ses, pkg, data);
    json_decref(data);
    return ret;
}

int rpc_reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_str = json_dumps(json, 0);
    if (message_str == NULL)
        return -__LINE__;
    log_trace("connection: %s size: %zu, send: %s", nw_sock_human_addr(&ses->peer_addr), strlen(message_str), message_str);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type  = RPC_PKG_TYPE_REPLY;
    reply.body      = message_str;
    reply.body_size = strlen(message_str);

    int ret = rpc_send(ses, &reply);
    free(message_str);
    return ret;
}

int rpc_reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = rpc_reply_json(ses, pkg, reply);
    json_decref(reply);
    return ret;
}

int rpc_reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg)
{
    return rpc_reply_error(ses, pkg, 1, "invalid argument");
}

int rpc_reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    return rpc_reply_error(ses, pkg, 2, "internal error");
}

int rpc_reply_error_service_unavailable(nw_ses *ses, rpc_pkg *pkg)
{
    return rpc_reply_error(ses, pkg, 3, "service unavailable");
}

int rpc_reply_error_service_timeout(nw_ses *ses, rpc_pkg *pkg)
{
    return rpc_reply_error(ses, pkg, 4, "service timeout");
}

int rpc_reply_error_unknown_command(nw_ses *ses, rpc_pkg *pkg)
{
    return rpc_reply_error(ses, pkg, 5, "unknown command");
}

int rpc_reply_error_require_auth(nw_ses *ses, rpc_pkg *pkg)
{
    return rpc_reply_error(ses, pkg, 6, "require auth");
}

int rpc_reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = rpc_reply_json(ses, pkg, reply);
    json_decref(reply);
    return ret;
}

int rpc_reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

int ws_send_json(nw_ses *ses, const json_t *json)
{
    char *message_str = json_dumps(json, 0);
    if (message_str == NULL)
        return -__LINE__;
    log_trace("connection: %s size: %zu, send: %s", nw_sock_human_addr(&ses->peer_addr), strlen(message_str), message_str);

    int ret = ws_send_text(ses, message_str);
    free(message_str);
    return ret;
}

int ws_send_error(nw_ses *ses, uint64_t id, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(id));

    int ret = ws_send_json(ses, reply);
    json_decref(reply);
    return ret;
}

int ws_send_error_invalid_argument(nw_ses *ses, uint64_t id)
{
    return ws_send_error(ses, id, 1, "invalid argument");
}

int ws_send_error_internal_error(nw_ses *ses, uint64_t id)
{
    return ws_send_error(ses, id, 2, "internal error");
}

int ws_send_error_service_unavailable(nw_ses *ses, uint64_t id)
{
    return ws_send_error(ses, id, 3, "service unavailable");
}

int ws_send_error_service_timeout(nw_ses *ses, uint64_t id)
{
    return ws_send_error(ses, id, 4, "service timeout");
}

int ws_send_error_unknown_method(nw_ses *ses, uint64_t id)
{
    return ws_send_error(ses, id, 5, "unknown method");
}

int ws_send_error_require_auth(nw_ses *ses, uint64_t id)
{
    return ws_send_error(ses, id, 6, "require auth");
}

int ws_send_result(nw_ses *ses, uint64_t id, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(id));

    int ret = ws_send_json(ses, reply);
    json_decref(reply);
    return ret;
}

int ws_send_success(nw_ses *ses, uint64_t id)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = ws_send_result(ses, id, result);
    json_decref(result);
    return ret;
}

int ws_send_notify(nw_ses *ses, const char *method, json_t *params)
{
    json_t *notify = json_object();
    json_object_set_new(notify, "method", json_string(method));
    json_object_set    (notify, "params", params);
    json_object_set_new(notify, "id", json_null());

    int ret = ws_send_json(ses, notify);
    json_decref(notify);
    return ret;
}

int http_reply_json(nw_ses *ses, json_t *json, uint32_t status)
{
    char *message_str = json_dumps(json, 0);
    if (message_str == NULL)
        return -__LINE__;
    size_t message_len = strlen(message_str);
    log_trace("connection: %s size: %zu, send: %s", nw_sock_human_addr(&ses->peer_addr), message_len, message_str);

    int ret = send_http_response_simple(ses, status, message_str, message_len);
    free(message_str);
    return ret;
}

int http_reply_error(nw_ses *ses, int64_t id, int code, const char *message, uint32_t status)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(id));

    int ret = http_reply_json(ses, reply, status);
    json_decref(reply);
    return ret;
}

int http_reply_error_bad_request(nw_ses *ses)
{
    return send_http_response_simple(ses, 400, NULL, 0);
}

int http_reply_error_invalid_argument(nw_ses *ses, int64_t id)
{
    return http_reply_error(ses, id, 1, "invalid argument", 400);
}

int http_reply_error_internal_error(nw_ses *ses, int64_t id)
{
    return http_reply_error(ses, id, 2, "internal error", 500);
}

int http_reply_error_service_unavailable(nw_ses *ses, int64_t id)
{
    return http_reply_error(ses, id, 3, "service unavailable", 500);
}

int http_reply_error_service_timeout(nw_ses *ses, int64_t id)
{
    return http_reply_error(ses, id, 4, "service timeout", 504);
}

int http_reply_error_not_found(nw_ses *ses, int64_t id)
{
    return http_reply_error(ses, id, 5, "not found", 404);
}

int http_reply_error_require_auth(nw_ses *ses, int64_t id)
{
    return http_reply_error(ses, id, 6, "require auth", 401);
}

int http_reply_result(nw_ses *ses, int64_t id, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(id));

    int ret = http_reply_json(ses, reply, 200);
    json_decref(reply);
    return ret;
}

int http_reply_success(nw_ses *ses, int64_t id)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = http_reply_result(ses, id, result);
    json_decref(result);
    return ret;
}

