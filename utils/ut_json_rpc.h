/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _UT_JSON_RPC_
# define _UT_JSON_RPC_

# include <stddef.h>
# include <stdint.h>
# include <stdbool.h>
# include <jansson.h>

int rpc_request_json(rpc_clt *clt, uint32_t command, uint32_t sequence, 
    uint64_t request_id, const json_t *params);
int rpc_request_json_unique(rpc_clt *clt, uint32_t command, uint32_t sequence,
    uint64_t request_id, uint32_t unique_id, const json_t *params);
int rpc_push_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json);
int rpc_push_result(nw_ses *ses, uint32_t command, json_t *result);
int rpc_push_date(nw_ses *ses, uint32_t command, char *data, size_t len);
int rpc_push_error(nw_ses *ses, uint32_t command, int code, const char *message);
int rpc_reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json);
int rpc_reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message);
int rpc_reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg);
int rpc_reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg);
int rpc_reply_error_service_unavailable(nw_ses *ses, rpc_pkg *pkg);
int rpc_reply_error_service_timeout(nw_ses *ses, rpc_pkg *pkg);
int rpc_reply_error_unknown_command(nw_ses *ses, rpc_pkg *pkg);
int rpc_reply_error_require_auth(nw_ses *ses, rpc_pkg *pkg);
int rpc_reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result);
int rpc_reply_success(nw_ses *ses, rpc_pkg *pkg);

int ws_send_json(nw_ses *ses, const json_t *json);
int ws_send_error(nw_ses *ses, uint64_t id, int code, const char *message);
int ws_send_error_invalid_argument(nw_ses *ses, uint64_t id);
int ws_send_error_internal_error(nw_ses *ses, uint64_t id);
int ws_send_error_service_unavailable(nw_ses *ses, uint64_t id);
int ws_send_error_service_timeout(nw_ses *ses, uint64_t id);
int ws_send_error_unknown_method(nw_ses *ses, uint64_t id);
int ws_send_error_require_auth(nw_ses *ses, uint64_t id);
int ws_send_result(nw_ses *ses, uint64_t id, json_t *result);
int ws_send_success(nw_ses *ses, uint64_t id);
int ws_send_notify(nw_ses *ses, const char *method, json_t *params);
json_t *ws_get_notify(const char *method, json_t *params);

int http_reply_json(nw_ses *ses, json_t *json, uint32_t status);
int http_reply_error(nw_ses *ses, int64_t id, int code, const char *message, uint32_t status);
int http_reply_error_bad_request(nw_ses *ses);
int http_reply_error_invalid_argument(nw_ses *ses, int64_t id);
int http_reply_error_internal_error(nw_ses *ses, int64_t id);
int http_reply_error_service_unavailable(nw_ses *ses, int64_t id);
int http_reply_error_service_timeout(nw_ses *ses, int64_t id);
int http_reply_error_not_found(nw_ses *ses, int64_t id);
int http_reply_error_require_auth(nw_ses *ses, int64_t id);
int http_reply_error_result_null(nw_ses *ses, int64_t id);
int http_reply_result(nw_ses *ses, int64_t id, json_t *result);
int http_reply_success(nw_ses *ses, int64_t id);

# endif

