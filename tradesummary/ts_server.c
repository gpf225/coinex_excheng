/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "ts_config.h"
# include "ts_server.h"
# include "ts_message.h"

# define MAX_USER_LIST_LEN 1000

static rpc_svr *svr;

static int on_cmd_trade_net_rank(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market list
    json_t *market_list = json_array_get(params, 0);
    if (!json_is_array(market_list))
        return rpc_reply_error_invalid_argument(ses, pkg);
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        if (!json_is_string(json_array_get(market_list, i)))
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // start time
    time_t now = time(NULL);
    time_t start_time = json_integer_value(json_array_get(params, 1));
    time_t end_time = json_integer_value(json_array_get(params, 2));
    if (start_time <= 0 || end_time <= 0 || start_time > end_time)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (start_time < now - settings.keep_days * 86400)
        start_time = now - settings.keep_days * 86400;
    if (end_time > now)
        end_time = now;

    json_t *result = get_trade_net_rank(market_list, start_time, end_time);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_trade_amount_rank(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market list
    json_t *market_list = json_array_get(params, 0);
    if (!json_is_array(market_list))
        return rpc_reply_error_invalid_argument(ses, pkg);
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        if (!json_is_string(json_array_get(market_list, i)))
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // start time
    time_t now = time(NULL);
    time_t start_time = json_integer_value(json_array_get(params, 1));
    time_t end_time = json_integer_value(json_array_get(params, 2));
    if (start_time <= 0 || end_time <= 0 || start_time > end_time)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (start_time < now - settings.keep_days * 86400)
        start_time = now - settings.keep_days * 86400;
    if (end_time > now)
        end_time = now;

    json_t *result = get_trade_amount_rank(market_list, start_time, end_time);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);
    int ret = rpc_reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_trade_users_volume(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return rpc_reply_error_invalid_argument(ses, pkg);

    // market list
    json_t *market_list = json_array_get(params, 0);
    if (!json_is_array(market_list))
        return rpc_reply_error_invalid_argument(ses, pkg);
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        if (!json_is_string(json_array_get(market_list, i)))
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // user list
    json_t *user_list = json_array_get(params, 1);
    if (!json_is_array(user_list) || json_array_size(user_list) > MAX_USER_LIST_LEN)
        return rpc_reply_error_invalid_argument(ses, pkg);
    for (size_t i = 0; i < json_array_size(user_list); ++i) {
        if (!json_is_integer(json_array_get(user_list, i)))
            return rpc_reply_error_invalid_argument(ses, pkg);
    }

    // start time
    time_t now = time(NULL);
    time_t start_time = json_integer_value(json_array_get(params, 2));
    time_t end_time = json_integer_value(json_array_get(params, 3));
    if (start_time <= 0 || end_time <= 0 || start_time > end_time)
        return rpc_reply_error_invalid_argument(ses, pkg);
    if (start_time < now - settings.keep_days * 86400)
        start_time = now - settings.keep_days * 86400;
    if (end_time > now)
        end_time = now;

    json_t *result = get_trade_users_volume(market_list, user_list, start_time, end_time);
    if (result == NULL)
        return rpc_reply_error_internal_error(ses, pkg);
    int ret = rpc_reply_result(ses, pkg, result);
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
    case CMD_TRADE_NET_RANK:
        profile_inc("cmd_trade_net_rank", 1);
        ret = on_cmd_trade_net_rank(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_trade_net_rank %s fail: %d", params_str, ret);
        }
        break;
    case CMD_TRADE_AMOUNT_RANK:
        profile_inc("cmd_trade_amount_rank", 1);
        ret = on_cmd_trade_amount_rank(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_trade_amount_rank %s fail: %d", params_str, ret);
        }
        break;
    case CMD_TRADE_USERS_VOLUME:
        profile_inc("cmd_trade_users_volume", 1);
        ret = on_cmd_trade_users_volume(ses, pkg, params);
        if (ret < 0) {
            log_error("cmd_trade_users_volume %s fail: %d", params_str, ret);
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

