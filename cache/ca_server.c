/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_cache.h"
# include "ca_depth.h"
# include "ca_kline.h"
# include "ca_market.h"
# include "ca_deals.h"
# include "ca_status.h"
# include "ca_server.h"

static rpc_svr *svr;

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
    profile_inc("invalid_argument", 1);
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
    if (json_array_size(params) != 3)
        return reply_error_internal_error(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    uint32_t limit = json_integer_value(json_array_get(params, 1));

    const char *interval = json_string_value(json_array_get(params, 2));
    if (interval == NULL)
        return reply_error_invalid_argument(ses, pkg);

    depth_request(ses, pkg, market, limit, interval);
    return 0;
}

static int on_method_depth_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    const char *interval = json_string_value(json_array_get(params, 1));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || interval == NULL || strlen(interval) >= INTERVAL_MAX_LEN) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    if (!market_exist(market)) {
        log_error("market not exist, market: %s", market);
        return reply_error_invalid_argument(ses, pkg);
    }

    depth_unsubscribe(ses, market, interval);
    int ret = depth_subscribe(ses, market, interval);
    if (ret != 0) {
        log_error("depth_subscribe fail, market: %s, interval: %s", market, interval);
        reply_error_internal_error(ses, pkg);
        return ret;
    }

    depth_send_last(ses, market, interval);

    return 0;
}

static int on_method_depth_subscribe_all(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    dict_t *dict_market = get_market();
    if (dict_size(dict_market) == 0) {
        log_error("dict_market is null");
        reply_error_internal_error(ses, pkg);
        return 0;
    }

    const char *interval = "0";
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_market);

    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        depth_unsubscribe(ses, market, interval);
        int ret = depth_subscribe(ses, market, interval);
        if (ret != 0) {
            log_error("depth_subscribe fail, market: %s, interval: %s", market, interval);
            continue;
        }
        depth_send_last(ses, market, interval);
    }
    dict_release_iterator(iter);
    
    add_subscribe_depth_all_ses(ses);
    return 0;
}

static int on_method_depth_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    const char *interval = json_string_value(json_array_get(params, 1));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || interval == NULL || strlen(interval) >= INTERVAL_MAX_LEN) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    depth_unsubscribe(ses, market, interval);
    return 0;
}

static int on_cmd_market_deals(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market)) {
        log_error("market not exist, market: %s", market);
        return reply_error_invalid_argument(ses, pkg);
    }

    int limit = json_integer_value(json_array_get(params, 1));
    if (limit <= 0 || limit > MARKET_DEALS_MAX)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t last_id = json_integer_value(json_array_get(params, 2));

    deals_request(ses, pkg, market, limit, last_id);
    return 0;
}

static int on_method_deals_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    if (!market_exist(market)) {
        log_error("market not exist, market: %s", market);
        return reply_error_invalid_argument(ses, pkg);
    }

    deals_unsubscribe(ses, market);
    int ret = deals_subscribe(ses, market);
    if (ret != 0) {
        log_error("deals_subscribe fail, market: %s", market);
        reply_error_internal_error(ses, pkg);
        return ret;
    }

    return 0;
}

static int on_method_deals_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    deals_unsubscribe(ses, market);
    return 0;
}

static int on_method_kline(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return reply_error_invalid_argument(ses, pkg);

    time_t start = json_integer_value(json_array_get(params, 1));
    if (start <= 0)
        return reply_error_invalid_argument(ses, pkg);

    time_t end = json_integer_value(json_array_get(params, 2));
    time_t now = time(NULL);
    if (end > now)
        end = now;
    if (end <= 0 || start > end)
        return reply_error_invalid_argument(ses, pkg);

    int interval = json_integer_value(json_array_get(params, 3));
    if (interval <= 0)
        return reply_error_invalid_argument(ses, pkg);

    kline_request(ses, pkg, market, start, end, interval);
    return 0;
}

static int on_method_kline_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    int interval = json_integer_value(json_array_get(params, 1));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || interval < 0) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    if (!market_exist(market)) {
        log_error("market not exist, market: %s", market);
        return reply_error_invalid_argument(ses, pkg);
    }

    kline_unsubscribe(ses, market, interval);
    int ret = kline_subscribe(ses, market, interval);
    if (ret != 0) {
        log_error("kline_subscribe fail, market: %s, interval: %d", market, interval);
        reply_error_internal_error(ses, pkg);
        return ret;
    }

    return 0;
}

static int on_method_kline_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    int interval = json_integer_value(json_array_get(params, 1));

    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN || interval < 0) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    kline_unsubscribe(ses, market, interval);
    return 0;
}

static int on_cmd_market_state(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    if (!market)
        return reply_error_invalid_argument(ses, pkg);
    if (!market_exist(market))
        return reply_error_invalid_argument(ses, pkg);

    int period = json_integer_value(json_array_get(params, 1));

    status_request(ses, pkg, market, period);
    return 0;
}

static int on_method_status_subscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    if (!market_exist(market)) {
        log_error("market not exist, market: %s", market);
        return reply_error_invalid_argument(ses, pkg);
    }

    status_unsubscribe(ses, market, 86400);
    int ret = status_subscribe(ses, market, 86400);
    if (ret != 0) {
        log_error("status_subscribe fail, market: %s", market);
        reply_error_internal_error(ses, pkg);
        return ret;
    }

    return 0;
}

static int on_method_status_unsubscribe(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL || strlen(market) >= MARKET_NAME_MAX_LEN) {
        log_error("parameter error");
        return reply_error_invalid_argument(ses, pkg);
    }

    status_unsubscribe(ses, market, 86400);
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
    case CMD_CACHE_DEPTH_SUBSCRIBE_ALL:
        profile_inc("cmd_cache_depth_sub_all", 1);
        ret = on_method_depth_subscribe_all(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_depth_subscribe_all fail: %d", ret);
        }
        break;
    case CMD_CACHE_DEPTH_UNSUBSCRIBE:
        profile_inc("cmd_cache_depth_unsub", 1);
        ret = on_method_depth_unsubscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_depth_unsubscribe fail: %d", ret);
        }
        break;
    case CMD_CACHE_DEALS:
        profile_inc("cmd_cache_deals", 1);
        ret = on_cmd_market_deals(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_deals fail: %d", ret);
        }
        break;
    case CMD_CACHE_DEALS_SUBSCRIBE:
        profile_inc("cmd_cache_deals_sub", 1);
        ret = on_method_deals_subscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_deals_subscribe fail: %d", ret);
        }
        break;
    case CMD_CACHE_DEALS_UNSUBSCRIBE:
        profile_inc("cmd_cache_deals_unsub", 1);
        ret = on_method_deals_unsubscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_deals_unsubscribe fail: %d", ret);
        }
        break;
    case CMD_CACHE_KLINE:
        profile_inc("cmd_cache_kline", 1);
        ret = on_method_kline(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_kline fail: %d", ret);
        }
        break;
    case CMD_CACHE_KLINE_SUBSCRIBE:
        profile_inc("cmd_cache_kline_sub", 1);
        ret = on_method_kline_subscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_kline_subscribe fail: %d", ret);
        }
        break;
    case CMD_CACHE_KLINE_UNSUBSCRIBE:
        profile_inc("cmd_cache_kline_unsub", 1);
        ret = on_method_kline_unsubscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_kline_unsubscribe fail: %d", ret);
        }
        break;
    case CMD_CACHE_STATUS:
        profile_inc("cmd_cache_status", 1);
        ret = on_cmd_market_state(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_state fail: %d", ret);
        }
        break;
    case CMD_CACHE_STATUS_SUBSCRIBE:
        profile_inc("cmd_cache_status_sub", 1);
        ret = on_method_status_subscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_status_subscribe fail: %d", ret);
        }
        break;
    case CMD_CACHE_STATUS_UNSUBSCRIBE:
        profile_inc("cmd_cache_status_unsub", 1);
        ret = on_method_status_unsubscribe(ses, pkg, params);
        if (ret < 0) {
            log_error("on_method_status_unsubscribe fail: %d", ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

    json_decref(params);
    if (ret != 0) {
        sds params_str = sdsnewlen(pkg->body, pkg->body_size);
        log_info("from: %s cmd: %u, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, params_str);
        sdsfree(params_str);
    }

    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_info("connection: %s close", nw_sock_human_addr(&ses->peer_addr));

    kline_unsubscribe_all(ses);
    depth_unsubscribe_all(ses);
    deals_unsubscribe_all(ses);
    status_unsubscribe_all(ses);
    del_subscribe_depth_all_ses(ses);
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

