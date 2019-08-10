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
    return rpc_reply_error_invalid_argument(ses, pkg);
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

    if (market == NULL || strlen(market) > MARKET_NAME_MAX_LEN || interval == NULL || strlen(interval) > INTERVAL_MAX_LEN) {
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
    return rpc_reply_error_invalid_argument(ses, pkg);
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

    if (market == NULL || strlen(market) > MARKET_NAME_MAX_LEN || interval == NULL || strlen(interval) > INTERVAL_MAX_LEN) {
        recv_str = sdsnewlen(pkg->body, pkg->body_size);
        goto error;
    }

    depth_unsubscribe(ses, market, interval);
    return 0;

error:
    log_error("parameter error, recv_str: %s", recv_str);
    sdsfree(recv_str);
    return rpc_reply_error_invalid_argument(ses, pkg);
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

