/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/06, create
 */

# include "mc_config.h"
# include "mc_server.h"

static rpc_svr *svr;
static redis_sentinel_t *redis;
static redisContext *redis_store;

void *redis_query(const char *format, ...)
{
    for (int i = 0; i < 2; ++i) {
        if (redis_store == NULL) {
            log_info("redis connection lost, try connect");
            redis_store = redis_sentinel_connect_master(redis);
            if (redis_store == NULL) {
                log_error("redis_sentinel_connect_master fail");
                break;
            }
        }

        va_list ap;
        va_start(ap, format);
        redisReply *reply = redisvCommand(redis_store, format, ap);
        va_end(ap);

        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply == NULL) {
                log_error("redisvCommand fail: %d, %s", redis_store->err, strerror(errno));
            } else {
                log_error("redisvCommand fail: %d, %s, %s", redis_store->err, strerror(errno), reply->str);
                freeReplyObject(reply);
            }
            redisFree(redis_store);
            redis_store = NULL;
            continue;
        }

        return reply;
    }

    return NULL;
}

static int on_cmd_monitor_inc(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    return 0;
}

static int on_cmd_monitor_set(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    return 0;
}

static int on_cmd_monitor_list_scope(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    return 0;
}

static int on_cmd_monitor_list_key(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    return 0;
}

static int on_cmd_monitor_list_host(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    return 0;
}

static int on_cmd_monitor_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    return 0;
}

static int on_cmd_monitor_daily(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    return 0;
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_MONITOR_INC:
        log_debug("from: %s cmd monitor inc, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_inc(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_inc %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_SET:
        log_debug("from: %s cmd monitor set, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_set(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_set %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_SCOPE:
        log_debug("from: %s cmd monitor list scope, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_scope(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_scope %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_KEY:
        log_debug("from: %s cmd monitor list key, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_key(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_key %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_HOST:
        log_debug("from: %s cmd monitor list host, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_host(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_QUERY:
        log_debug("from: %s cmd monitor query minute, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_DAILY:
        log_debug("from: %s cmd monitor query daily, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_daily(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
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

    redis = redis_sentinel_create(&settings.redis);
    if (redis == NULL)
    redis_store = redis_sentinel_connect_master(redis);
    if (redis_store == NULL)
        return -__LINE__;

    return 0;
}

