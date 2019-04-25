/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */

# include "me_config.h"
# include "nw_state.h"
# include "me_access.h"
# include "me_reply.h"

static rpc_svr *svr;
static rpc_clt *writer_clt;
static rpc_clt **reader_clt_arr;
static nw_state *state_context;

struct state_info {
    nw_ses  *ses;
    uint64_t ses_id;
    uint32_t sequence;
};

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    switch (pkg->command) {
    case CMD_ASSET_LIST:
    case CMD_ASSET_QUERY:
    case CMD_ASSET_QUERY_LOCK:
    case CMD_ORDER_PENDING:
    case CMD_ORDER_BOOK:
    case CMD_ORDER_DEPTH:
    case CMD_ORDER_PENDING_DETAIL:
    case CMD_ORDER_PENDING_STOP:
    case CMD_ORDER_STOP_BOOK:
    case CMD_MARKET_LIST:
        //read
    case CMD_ASSET_UPDATE:
    case CMD_ASSET_LOCK:
    case CMD_ASSET_UNLOCK:
    case CMD_ASSET_BACKUP:
    case CMD_ORDER_PUT_LIMIT:
    case CMD_ORDER_PUT_MARKET:
    case CMD_ORDER_CANCEL:
    case CMD_ORDER_PUT_STOP_LIMIT:
    case CMD_ORDER_PUT_STOP_MARKET:
    case CMD_ORDER_CANCEL_STOP:
    case CMD_MARKET_SELF_DEAL:
        //write
    case CMD_CONFIG_UPDATE_ASSET:
    case CMD_CONFIG_UPDATE_MARKET:
        //write && read

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

static int init_server()
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

static void on_state_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
    struct state_info *info = entry->data;
    if (info->ses->id == info->ses_id) {
        log_error("request timeout");
    }
}

static void on_worker_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect worker %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect worker %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_worker_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry) {
        uint32_t sequence = pkg->sequence;
        struct state_info *info = entry->data;
        if (info->ses->id == info->ses_id) {
            pkg->sequence = info->sequence;
            rpc_send(info->ses, pkg);
        }
        nw_state_del(state_context, sequence);
    }
}

static rpc_clt *init_clt(char *name, int port_offset)
{
    rpc_clt_cfg cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.name = name;
    cfg.addr_count = 1;
    cfg.addr_arr = malloc(sizeof(nw_addr_t));
    memcpy(cfg.addr_arr, &settings.svr.bind_arr->addr, sizeof(nw_addr_t));
    cfg.addr_arr->in.sin_port = htons(ntohs(cfg.addr_arr->in.sin_port) + port_offset);
    cfg.sock_type = settings.svr.bind_arr->sock_type;
    cfg.max_pkg_size = 1000 * 1000;

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_worker_connect;
    ct.on_recv_pkg = on_worker_recv_pkg;
    return rpc_clt_create(&cfg, &ct);
}

static int init_worker_clt()
{
    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_state_timeout;
    state_context = nw_state_create(&st, sizeof(struct state_info));
    if (state_context == NULL)
        return -__LINE__;

    if (settings.svr.bind_count != 1)
        return -__LINE__;

    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET)
        return -__LINE__;

    writer_clt = init_clt("writer_clt", 1);
    if (writer_clt == NULL) {
        return -__LINE__;
    }

    if (rpc_clt_start(writer_clt) < 0)
            return -__LINE__;

    reader_clt_arr = malloc(sizeof(void *) * settings.reader_num);
    for (int i = 0; i < settings.reader_num; ++i) {
        sds name = sdsempty();
        name = sdscatprintf(name, "reader_clt_%d", i);

        reader_clt_arr[i] = init_clt(name, i + 2);
        if (reader_clt_arr[i] == NULL)
            return -__LINE__;
        if (rpc_clt_start(reader_clt_arr[i]) < 0)
            return -__LINE__;

        sdsfree(name);
    }
    return 0;
}

int init_access()
{
    int ret = init_server();
    if (ret < 0) {
        return -__LINE__;
    }
    ret = init_worker_clt();
    if (ret < 0) {
        return -__LINE__;
    }
    return 0;
}