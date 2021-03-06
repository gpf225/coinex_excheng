/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */

# include "me_config.h"
# include "nw_state.h"
# include "me_access.h"

static rpc_svr *svr;
static cli_svr *svrcli;
static rpc_clt *writer_clt;
static rpc_clt **reader_clt_arr;
static nw_state *state_context;
static bool available;
static uint32_t reader_loop;

struct state_info {
    nw_ses  *ses;
    uint64_t ses_id;
    uint32_t sequence;
    uint32_t command;
};

static void sendto_clt_pkg(rpc_clt *clt, nw_ses *ses, rpc_pkg *pkg)
{
    nw_state_entry *entry = nw_state_add(state_context, settings.worker_timeout, 0);
    struct state_info *info = entry->data;
    info->ses = ses;
    info->ses_id = ses->id;
    info->sequence = pkg->sequence;
    info->command = pkg->command;

    pkg->sequence = entry->id;
    rpc_clt_send(clt, pkg);
}

static void sendto_writer(nw_ses *ses, rpc_pkg *pkg)
{
    if (!rpc_clt_connected(writer_clt)) {
        rpc_reply_error_internal_error(ses, pkg);
        log_fatal("lose connection to writer");
        return;
    }

    sendto_clt_pkg(writer_clt, ses, pkg);
    profile_inc("access_to_write", 1);
}

static void sendto_reader_fixed(nw_ses *ses, rpc_pkg *pkg, uint32_t unique_id)
{
    int reader_id = unique_id % (settings.reader_num - 1);
    if (!rpc_clt_connected(reader_clt_arr[reader_id])) {
        rpc_reply_error_internal_error(ses, pkg);
        log_fatal("lose connection to reader: %d", reader_id);
        return;
    }

    sendto_clt_pkg(reader_clt_arr[reader_id], ses, pkg);
    char str[100];
    snprintf(str, sizeof(str), "access_to_reader_%d", reader_id);
    profile_inc(str, 1);
}

static void sendto_reader_loop(nw_ses *ses, rpc_pkg *pkg)
{
    int reader_id = 0;
    bool connected = false;
    for (int i = 0; i < settings.reader_num - 1; ++i) {
        reader_id = (reader_loop++) % (settings.reader_num - 1);
        if (rpc_clt_connected(reader_clt_arr[reader_id])) {
            connected = true;
            break;
        }
        log_fatal("lose connection to reader: %d", reader_id);
    }

    if (!connected) {
        rpc_reply_error_internal_error(ses, pkg);
        log_fatal("lose connection to all reader");
        return;
    }

    sendto_clt_pkg(reader_clt_arr[reader_id], ses, pkg);
    char str[100];
    snprintf(str, sizeof(str), "access_to_reader_%d", reader_id);
    profile_inc(str, 1);
}

static int get_unique_from_ext(rpc_pkg *pkg, uint32_t *unique_id)
{
    void *pos = pkg->ext;
    size_t left = pkg->ext_size;
    while (left > 0) {
        uint16_t type, size;
        ERR_RET(unpack_uint16_le(&pos, &left, &type));
        ERR_RET(unpack_uint16_le(&pos, &left, &size));
        if (type == RPC_EXT_TYPE_UNIQUE) {
            ERR_RET(unpack_uint32_le(&pos, &left, unique_id));
            return 0;
        } else {
            ERR_RET(unpack_pass(&pos, &left, size));
        }
    }

    return 0;
}

static void sendto_reader(nw_ses *ses, rpc_pkg *pkg)
{
    uint32_t unique_id = 0;
    if (pkg->ext_size > 0) {
        get_unique_from_ext(pkg, &unique_id);
    }

    if (unique_id > 0) {
        sendto_reader_fixed(ses, pkg, unique_id);
    } else {
        sendto_reader_loop(ses, pkg);
    }
}

static void sendto_reader_summary(nw_ses *ses, rpc_pkg *pkg)
{
    int reader_id = settings.reader_num - 1;
    if (!rpc_clt_connected(reader_clt_arr[reader_id])) {
        rpc_reply_error_internal_error(ses, pkg);
        log_fatal("lose connection to summary reader: %d", reader_id);
        return;
    }

    sendto_clt_pkg(reader_clt_arr[reader_id], ses, pkg);
    char str[100];
    snprintf(str, sizeof(str), "access_to_reader_%d", reader_id);
    profile_inc(str, 1);
}

static void sendto_all(nw_ses *ses, rpc_pkg *pkg)
{
    if (!rpc_clt_connected(writer_clt)) {
        rpc_reply_error_internal_error(ses, pkg);
        log_fatal("lose connection to writer");
        return;
    }

    for (int i = 0; i < settings.reader_num; ++i) {
        if (!rpc_clt_connected(reader_clt_arr[i])) {
            rpc_reply_error_internal_error(ses, pkg);
            log_fatal("lose connection to reader: %d", i);
            return;
        }
    }

    sendto_clt_pkg(writer_clt, ses, pkg);
    for (int i = 0; i < settings.reader_num; ++i) {
        sendto_clt_pkg(reader_clt_arr[i], ses, pkg);
    }
}

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
    if (!available) {
        rpc_reply_error_service_unavailable(ses, pkg);
        return;
    }

    switch (pkg->command) {
    case CMD_ASSET_UPDATE:
    case CMD_ASSET_UPDATE_BATCH:
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
    case CMD_ORDER_CANCEL_ALL:
    case CMD_ORDER_CANCEL_STOP_ALL:
    case CMD_CALL_AUCTION_START:
    case CMD_CALL_AUCTION_EXECUTE:
    case CMD_ORDER_CANCEL_BATCH:
    case CMD_ASSET_QUERY_INTIME:
    case CMD_ASSET_QUERY_ALL_INTIME:
    case CMD_ASSET_QUERY_LOCK_INTIME:
    case CMD_ASSET_QUERY_USERS_INTIME:
    case CMD_ORDER_PENDING_INTIME:
    case CMD_ORDER_PENDING_STOP_INTIME:
        sendto_writer(ses, pkg);
        break;
    case CMD_ASSET_QUERY:
    case CMD_ASSET_QUERY_ALL:
    case CMD_ASSET_QUERY_LOCK:
    case CMD_ORDER_PENDING:
    case CMD_ORDER_PENDING_STOP:
    case CMD_ORDER_BOOK:
    case CMD_ORDER_STOP_BOOK:
    case CMD_ORDER_DEPTH:
    case CMD_ORDER_PENDING_DETAIL:
    case CMD_MARKET_LIST:
    case CMD_MARKET_DETAIL:
    case CMD_ASSET_LIST:
        sendto_reader(ses, pkg);
        break;
    case CMD_ASSET_QUERY_USERS:
    case CMD_ASSET_SUMMARY:
    case CMD_MARKET_SUMMARY:
        sendto_reader_summary(ses, pkg);
        break;
    case CMD_CONFIG_UPDATE_ASSET:
    case CMD_CONFIG_UPDATE_MARKET:
        sendto_all(ses, pkg);
        break;
    default:
        profile_inc("method_not_found", 1);
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

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
        rpc_pkg pkg;
        memset(&pkg, 0, sizeof(pkg));
        pkg.sequence = info->sequence;
        rpc_reply_error_service_timeout(info->ses, &pkg);
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
    if (pkg->pkg_type == RPC_PKG_TYPE_PUSH) {
        switch (pkg->command) {
        case CMD_REDER_ERROR:
            available = false;
            log_fatal("from: %s reader error", nw_sock_human_addr(&ses->peer_addr));
            break;
        default:
            profile_inc("method_push_not_found", 1);
            log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
            break;
        }
    } else {
        nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
        if (entry) {
            uint32_t sequence = pkg->sequence;
            struct state_info *info = entry->data;
            if (info->ses->id == info->ses_id) {
                pkg->sequence = info->sequence;
                pkg->command  = info->command;
                rpc_send(info->ses, pkg);
            }
            nw_state_del(state_context, sequence);
        }
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
    cfg.max_pkg_size = settings.svr.max_pkg_size;

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
        if (reader_clt_arr[i] == NULL) {
            sdsfree(name);
            return -__LINE__;
        }
        if (rpc_clt_start(reader_clt_arr[i]) < 0) {
            sdsfree(name);
            return -__LINE__;
        }

        sdsfree(name);
    }
    return 0;
}

static sds on_cmd_status(const char *cmd, int argc, sds *argv)
{
    sds reply = sdsempty();
    if (available) {
        reply = sdscatprintf(reply, "sevice available\n");
    } else {
        reply = sdscatprintf(reply, "sevice unavailable\n");
    }

    if (rpc_clt_connected(writer_clt)) {
        reply = sdscatprintf(reply, "writer ok\n");
    } else {
        reply = sdscatprintf(reply, "writer dead\n");
    }

    for (int i = 0; i < settings.reader_num; ++i) {
        if (rpc_clt_connected(reader_clt_arr[i])) {
            reply = sdscatprintf(reply, "reader: %u ok\n", i);
        } else {
            reply = sdscatprintf(reply, "reader: %u dead\n", i);
        }
    }

    return reply;
}

static sds on_cmd_unavailable(const char *cmd, int argc, sds *argv)
{
    available = false;
    return sdsnew("OK\n");
}

static sds on_cmd_available(const char *cmd, int argc, sds *argv)
{
    available = true;
    return sdsnew("OK\n");
}

static int init_cli()
{
    svrcli = cli_svr_create(&settings.cli);
    if (svrcli == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svrcli, "status", on_cmd_status);
    cli_svr_add_cmd(svrcli, "unavailable", on_cmd_unavailable);
    cli_svr_add_cmd(svrcli, "available", on_cmd_available);
    return 0;
}

int init_access()
{
    available = false;
    int ret = init_worker_clt();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_cli();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_server();
    if (ret < 0) {
        return -__LINE__;
    }
    available = true;
    return 0;
}
