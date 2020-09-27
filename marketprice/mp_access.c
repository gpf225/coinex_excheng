/*
 * Description: 
 *     History: yang@haipo.me, 2016/04/01, create
 */

# include "mp_config.h"
# include "mp_message.h"

static rpc_svr *svr;
static rpc_clt **worker_arr;
static nw_state *state_context;

struct state_info {
    nw_ses  *ses;
    uint64_t ses_id;
    uint32_t sequence;
    uint32_t worker_id;
};

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (market == NULL) {
        goto decode_error;
    }

    nw_state_entry *entry = nw_state_add(state_context, settings.worker_timeout, 0);
    struct state_info *state = entry->data;
    state->ses = ses;
    state->ses_id = ses->id;
    state->sequence = pkg->sequence;

    pkg->sequence = entry->id;
    int id = get_market_id(market);
    state->worker_id = id;
    rpc_clt_send(worker_arr[id], pkg);

    json_decref(params);
    return;

decode_error:
    if (params)
        json_decref(params);
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

static void on_state_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
    struct state_info *info = entry->data;
    log_info("worker_id: %d", info->worker_id);
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

int init_access(void)
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

    worker_arr = malloc(sizeof(void *) * settings.worker_num + 1);
    for (int i = 0; i < settings.worker_num + 1; ++i) {
        sds name = sdsempty();
        name = sdscatprintf(name, "worker_%d", i);

        rpc_clt_cfg cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.name = name;
        cfg.addr_count = 1;
        cfg.addr_arr = malloc(sizeof(nw_addr_t));
        memcpy(cfg.addr_arr, &settings.svr.bind_arr->addr, sizeof(nw_addr_t));
        cfg.addr_arr->in.sin_port = htons(ntohs(cfg.addr_arr->in.sin_port) + i + 1);
        cfg.sock_type = bind_arr->sock_type;
        cfg.max_pkg_size = 1000 * 1000;

        rpc_clt_type ct;
        memset(&ct, 0, sizeof(ct));
        ct.on_connect = on_worker_connect;
        ct.on_recv_pkg = on_worker_recv_pkg;
        worker_arr[i] = rpc_clt_create(&cfg, &ct);
        if (worker_arr[i] == NULL)
            return -__LINE__;
        if (rpc_clt_start(worker_arr[i]) < 0)
            return -__LINE__;

        sdsfree(name);
    }

    return 0;
}

