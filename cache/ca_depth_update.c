/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# include "ca_depth_update.h"
# include "ca_depth_cache.h"
# include "ca_server.h"

static rpc_clt *matchengine = NULL;
static nw_state *state_context = NULL;

struct state_data {
    char market[MARKET_NAME_MAX_LEN];
    char interval[INTERVAL_MAX_LEN];
    uint32_t limit;
    nw_ses *ses;
    rpc_pkg pkg;
    bool reply;
};

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    log_trace("recv depth pkg from: %s, sequence: %u", nw_sock_human_addr(&ses->peer_addr), pkg->sequence);
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        return;
    }
    struct state_data *state = entry->data;
    
    bool success = false;
    json_t *reply = NULL;
    do {
        reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
        if (reply == NULL) {
            break;
        }
        json_t *error = json_object_get(reply, "error");
        if (!error) {
            break;
        }

        json_t *result = json_object_get(reply, "result");
        if (!result) {
            break;
        }
        
        depth_cache_set(state->market, state->interval, state->limit, result);
        if (state->reply) {
            reply_result(state->ses, &state->pkg, result);
        }
        success = true;
    } while (0);

    if (!success) {
        reply_error_internal_error(state->ses, &state->pkg);
    }
    if (reply) {
        json_decref(reply);
    }
    nw_state_del(state_context, pkg->sequence);
}

int depth_update(nw_ses *ses, rpc_pkg *ses_pkg, const char *market, const char *interval, uint32_t limit, bool reply)
{
    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(limit));
    json_array_append_new(params, json_string(interval));

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(state->interval, interval, INTERVAL_MAX_LEN - 1);
    state->limit = limit;
    state->reply = reply;
    if (reply) {
        state->ses = ses;
        memset(&state->pkg, 0, sizeof(rpc_pkg));
        memcpy(&state->pkg, ses_pkg, RPC_PKG_HEAD_SIZE);
    }
    
    rpc_pkg req_pkg;
    memset(&req_pkg, 0, sizeof(req_pkg));
    req_pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    req_pkg.command   = CMD_ORDER_DEPTH;
    req_pkg.sequence  = state_entry->id;
    req_pkg.body      = json_dumps(params, 0);
    req_pkg.body_size = strlen(req_pkg.body);

    rpc_clt_send(matchengine, &req_pkg);
    free(req_pkg.body);
    json_decref(params);

    return 0;
}

static void on_timeout(nw_state_entry *entry)
{
    log_fatal("query order depth timeout, state id: %u", entry->id);
}

int init_depth_update(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;
    
    assert(matchengine == NULL);
    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL) {
        log_stderr("rpc_clt_create failed");
        return -__LINE__;
    }
    if (rpc_clt_start(matchengine) < 0) {
        log_stderr("rpc_clt_start failed");
        return -__LINE__;
    }

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL) {
        log_stderr("nw_state_create failed");
        return -__LINE__;
    }

    return 0;
}