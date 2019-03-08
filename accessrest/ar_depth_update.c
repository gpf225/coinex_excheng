/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/01, create
 */

# include "ar_depth_update.h"
# include "ar_depth_cache.h"
# include "ar_depth_wait_queue.h"
# include "ar_server.h"
# include "ar_common.h"
# include "ar_statistic.h"

static dict_t *dict_depth_update_queue = NULL;
static rpc_clt *cache = NULL;
static nw_state *state_context = NULL;

struct state_data {
    char market[MARKET_NAME_MAX_LEN];
    char interval[INTERVAL_MAX_LEN];
    uint32_t limit;
    nw_ses *ses;
    uint64_t ses_id;
};

static int init_depth_update_queue(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;

    dict_depth_update_queue = dict_create(&dt, 128);
    if (dict_depth_update_queue == NULL) {
        return -__LINE__;
    }
    return 0;
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void reply_to_ses(const char *market, const char *interval, json_t *result, nw_ses *ses, list_t *list)
{
    list_node *node = NULL;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        struct depth_wait_item *item = node->value;
        nw_state_del(state_context, item->sequence);
        stat_depth_update_released();

        json_t *new_result = depth_get_result(result, item->limit);
        int ret = send_result(ses, new_result);
        if (ret != 0) {
            log_error("send_result failed, ses:%p at %s-%s-%u", ses, market, interval, item->limit);
        }
        list_del(list, node);
        json_decref(new_result);  
    } 
    list_release_iterator(iter);  
}

static void result_handle(const char *market, const char *interval, json_t *result)
{
    struct depth_wait_val *val = depth_wait_queue_get(market, interval);
    if (val == NULL) {
        return ;
    }

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(val->dict_wait_session);
    log_trace("%s-%s size:%u", market, interval, dict_size(val->dict_wait_session));
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        list_t *list = entry->val;
        reply_to_ses(market, interval, result, ses, list);
        if (list_len(list) == 0) {
            dict_delete(val->dict_wait_session, ses);
        }
    }
    dict_release_iterator(iter);
}

static void depth_update_queue_remove(const char *market, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_delete(dict_depth_update_queue, &key);
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    //REPLY_TRACE_LOG(ses, pkg);

    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        return;
    }
    struct state_data *state = entry->data;
    log_trace("depth reply from cache, depth: %s-%s-%u", state->market, state->interval, state->limit);
    depth_update_queue_remove(state->market, state->interval);
    ut_rpc_reply_t *rpc_reply = NULL;
    do {
        rpc_reply = reply_load(pkg->body, pkg->body_size);
        if (!reply_valid(rpc_reply)) {
            REPLY_INVALID_LOG(ses, pkg);
            break;
        }
        if (!reply_ok(rpc_reply)) {
            REPLY_ERROR_LOG(ses, pkg);
            break;
        }
        
        if (pkg->command == CMD_ORDER_DEPTH_REST) {
            if (!json_is_integer(json_object_get(rpc_reply->result, "ttl"))) {
                REPLY_ERROR_LOG(ses, pkg);
                break;
            }
            uint32_t ttl = json_integer_value(json_object_get(rpc_reply->result, "ttl"));
            depth_cache_set(state->market, state->interval, ttl, rpc_reply->result);
            result_handle(state->market, state->interval, rpc_reply->result);
        } else {
            log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        }
    } while(0);

    reply_release(rpc_reply);
    nw_state_del(state_context, pkg->sequence);
}

static void push_to_wait_queue(nw_ses *ses, const char *market, const char *interval, uint32_t limit)
{
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(state->interval, interval, INTERVAL_MAX_LEN - 1);
    state->limit = limit;
    state->ses = ses;
    state->ses_id = ses->id;
    depth_wait_queue_add(market, interval, limit, ses, state_entry->id);
}

int depth_update(nw_ses *ses, const char *market, const char *interval, uint32_t limit)
{
    push_to_wait_queue(ses, market, interval, limit);

    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_entry *entry = dict_find(dict_depth_update_queue, &key);
    if (entry != NULL) {    
        stat_depth_update_wait();
        return 0;
    }

    if (dict_add(dict_depth_update_queue, &key, NULL) == NULL) {
        return -__LINE__;
    }
    stat_depth_update();

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(state->interval, interval, INTERVAL_MAX_LEN - 1);
    state->limit = settings.depth_limit_max;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(settings.depth_limit_max));
    json_array_append_new(params, json_string(interval));
    
    rpc_pkg req_pkg;
    memset(&req_pkg, 0, sizeof(req_pkg));
    req_pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    req_pkg.command   = CMD_ORDER_DEPTH_REST;
    req_pkg.sequence  = state_entry->id;
    req_pkg.body      = json_dumps(params, 0);
    req_pkg.body_size = strlen(req_pkg.body);

    rpc_clt_send(cache, &req_pkg);
    free(req_pkg.body);
    json_decref(params);

    return 0;
}

static void on_timeout(nw_state_entry *entry)
{
    stat_depth_update_timeout();
    struct state_data *state = entry->data;
    log_error("query depth timeout, %s-%s-%u state id: %u", state->market, state->interval, state->limit, entry->id);

    depth_update_queue_remove(state->market, state->interval);
    if (state->ses != NULL && state->ses->id == state->ses_id) {
        depth_wait_queue_remove(state->market, state->interval, state->limit, state->ses, entry->id);
        reply_time_out(state->ses);
    }
}

int init_depth_update(void)
{
    int ret = init_depth_update_queue();
    if (ret != 0) {
        return ret;
    }

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;
    
    assert(cache == NULL);
    cache = rpc_clt_create(&settings.cache, &ct);
    if (cache == NULL) {
        log_stderr("rpc_clt_create failed");
        return -__LINE__;
    }
    if (rpc_clt_start(cache) < 0) {
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