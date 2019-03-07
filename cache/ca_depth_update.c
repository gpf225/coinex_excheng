/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# include "ca_depth_update.h"
# include "ca_depth_cache.h"
# include "ca_depth_poll.h"
# include "ca_depth_wait_queue.h"
# include "ca_common.h"
# include "ca_server.h"
# include "ca_statistic.h"

# define WAIT_TYPE_HTTP  1
# define WAIT_TYPE_REST  2

static dict_t *dict_depth_update_queue = NULL;
static rpc_clt *matchengine = NULL;
static nw_state *state_context = NULL;

typedef struct depth_req_val {
    uint32_t limit;
}depth_req_val;

struct state_data {
    char market[MARKET_NAME_MAX_LEN];
    char interval[INTERVAL_MAX_LEN];
    uint32_t limit;
    nw_ses *ses;
    uint64_t ses_id;
};

static void *dict_depth_req_val_dup(const void *val)
{
    struct depth_req_val *obj = malloc(sizeof(struct depth_req_val));
    memcpy(obj, val, sizeof(struct depth_req_val));
    return obj;
}

static void dict_depth_req_val_free(void *val)
{
    free(val);
}

static int init_depth_update_queue(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_req_val_dup;
    dt.val_destructor = dict_depth_req_val_free;

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

static void reply_to_ses(const char *market, const char *interval, uint32_t limit, json_t *result, uint64_t ttl, nw_ses *ses, list_t *list)
{
    list_node *node = NULL;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        struct depth_wait_item *item = node->value;
        if (limit >= item->limit) {
            nw_state_del(state_context, item->sequence);
            stat_depth_update_released();

            json_t *new_result = NULL;
            if (item->wait_type == WAIT_TYPE_REST) {
                new_result = depth_get_result_rest(result, limit, item->limit, ttl);
            } else {
                new_result = depth_get_result(result, limit, item->limit);
            }
            int ret = reply_result(ses, &item->pkg, new_result);
            if (ret != 0) {
                log_error("send_result failed, ses:%p at %s-%s-%u", ses, market, interval, item->limit);
            }
            list_del(list, node);
            json_decref(new_result);  
        }
    } 
    list_release_iterator(iter);  
}

static void result_handle(const char *market, const char *interval, uint32_t limit, json_t *result, uint64_t ttl)
{
    struct depth_wait_val *val = depth_wait_queue_get(market, interval);
    if (val == NULL) {
        return ;
    }

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(val->dict_wait_session);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        list_t *list = entry->val;
        reply_to_ses(market, interval, limit, result, ttl, ses, list);
        if (list_len(list) == 0) {
            dict_delete(val->dict_wait_session, ses);
        }
    }
    dict_release_iterator(iter);
}

static void depth_update_queue_remove(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_entry *entry = dict_find(dict_depth_update_queue, &key);
    if (entry == NULL) {
        return ;
    }
    struct depth_req_val *val = entry->val;
    assert(val->limit >= limit);
    if (limit == val->limit) {
        dict_delete(dict_depth_update_queue, &key);
    }
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    //REPLY_TRACE_LOG(ses, pkg);

    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        return;
    }
    struct state_data *state = entry->data;
    log_trace("depth reply from matchengine, depth: %s-%s-%u", state->market, state->interval, state->limit);
    depth_update_queue_remove(state->market, state->interval, state->limit);

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
        
        if (pkg->command == CMD_ORDER_DEPTH) {
            struct depth_cache_val *val = depth_cache_set(state->market, state->interval, state->limit, rpc_reply->result);
            if (val == NULL) {
                log_fatal("add cache failed, %s-%s-%u", state->market, state->interval, state->limit);
                break;
            }
            //depth_sub_handle(state->market, state->interval, rpc_reply->result, state->limit);
            result_handle(state->market, state->interval, state->limit, rpc_reply->result, val->ttl);
        } else {
            log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        }
    } while(0);

    reply_release(rpc_reply);
    nw_state_del(state_context, pkg->sequence);
}

static void push_to_wait_queue(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval, uint32_t limit, int wait_type)
{
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(state->interval, interval, INTERVAL_MAX_LEN - 1);
    state->limit = limit;
    if (ses != NULL) {
        state->ses = ses;
        state->ses_id = ses->id;
        depth_wait_queue_add(state->market, state->interval, state->limit, ses, state_entry->id, pkg, wait_type);
    }
}

static int depth_update(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval, uint32_t limit, int wait_type)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_entry *entry = dict_find(dict_depth_update_queue, &key);
    if (entry == NULL) {
        struct depth_req_val val;
        memset(&val, 0, sizeof(struct depth_req_val));
        entry = dict_add(dict_depth_update_queue, &key, &val);
        if (entry == NULL) {
            return -__LINE__;
        }
    }
    
    push_to_wait_queue(ses, pkg, market, interval, limit, wait_type);
    struct depth_req_val *val = entry->val;
    if (val->limit >= limit) {
        log_trace("%s-%s-%u has an ongoing request which limit is:%u", market, interval, limit, val->limit);
        stat_depth_update_wait();
        return 0;
    }
    stat_depth_update();
    limit = depth_cache_get_update_limit(market, interval, limit);
    val->limit = limit;
    log_trace("going to update depth %s-%s-%u", market, interval, limit);
    
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(state->interval, interval, INTERVAL_MAX_LEN - 1);
    state->limit = limit;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(limit));
    json_array_append_new(params, json_string(interval));
    
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

int depth_update_http(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval, uint32_t limit)
{
    return depth_update(ses, pkg, market, interval, limit, WAIT_TYPE_HTTP);
}

int depth_update_rest(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval, uint32_t limit)
{
    return depth_update(ses, pkg, market, interval, limit, WAIT_TYPE_REST);
}

int depth_update_sub(const char *market, const char *interval, uint32_t limit)
{
    return depth_update(NULL, NULL, market, interval, limit, 0);
}


static void on_timeout(nw_state_entry *entry)
{
    stat_depth_update_timeout();
    struct state_data *state = entry->data;
    log_fatal("query order depth timeout, state id: %u, %s-%s-%u", entry->id, state->market, state->interval, state->limit);
    depth_update_queue_remove(state->market, state->interval, state->limit);
    if (state->ses != NULL) {
        depth_wait_queue_remove(state->market, state->interval, state->limit, state->ses, entry->id);
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