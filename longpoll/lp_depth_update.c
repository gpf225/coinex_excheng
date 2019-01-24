/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/01, create
 */

# include "lp_depth_update.h"
# include "lp_depth_sub.h"
# include "lp_server.h"
# include "lp_statistic.h"

static nw_state *state_context = NULL;
static nw_timer poll_depth_timer;
static rpc_clt *matchengine = NULL;

struct state_data {
    struct depth_key key;
    struct depth_limit_val list;
};

static void depth_on_notify(struct depth_key *key, json_t *depth_data)
{
    dict_t *dict_depth_sub = depth_get_sub();
    dict_entry *entry = dict_find(dict_depth_sub, key);
    if (entry == NULL) {
        log_info("subscribe_item[%s-%s-%d] not exist.", key->market, key->interval, key->limit);
        return ;
    }

    struct depth_val *val = entry->val;
    assert(dict_size(val->sessions) > 0);

    dict_entry *session_entry = NULL;
    dict_iterator *iter = dict_get_iterator(val->sessions);
    while ( (session_entry = dict_next(iter)) != NULL) {
        json_t *reply = json_object();
        json_object_set_new(reply, "market", json_string(key->market));
        json_object_set_new(reply, "interval", json_string(key->interval));
        json_object_set_new(reply, "limit", json_integer(key->limit));
        json_object_set    (reply, "data", depth_data);
        
        nw_ses *ses = session_entry->key;
        notify_message(ses, CMD_LP_DEPTH_UPDATE, reply);
        json_decref(reply);
    }
    dict_release_iterator(iter);
}

static json_t* generate_depth_data(json_t *depth_data, int limit) {
    if (depth_data == NULL) {
        return json_null();
    }

    json_t *new_data = json_array();
    int size = json_array_size(depth_data) > limit ? limit : json_array_size(depth_data);
    for (int i = 0; i < size; ++i) {
        json_t *unit = json_array_get(depth_data, i);
        json_array_append(new_data, unit);
    }

    return new_data;
}

static void on_notify_subscribe(struct depth_key *key, struct depth_limit_val *list, json_t *depth_data) {
    stat_depth_update();
    if (list->size == 1) {
        key->limit = list->max;
        depth_on_notify(key, depth_data);
        return ;
    }

    for (int i = 0; i < list->size; ++i) {
        if (list->limits[i] == list->max) {
            key->limit = list->max;
            depth_on_notify(key, depth_data);
        } else {
            int limit = list->limits[i];
            json_t *asks_array = json_object_get(depth_data, "asks");
            json_t *bids_array = json_object_get(depth_data, "bids");

            json_t *new_depth_data = json_object();
            json_object_set_new(new_depth_data, "asks", generate_depth_data(asks_array, limit));
            json_object_set_new(new_depth_data, "bids", generate_depth_data(bids_array, limit));
            json_object_set    (new_depth_data, "last", json_object_get(depth_data, "last"));
            json_object_set    (new_depth_data, "time", json_object_get(depth_data, "time"));
            
            key->limit = limit;
            depth_on_notify(key, new_depth_data);
            json_decref(new_depth_data);
        }
    }
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        return;
    }

    ut_rpc_reply_t *rpc_reply = reply_load(pkg->body, pkg->body_size);
    if (rpc_reply == NULL) {
        log_fatal("reply_load returns NULL, system maybe lack memory");
        nw_state_del(state_context, pkg->sequence);
        return ;
    }

    do {
        if (!reply_valid(rpc_reply)) {
            REPLY_INVALID_LOG(ses, pkg);
            break;
        }
        if (!reply_ok(rpc_reply)) {
            REPLY_ERROR_LOG(ses, pkg);
            break;
        }

        if (pkg->command == CMD_ORDER_DEPTH) {
             struct state_data *state = entry->data;
             on_notify_subscribe(&state->key, &state->list, rpc_reply->result);
        } else {
            log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        }
    } while(0);

    reply_release(rpc_reply);
    nw_state_del(state_context, pkg->sequence);
}

static void on_poll_depth(nw_timer *timer, void *privdata) 
{
    if (!rpc_clt_connected(matchengine)) {
        log_error("matchengine does not connected");
        return ;
    }

    dict_t *depth_item = depth_get_item();
    if (dict_size(depth_item) == 0) {
        return ;
    }
    
    stat_depth_poll(dict_size(depth_item));

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(depth_item);
    while ( (entry = dict_next(iter)) != NULL) {
        struct depth_limit_val *list = entry->val;
        struct depth_key *key = entry->key;

        json_t *params = json_array();
        json_array_append_new(params, json_string(key->market));
        json_array_append_new(params, json_integer(list->max));
        json_array_append_new(params, json_string(key->interval));
        log_trace("poll depth: %s-%s-%u", key->market, key->interval, list->max);

        nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
        struct state_data *state = state_entry->data;
        memcpy(&state->key, key, sizeof(struct depth_key));
        memcpy(&state->list, list, sizeof(struct depth_limit_val));
  
        rpc_pkg pkg;
        memset(&pkg, 0, sizeof(pkg));
        pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
        pkg.command   = CMD_ORDER_DEPTH;
        pkg.sequence  = state_entry->id;
        pkg.body      = json_dumps(params, 0);
        pkg.body_size = strlen(pkg.body);

        rpc_clt_send(matchengine, &pkg);
        free(pkg.body);
        json_decref(params);
    }
    dict_release_iterator(iter);
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

static void on_timeout(nw_state_entry *entry)
{
    log_fatal("query depth timeout, state id: %u", entry->id);
}

int init_depth_update(void)
{
    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL) {
        log_stderr("nw_state_create failed");
        return -__LINE__;
    }
    
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;
    
    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL) {
        log_stderr("rpc_clt_create matchengine failed");
        return -__LINE__;
    }
    if (rpc_clt_start(matchengine) < 0) {
        log_stderr("rpc_clt_start matchengine failed");
        return -__LINE__;
    }

    nw_timer_set(&poll_depth_timer, settings.poll_depth_interval, true, on_poll_depth, NULL);
    nw_timer_start(&poll_depth_timer);

    return 0;
}