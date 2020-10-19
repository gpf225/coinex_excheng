/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_status.h"
# include "ca_server.h"
# include "ca_market.h"
# include "ca_depth.h"

static rpc_clt *marketprice;
static rpc_clt *matchengine;
static nw_state *state_context;

static dict_t *dict_state;
static dict_t *dict_session;
static rpc_svr *state_svr;
static nw_timer timer;

struct dict_status_val {
    json_t      *last_state;
    json_t      *last_depth;
};

struct state_data {
    char        market[MARKET_NAME_MAX_LEN + 1];
    uint32_t    cmd;
};

static void *dict_status_sub_val_dup(const void *val)
{
    struct dict_status_val *obj = malloc(sizeof(struct dict_status_val));
    memcpy(obj, val, sizeof(struct dict_status_val));
    return obj;
}

static void dict_status_sub_val_free(void *key)
{
    struct dict_status_val *obj = key;
    if (obj->last_state)
        json_decref(obj->last_state);
    if (obj->last_depth)
        json_decref(obj->last_depth);
    free(obj);
}

json_t *create_init_depth()
{
    json_t *depth = json_object();
    json_object_set_new(depth, "buy", json_string("0"));
    json_object_set_new(depth, "buy_amount", json_string("0"));
    json_object_set_new(depth, "sell", json_string("0"));
    json_object_set_new(depth, "sell_amount", json_string("0"));
    return depth;
}

static void notify_state(void)
{
    dict_entry *entry;
    json_t *result = json_array();
    dict_iterator *iter = dict_get_iterator(dict_state);
    uint64_t now = (uint64_t)(current_timestamp() * 1000);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        struct dict_status_val *val = entry->val;
        if (market_is_index(market) || market_is_zone(market)) {
            if (val->last_state == NULL) {
                continue;
            }
            if (val->last_depth == NULL) {
                val->last_depth = create_init_depth();
            }
        } else {
            if (val->last_state == NULL || val->last_depth == NULL) {
                continue;
            }
        }

        json_t *params = json_object();
        json_object_set_new(params, "name", json_string(market));
        json_object_set_new(params, "date", json_integer(now));
        json_object_set    (params, "state", val->last_state);
        json_object_set    (params, "depth", val->last_depth);
        json_array_append_new(result, params);
    }
    dict_release_iterator(iter);

    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set_new(reply, "result", result);
    json_object_set_new(reply, "id", json_integer(0));

    char *message = json_dumps(reply, 0);
    size_t message_len = strlen(message);
    json_decref(reply);

    iter = dict_get_iterator(dict_session);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        rpc_push_date(ses, CMD_CACHE_STATUS_UPDATE, message, message_len);
    }
    dict_release_iterator(iter);
    free(message);
}

static void on_timeout(nw_state_entry *entry)
{
    struct state_data *state = entry->data;
    log_error("query timeout, state id: %u, command: %u", entry->id, state->cmd);
    if(nw_state_count(state_context) == 1) {
        notify_state();
    }

    if (state->cmd == CMD_ORDER_DEPTH) {
        profile_inc("query_depth_timeout", 1);
    } else if (state->cmd == CMD_MARKET_STATUS) {
        profile_inc("query_state_timeout", 1);
    }

    return;
}

static int status_sub_reply(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct dict_status_val val;
        memset(&val, 0, sizeof(val));
        entry = dict_add(dict_state, (char *)market, &val);
    }

    struct dict_status_val *obj = entry->val;
    if (obj->last_state)
        json_decref(obj->last_state);
    obj->last_state = result;
    json_incref(result);

    return 0;
}

static int order_depth_reply(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct dict_status_val val;
        memset(&val, 0, sizeof(val));
        entry = dict_add(dict_state, (char *)market, &val);
    }

    json_t *depth = json_object();
    json_t *bids = json_object_get(result, "bids");
    if (json_array_size(bids) == 1) {
        json_t *buy = json_array_get(bids, 0);
        json_object_set(depth, "buy", json_array_get(buy, 0));
        json_object_set(depth, "buy_amount", json_array_get(buy, 1));
    } else {
        json_object_set_new(depth, "buy", json_string("0"));
        json_object_set_new(depth, "buy_amount", json_string("0"));
    }

    json_t *asks = json_object_get(result, "asks");
    if (json_array_size(asks) == 1) {
        json_t *sell = json_array_get(asks, 0);
        json_object_set(depth, "sell", json_array_get(sell, 0));
        json_object_set(depth, "sell_amount", json_array_get(sell, 1));
    } else {
        json_object_set_new(depth, "sell", json_string("0"));
        json_object_set_new(depth, "sell_amount", json_string("0"));
    }

    struct dict_status_val *obj = entry->val;
    if (obj->last_depth)
        json_decref(obj->last_depth);
    obj->last_depth = depth;

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

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        log_error("nw_state_get get null");
        return;
    }

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_error("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    bool is_error = false;
    struct state_data *state = entry->data;
    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        is_error = true;
        sdsfree(reply_str);
    }

    switch (pkg->command) {
    case CMD_MARKET_STATUS:
        if (!is_error) {
            status_sub_reply(state->market, result);
            profile_inc("state_reply_success", 1);
        } else {
            profile_inc("state_reply_error", 1);
        }
        break;
     case CMD_ORDER_DEPTH:
        if (!is_error) {
            order_depth_reply(state->market, result);
            profile_inc("depth_reply_success", 1);
        } else {
            profile_inc("depth_reply_error", 1);
        }
        break;
    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    json_decref(reply);
    nw_state_del(state_context, pkg->sequence);

    if(nw_state_count(state_context) == 0) {
        notify_state();
    }

    return;
}

static int query_market_status(const char *market)
{
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    sstrncpy(state->market, market, sizeof(state->market));
    state->cmd = CMD_MARKET_STATUS;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(86400));

    rpc_request_json(marketprice, state->cmd, state_entry->id, 0, params);
    json_decref(params);
    profile_inc("request_state", 1);

    return 0;
}

static int query_market_depth(const char *market)
{
    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(1));
    json_array_append_new(params, json_string("0"));
    json_array_append_new(params, json_integer(0));

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    sstrncpy(state->market, market, sizeof(state->market));
    state->cmd = CMD_ORDER_DEPTH;

    uint32_t unique_id = dict_generic_hash_function(market, strlen(market));
    rpc_request_json_unique(matchengine, state->cmd, state_entry->id, 0, unique_id, params);
    json_decref(params);
    profile_inc("request_depth", 1);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata) 
{
    dict_entry *entry = NULL;
    dict_iterator *iter = NULL;
    dict_t *dict_market = get_market();

    // update market list
    iter = dict_get_iterator(dict_state);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        if (dict_find(dict_market, market) == NULL) {
            dict_delete(dict_state, market);
        }
    }
    dict_release_iterator(iter);

    // query depth and state data
    iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        if (!market_is_index(market) && !market_is_zone(market)) {
            query_market_depth(market);
        }
        query_market_status(market);
    }
    dict_release_iterator(iter);
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    dict_add(dict_session, ses, NULL);
    log_info("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    dict_delete(dict_session, ses);
    log_info("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

int init_status(void)
{
    // cli to marketprice
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL)
        return -__LINE__;
    if (rpc_clt_start(marketprice) < 0)
        return -__LINE__;

    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL)
        return -__LINE__;
    if (rpc_clt_start(matchengine) < 0)
        return -__LINE__;

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;
    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL)
        return -__LINE__;

    // state_svr
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg         = svr_on_recv_pkg;
    type.on_new_connection   = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    state_svr = rpc_svr_create(&settings.state_svr, &type);
    if (state_svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(state_svr) < 0)
        return -__LINE__;

    // sub session
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = ptr_dict_hash_func;
    dt.key_compare   = ptr_dict_key_compare;
    dict_session     = dict_create(&dt, 32);
    if (dict_session == NULL)
        return -__LINE__;

    // dict_state
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = str_dict_hash_function;
    dt.key_compare     = str_dict_key_compare;
    dt.key_dup         = str_dict_key_dup;
    dt.key_destructor  = str_dict_key_free;
    dt.val_dup         = dict_status_sub_val_dup;
    dt.val_destructor  = dict_status_sub_val_free;
    dict_state = dict_create(&dt, 256);
    if (dict_state == NULL)
        return -__LINE__;

    nw_timer_set(&timer, settings.status_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

