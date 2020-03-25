/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_deals.h"
# include "ca_server.h"
# include "ca_market.h"

static rpc_clt *marketprice;
static nw_state *state_context;

static dict_t *dict_deals;
static dict_t *dict_session;

static nw_timer timer;
static rpc_svr *deals_svr;

struct dict_deals_val {
    uint64_t last_id;
    json_t   *deals;
};

struct state_data {
    char     market[MARKET_NAME_MAX_LEN + 1];
};

static void *dict_deals_sub_val_dup(const void *val)
{
    struct dict_deals_val *obj = malloc(sizeof(struct dict_deals_val));
    memcpy(obj, val, sizeof(struct dict_deals_val));
    return obj;
}

static void dict_deals_sub_val_free(void *key)
{
    struct dict_deals_val *obj = key;
    if (obj->deals)
        json_decref(obj->deals);
    free(obj);   
}

static void on_timeout(nw_state_entry *entry)
{
    log_error("query timeout, state id: %u", entry->id);
    profile_inc("query_deals_timeout", 1);
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

static int deals_reply(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry == NULL) {
        struct dict_deals_val val;
        memset(&val, 0, sizeof(val));

        entry = dict_add(dict_deals, (char *)market, &val);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    struct dict_deals_val *obj = entry->val;

    if (!json_is_array(result))
        return -__LINE__;
    size_t array_size = json_array_size(result);
    if (array_size == 0)
        return 0;

    json_t *first = json_array_get(result, 0);
    if (first == NULL || !json_is_integer(json_object_get(first, "id")))
        return -__LINE__;

    uint64_t id = json_integer_value(json_object_get(first, "id"));
    log_info("deal sub reply, market: %s, array_size: %zd, last_id: %zd", market, array_size, id);

    if (id == 0)
        return -__LINE__;

    double start = current_timestamp();
    obj->last_id = id;
    json_incref(result);
    if (obj->deals) {
        json_array_extend(result, obj->deals);
        json_decref(obj->deals);
    }
    obj->deals = result;

    while (json_array_size(obj->deals) > settings.deal_max) {
        json_array_remove(obj->deals, json_array_size(obj->deals) - 1);
    }
    double end = current_timestamp();
    log_info("market: %s, array size: %ld, cost: %lf", market, json_array_size(obj->deals), end - start);

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append(params, result);

    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set_new(reply, "result", params);
    json_object_set_new(reply, "id", json_integer(0));

    char *message = json_dumps(reply, 0);
    size_t message_len = strlen(message);
    json_decref(reply);

    dict_iterator *iter = dict_get_iterator(dict_session);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        rpc_push_date(ses, CMD_CACHE_DEALS_UPDATE, message, message_len);
    }
    dict_release_iterator(iter);
    free(message);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    if (pkg->command != CMD_MARKET_DEALS) {
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        return;
    }

    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL)
        return;
    struct state_data *state = entry->data;

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, market: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), state->market, pkg->command, hex);
        sdsfree(hex);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    bool is_error = false;
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("reply from: %s, market: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), state->market, pkg->command, reply_str);

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        log_error("error reply from: %s, market: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), state->market, pkg->command, reply_str);
        is_error = true;
    }

    if (!is_error) {
        profile_inc("deasl_reply_success", 1);
        deals_reply(state->market, result);
    } else {
        profile_inc("deasl_reply_fail", 1);
    }

    json_decref(reply);
    sdsfree(reply_str);
    nw_state_del(state_context, pkg->sequence);
}

static int deals_request(const char *market, uint64_t last_id)
{
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    sstrncpy(state->market, market, sizeof(state->market));

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(settings.deal_max));
    json_array_append_new(params, json_integer(last_id));

    rpc_request_json(marketprice, CMD_MARKET_DEALS, state_entry->id, 0, params);
    json_decref(params);
    profile_inc("request_deals", 1);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata) 
{
    dict_entry *entry;
    dict_t *dict_market = get_market();
    dict_iterator *iter = dict_get_iterator(dict_market);

    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;

        uint64_t last_id = 0;
        entry = dict_find(dict_deals, market);
        if (entry) {
            struct dict_deals_val *deal_val = entry->val;
            last_id = deal_val->last_id;
        }
        deals_request(market, last_id);
        log_trace("deal sub request, market: %s, last_id: %zd", market, last_id);
    }
    dict_release_iterator(iter);
}

static int send_market_deals(nw_ses *ses, const char *market)
{
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry == NULL) {
        return -__LINE__;
    }

    double start = current_timestamp();
    struct dict_deals_val *obj = entry->val;
    if (json_array_size(obj->deals) == 0) {
        return 0;
    }

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, obj->deals);
    rpc_push_result(ses, CMD_CACHE_DEALS_UPDATE, params);
    double end = current_timestamp();
    log_info("market: %s, cost: %lf", market, end - start);
    json_decref(params);
    return 0;
}

static void send_full_deals(nw_ses *ses)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_deals);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        send_market_deals(ses, market);
    }
    dict_release_iterator(iter);

    return;
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    dict_add(dict_session, ses, NULL);
    send_full_deals(ses);
    log_info("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    dict_delete(dict_session, ses);
    log_info("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

int init_deals(void)
{
    // marketprice
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL)
        return -__LINE__;
    if (rpc_clt_start(marketprice) < 0)
        return -__LINE__;

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;
    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL)
        return -__LINE__;

    // deals deals_svr
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg         = svr_on_recv_pkg;
    type.on_new_connection   = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    deals_svr = rpc_svr_create(&settings.deals_svr, &type);
    if (deals_svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(deals_svr) < 0)
        return -__LINE__;

    // sub session
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = ptr_dict_hash_func;
    dt.key_compare   = ptr_dict_key_compare;
    dict_session = dict_create(&dt, 32);
    if (dict_session == NULL)
        return -__LINE__;

    // dict_deals
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = str_dict_hash_function;
    dt.key_compare     = str_dict_key_compare;
    dt.key_dup         = str_dict_key_dup;
    dt.key_destructor  = str_dict_key_free;
    dt.val_dup         = dict_deals_sub_val_dup;
    dt.val_destructor  = dict_deals_sub_val_free;
    dict_deals = dict_create(&dt, 256);
    if (dict_deals == NULL)
        return -__LINE__;

    nw_timer_set(&timer, settings.deals_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

