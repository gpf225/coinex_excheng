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

struct dict_deals_key {
    char market[MARKET_NAME_MAX_LEN];
};

struct dict_deals_val {
    uint64_t last_id;
    list_t   *deals;
};

struct state_data {
    char     market[MARKET_NAME_MAX_LEN];
};

static uint32_t dict_deals_sub_hash_function(const void *key)
{
    const struct dict_deals_key *obj = key;
    return dict_generic_hash_function(obj->market, strlen(obj->market));
}

static int dict_deals_sub_key_compare(const void *key1, const void *key2)
{
    const struct dict_deals_key *obj1 = key1;
    const struct dict_deals_key *obj2 = key2;

    if (strcmp(obj1->market, obj2->market) == 0) {
        return 0;
    } else {
        return 1;
    }
}

static void *dict_deals_sub_key_dup(const void *key)
{
    struct dict_deals_key *obj = malloc(sizeof(struct dict_deals_key));
    memcpy(obj, key, sizeof(struct dict_deals_key));
    return obj;
}

static void dict_deals_sub_key_free(void *key)
{
    free(key);
}

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
        list_release(obj->deals);
    free(obj);   
}

static void list_free(void *value)
{
    json_decref(value);
}

static dict_t* dict_create_deals_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare   = dict_ses_hash_compare;

    return dict_create(&dt, 16);
}

static void on_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
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

static int notify_message(nw_ses *ses, int command, json_t *message)
{
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.command = command;

    return reply_result(ses, &pkg, message);
}

static int deals_reply(const char *market, json_t *result)
{
    struct dict_deals_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_deals, &key);
    if (entry == NULL)
        return -__LINE__;

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
    obj->last_id = id;

    for (size_t i = array_size; i > 0; --i) {
        json_t *deal = json_array_get(result, i - 1);
        json_incref(deal);
        list_add_node_head(obj->deals, deal);
    }

    while (obj->deals->len > settings.deal_max) {
        list_del(obj->deals, list_tail(obj->deals));
    }

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append(params, result);

    dict_iterator *iter = dict_get_iterator(dict_session);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        notify_message(ses, CMD_CACHE_DEALS_UPDATE, params);
    }
    dict_release_iterator(iter);

    json_decref(params);
    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        log_error("nw_state_get get null");
        return;
    }

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

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, market: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), state->market, pkg->command, reply_str);
        sdsfree(reply_str);
        is_error = true;
    }

    switch (pkg->command) {
    case CMD_MARKET_DEALS:
        if (!is_error) {
            deals_reply(state->market, result);
        }
        break;

    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    json_decref(reply);
    nw_state_del(state_context, pkg->sequence);
}

static int deals_request(const char *market, uint64_t last_id)
{
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(settings.deal_max));
    json_array_append_new(params, json_integer(last_id));

    rpc_pkg req_pkg;
    memset(&req_pkg, 0, sizeof(req_pkg));
    req_pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    req_pkg.command   = CMD_MARKET_DEALS;
    req_pkg.sequence  = state_entry->id;
    req_pkg.body      = json_dumps(params, 0);
    req_pkg.body_size = strlen(req_pkg.body);

    rpc_clt_send(marketprice, &req_pkg);
    free(req_pkg.body);
    json_decref(params);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata) 
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_deals);

    while ((entry = dict_next(iter)) != NULL) {
        struct dict_deals_key *key = entry->key;
        struct dict_deals_val *val = entry->val;

        log_trace("deal sub request, market: %s, last_id: %zd", key->market, val->last_id);
        deals_request(key->market, val->last_id);
    }
    dict_release_iterator(iter);
}

int add_deals_market(const char *market)
{
    log_info("deals subscribe, market: %s", market);
    struct dict_deals_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_deals, &key);
    if (entry == NULL) {
        struct dict_deals_val val;
        memset(&val, 0, sizeof(val));

        list_type lt;
        memset(&lt, 0, sizeof(lt));
        lt.free = list_free;

        val.deals = list_create(&lt);
        if (val.deals == NULL)
            return -__LINE__;

        entry = dict_add(dict_deals, &key, &val);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    return 0;  
}

void delete_deals_market(const char *market)
{
    log_info("deals subscribe, market: %s", market);
    struct dict_deals_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_delete(dict_deals, &key);
}

static int send_market_deals(nw_ses *ses, const char *market)
{
    struct dict_deals_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_deals, &key);
    if (entry == NULL)
        return -__LINE__;
    struct dict_deals_val *obj = entry->val;
    if (obj->deals->len == 0)
        return 0;

    int count = 0;
    json_t *deals = json_array();
    list_node *node;

    list_iter *iter = list_get_iterator(obj->deals, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        json_array_append(deals, node->value);
        count++;
    }
    list_release_iterator(iter);

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, deals);
    notify_message(ses, CMD_CACHE_DEALS_UPDATE, params);

    json_decref(params);
    return 0;
}

static void send_full_deals(nw_ses *ses)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_deals);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_deals_key *key = entry->key;
        send_market_deals(ses, key->market);
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
    // cli to marketprice
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL) {
        log_stderr("rpc_clt_create failed");
        return -__LINE__;
    }
    if (rpc_clt_start(marketprice) < 0) {
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

    dict_session = dict_create_deals_session();
    if (dict_session == NULL)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = dict_deals_sub_hash_function;
    dt.key_compare     = dict_deals_sub_key_compare;
    dt.key_dup         = dict_deals_sub_key_dup;
    dt.key_destructor  = dict_deals_sub_key_free;
    dt.val_dup         = dict_deals_sub_val_dup;
    dt.val_destructor  = dict_deals_sub_val_free;

    dict_deals = dict_create(&dt, 256);
    if (dict_deals == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    nw_timer_set(&timer, settings.sub_deals_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

