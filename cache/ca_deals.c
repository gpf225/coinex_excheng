/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_deals.h"
# include "ca_cache.h"
# include "ca_server.h"
# include "ca_market.h"

static rpc_clt *marketprice;
static nw_state *state_context;

static dict_t *dict_deals_sub;
static nw_timer timer;

# define DEALS_QUERY_LIMIT 100

struct dict_deals_key {
    char market[MARKET_NAME_MAX_LEN];
};

struct dict_deals_sub_val {
    dict_t   *sessions; 
    uint64_t last_id;
};

struct state_data {
    char     market[MARKET_NAME_MAX_LEN];
    int      limit;
    uint64_t last_id;
    nw_ses   *ses;
    uint64_t ses_id;
};

static uint32_t dict_deals_sub_hash_function(const void *key)
{
    const struct dict_deals_key *obj = key;
    return dict_generic_hash_function(key, strlen(obj->market));
}

static int dict_deals_sub_key_compare(const void *key1, const void *key2)
{
    const struct dict_deals_key *obj1 = key1;
    const struct dict_deals_key *obj2 = key1;

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
    struct dict_deals_sub_val *obj = malloc(sizeof(struct dict_deals_sub_val));
    memcpy(obj, val, sizeof(struct dict_deals_sub_val));
    return obj;
}

static void dict_deals_sub_val_free(void *key)
{
    struct dict_deals_sub_val *obj = key;
    if (obj->sessions != NULL)
        dict_release(obj->sessions);
    free(obj);   
}

static dict_t* dict_create_deals_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_hash_compare;

    return dict_create(&dt, 16);
}

static void on_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
    struct state_data *state = entry->data;
    if (state->ses->id == state->ses_id) {
        rpc_pkg reply_rpc;
        memset(&reply_rpc, 0, sizeof(reply_rpc));
        reply_rpc.command = CMD_CACHE_DEALS;
        reply_time_out(state->ses, &reply_rpc);
    } else {
        log_error("ses id not equal");
    }
}

static bool process_cache(nw_ses *ses, rpc_pkg *pkg, const char *market, int limit, uint64_t last_id)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "deals-%s-%d-%ld", market, limit, last_id);

    struct dict_cache_val *cache = get_cache(key, settings.cache_timeout);
    if (cache == NULL) {
        sdsfree(key);
        return false;
    }

    double now = current_timestamp();
    int ttl = now - cache->time;

    json_t *new_result = json_object();
    json_object_set_new(new_result, "ttl", json_integer(ttl));
    json_object_set(new_result, "cache_result", cache->result);

    reply_result(ses, pkg, new_result);
    sdsfree(key);
    json_decref(new_result);

    return true;
}

static void deals_reply(nw_ses *ses, json_t *result)
{
    json_t *new_result = json_object();
    json_object_set_new(new_result, "ttl", json_integer(settings.cache_timeout));
    json_object_set(new_result, "cache_result", result);

    rpc_pkg reply_rpc;
    memset(&reply_rpc, 0, sizeof(reply_rpc));
    reply_rpc.command = CMD_CACHE_DEALS;
    reply_result(ses, &reply_rpc, new_result);
    json_decref(new_result);

    return;
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

static int deals_sub_reply(const char *market, json_t *result)
{
    struct dict_deals_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_deals_sub, &key);
    if (entry == NULL)
        return -__LINE__;

    struct dict_deals_sub_val *obj = entry->val;

    if (!json_is_array(result))
        return -__LINE__;
    size_t array_size = json_array_size(result);
    if (array_size == 0)
        return 0;

    json_t *first = json_array_get(result, 0);
    uint64_t id = json_integer_value(json_object_get(first, "id"));
    if (id == 0)
        return -__LINE__;
    obj->last_id = id;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append(params, result);

    dict_iterator *iter = dict_get_iterator(obj->sessions);
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
        return;
    }

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    struct state_data *state = entry->data;

    switch (pkg->command) {
    case CMD_MARKET_DEALS:
        if (state->ses) { // out request
            if (state->ses->id == state->ses_id) {
                deals_reply(state->ses, result);

                sds cache_key = sdsempty();
                cache_key = sdscatprintf(cache_key, "deals-%s-%d-%ld", state->market, state->limit, state->last_id);
                add_cache(cache_key, result);
                sdsfree(cache_key);
            } else {
                sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
                log_error("ses id not equal, reply: %s", reply_str);
                sdsfree(reply_str);
            }
        } else { // sub timer request
            deals_sub_reply(state->market, result);
        }
        
        break;
    }

    json_decref(reply);
    nw_state_del(state_context, pkg->sequence);
}

int deals_request(nw_ses *ses, rpc_pkg *pkg, const char *market, int limit, uint64_t last_id)
{
    if (ses != NULL && process_cache(ses, pkg, market, limit, last_id))
        return 0;

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    state->limit = limit;
    state->last_id = last_id;

    state->ses = ses;
    state->ses_id = ses->id;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(limit));
    json_array_append_new(params, json_integer(last_id));

    rpc_pkg req_pkg;
    memset(&req_pkg, 0, sizeof(req_pkg));
    req_pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    req_pkg.command   = CMD_MARKET_DEALS;
    req_pkg.sequence  = state_entry->id;
    req_pkg.body      = json_dumps(params, 0);
    req_pkg.body_size = strlen(req_pkg.body);

    rpc_clt_send(marketprice, &req_pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(marketprice)), req_pkg.command, req_pkg.sequence, (char *)req_pkg.body);
    free(req_pkg.body);
    json_decref(params);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata) 
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_deals_sub);

    while ((entry = dict_next(iter)) != NULL) {
        struct dict_deals_key *key = entry->key;
        struct dict_deals_sub_val *val = entry->val;
        if (dict_size(val->sessions) == 0 || !market_exist(key->market))
            continue;

        deals_request(NULL, NULL, key->market, DEALS_QUERY_LIMIT, val->last_id);
    }
    dict_release_iterator(iter);
}

int deals_subscribe(nw_ses *ses, const char *market)
{
    struct dict_deals_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_deals_sub, &key);
    if (entry == NULL) {
        struct dict_deals_sub_val val;
        memset(&val, 0, sizeof(val));

        val.sessions = dict_create_deals_session();
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_deals_sub, &key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct dict_deals_sub_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    return 0;  
}

void deals_unsubscribe(nw_ses *ses, const char *market)
{
    struct dict_deals_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_deals_sub, &key);
    if (entry == NULL)
        return;

    struct dict_deals_sub_val *val = entry->val;
    dict_delete(val->sessions, ses);

    return;
}

void deals_unsubscribe_all(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_deals_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_deals_sub_val *val = entry->val;
        dict_delete(val->sessions, ses);
    }
    dict_release_iterator(iter);

    return;
}

int init_deals(void)
{
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

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = dict_deals_sub_hash_function;
    dt.key_compare     = dict_deals_sub_key_compare;
    dt.key_dup         = dict_deals_sub_key_dup;
    dt.key_destructor  = dict_deals_sub_key_free;
    dt.val_dup         = dict_deals_sub_val_dup;
    dt.val_destructor  = dict_deals_sub_val_free;

    dict_deals_sub = dict_create(&dt, 512);
    if (dict_deals_sub == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    nw_timer_set(&timer, settings.sub_deals_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}



