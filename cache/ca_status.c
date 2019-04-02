/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_deals.h"
# include "ca_cache.h"
# include "ca_server.h"

static rpc_clt *marketprice;
static nw_state *state_context;

static dict_t *dict_status_sub;
static nw_timer timer;

struct dict_status_key {
    char        market[MARKET_NAME_MAX_LEN];
    int         period;
};

struct dict_status_sub_val {
    dict_t      *sessions; 
    json_t      *sub_last;
};

struct state_data {
    char        market[MARKET_NAME_MAX_LEN];
    int         period;
    nw_ses      *ses;
    uint64_t    ses_id;
};

static uint32_t dict_status_sub_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_status_key));
}

static int dict_status_sub_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct dict_status_key));
}

static void *dict_status_sub_key_dup(const void *key)
{
    struct dict_status_key *obj = malloc(sizeof(struct dict_status_key));
    memcpy(obj, key, sizeof(struct dict_status_key));
    return obj;
}

static void dict_status_sub_key_free(void *key)
{
    free(key);
}

static void *dict_status_sub_val_dup(const void *val)
{
    struct dict_status_sub_val *obj = malloc(sizeof(struct dict_status_sub_val));
    memcpy(obj, val, sizeof(struct dict_status_sub_val));
    return obj;
}

static void dict_status_sub_val_free(void *key)
{
    struct dict_status_sub_val *obj = key;
    if (obj->sessions != NULL)
        dict_release(obj->sessions);
    free(obj);
}

static dict_t* dict_create_status_session(void)
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
        reply_rpc.command = CMD_CACHE_STATUS;
        reply_time_out(state->ses, &reply_rpc);
    } else {
        log_error("ses id not equal");
    }
}

static bool process_cache(nw_ses *ses, rpc_pkg *pkg, const char *market, int period)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "status_%s_%d", market, period);

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

static void status_reply(nw_ses *ses, json_t *result)
{
    json_t *new_result = json_object();
    json_object_set_new(new_result, "ttl", json_integer(settings.cache_timeout));
    json_object_set(new_result, "cache_result", result);

    rpc_pkg reply_rpc;
    memset(&reply_rpc, 0, sizeof(reply_rpc));
    reply_rpc.command = CMD_CACHE_STATUS;
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

static int status_sub_reply(const char *market, int period, json_t *result)
{
    struct dict_status_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    key.period = period;

    dict_entry *entry = dict_find(dict_status_sub, &key);
    if (entry == NULL)
        return -__LINE__;

    struct dict_status_sub_val *obj = entry->val;
    char *last_str = NULL;

    if (obj->sub_last)
        last_str = json_dumps(obj->sub_last, JSON_SORT_KEYS);
    char *curr_str = json_dumps(result, JSON_SORT_KEYS);

    if (obj->sub_last == NULL || strcmp(last_str, curr_str) != 0) {
        if (obj->sub_last)
            json_decref(obj->sub_last);
        obj->sub_last = result;
        json_incref(result);

        dict_iterator *iter = dict_get_iterator(obj->sessions);
        while ((entry = dict_next(iter)) != NULL) {
            nw_ses *ses = entry->key;
            notify_message(ses, CMD_CACHE_STATUS_UPDATE, result);
        }
        dict_release_iterator(iter);
    }

    free(last_str);
    free(curr_str);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        return;
    }
    struct state_data *state = entry->data;

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

    switch (pkg->command) {
    case CMD_MARKET_STATUS:
        if (state->ses) { // out request
            if (state->ses->id == state->ses_id) {
                status_reply(state->ses, result);

                sds cache_key = sdsempty();
                cache_key = sdscatprintf(cache_key, "status_%s_%d", state->market, state->period);
                add_cache(cache_key, result);
                sdsfree(cache_key);
            } else {
                sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
                log_error("ses id not equal, reply: %s", reply_str);
                sdsfree(reply_str);
            }
        } else { // sub timer request
            status_sub_reply(state->market, state->period, result);
        }

        break;
    }

    json_decref(reply);
    nw_state_del(state_context, pkg->sequence);
}

int status_request(nw_ses *ses, rpc_pkg *pkg, const char *market, int period)
{
    if (ses != NULL && process_cache(ses, pkg, market, period))
        return 0;

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    state->period = period;

    state->ses = ses;
    state->ses_id = ses->id;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(period));

    rpc_pkg req_pkg;
    memset(&req_pkg, 0, sizeof(req_pkg));
    req_pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    req_pkg.command   = CMD_MARKET_STATUS;
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

static void on_sub_timer(nw_timer *timer, void *privdata) 
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_status_sub);

    while ((entry = dict_next(iter)) != NULL) {
        struct dict_status_sub_val *val = entry->val;
        if (dict_size(val->sessions) == 0) {
            continue;
        }

        struct dict_status_key *key = entry->key;
        status_request(NULL, NULL, key->market, key->period);
    }
    dict_release_iterator(iter);
}

int status_subscribe(nw_ses *ses, const char *market, int period)
{
    struct dict_status_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    key.period = period;

    dict_entry *entry = dict_find(dict_status_sub, &key);
    if (entry == NULL) {
        struct dict_status_sub_val val;
        memset(&val, 0, sizeof(val));

        val.sessions = dict_create_status_session();
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_status_sub, &key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct dict_status_sub_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    return 0;  
}

void status_unsubscribe(nw_ses *ses, const char *market, int period)
{
    struct dict_status_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    key.period = period;

    dict_entry *entry = dict_find(dict_status_sub, &key);
    if (entry == NULL)
        return;

    struct dict_status_sub_val *val = entry->val;
    dict_delete(val->sessions, ses);

    return;
}

void status_unsubscribe_all(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_status_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_status_sub_val *val = entry->val;
        dict_delete(val->sessions, ses);
    }
    dict_release_iterator(iter);

    return;
}

int init_status(void)
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
    dt.hash_function   = dict_status_sub_hash_function;
    dt.key_compare     = dict_status_sub_key_compare;
    dt.key_dup         = dict_status_sub_key_dup;
    dt.key_destructor  = dict_status_sub_key_free;
    dt.val_dup         = dict_status_sub_val_dup;
    dt.val_destructor  = dict_status_sub_val_free;

    dict_status_sub = dict_create(&dt, 512);
    if (dict_status_sub == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    nw_timer_set(&timer, settings.sub_status_interval, true, on_sub_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}
