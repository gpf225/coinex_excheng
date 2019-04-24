/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_depth.h"
# include "ca_market.h"
# include "ca_server.h"
# include "ca_cache.h"

static rpc_clt *matchengine;
static nw_state *state_context;

static dict_t *dict_depth_sub;
static nw_timer timer;

struct dict_depth_key {
    char      market[MARKET_NAME_MAX_LEN];
    char      interval[INTERVAL_MAX_LEN];
};

struct dict_depth_sub_val {
    dict_t   *sessions; 
    json_t   *last;
};

struct state_data {
    char      market[MARKET_NAME_MAX_LEN];
    char      interval[INTERVAL_MAX_LEN];
    int       limit;
    nw_ses    *ses;
    uint64_t  ses_id;
    uint32_t  sequence;
};

uint32_t dict_depth_sub_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_depth_key));
}

int dict_depth_sub_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct dict_depth_key));
}

void *dict_depth_sub_key_dup(const void *key)
{
    struct dict_depth_key *obj = malloc(sizeof(struct dict_depth_key));
    memcpy(obj, key, sizeof(struct dict_depth_key));
    return obj;
}

void dict_depth_sub_key_free(void *key)
{
    free(key);
}

static void *dict_depth_sub_val_dup(const void *val)
{
    struct dict_depth_sub_val *obj = malloc(sizeof(struct dict_depth_sub_val));
    memcpy(obj, val, sizeof(struct dict_depth_sub_val));
    return obj;
}

static void dict_depth_sub_val_free(void *key)
{
    struct dict_depth_sub_val *obj = key;
    if (obj->sessions != NULL)
        dict_release(obj->sessions);
    if (obj->last != NULL)
        json_decref(obj->last);

    free(obj);
}

static dict_t* dict_create_depth_session(void)
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

static int notify_message(nw_ses *ses, int command, json_t *message)
{
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.command = command;

    return reply_result(ses, &pkg, message);
}

static void depth_set_key(struct dict_depth_key *key, const char *market, const char *interval)
{
    memset(key, 0, sizeof(struct dict_depth_key));
    strncpy(key->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key->interval, interval, INTERVAL_MAX_LEN - 1);
}

static bool is_json_equal(json_t *l, json_t *r)
{
    if (l == NULL || r == NULL)
        return false;

    char *l_str = json_dumps(l, JSON_SORT_KEYS);
    char *r_str = json_dumps(r, JSON_SORT_KEYS);
    int ret = strcmp(l_str, r_str);

    free(l_str);
    free(r_str);
    return ret == 0;
}

static bool is_depth_equal(json_t *last, json_t *now)
{
    if (last == NULL || now == NULL)
        return false;

    if (!is_json_equal(json_object_get(last, "asks"), json_object_get(now, "asks")))
        return false;

    return is_json_equal(json_object_get(last, "bids"), json_object_get(now, "bids"));
}

static int depth_sub_reply(const char *market, const char *interval, json_t *result)
{
    struct dict_depth_key key;
    depth_set_key(&key, market, interval);  

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL)
        return -__LINE__;

    struct dict_depth_sub_val *val = entry->val;
    if (is_depth_equal(val->last, result))
        return 0;

    if (val->last != NULL)
        json_decref(val->last);

    val->last = result;
    json_incref(val->last);

    json_t *reply = json_object();
    json_object_set_new(reply, "market", json_string(market));
    json_object_set_new(reply, "interval", json_string(interval));
    json_object_set    (reply, "data", result);

    dict_iterator *iter = dict_get_iterator(val->sessions);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        notify_message(ses, CMD_CACHE_DEPTH_UPDATE, reply);
    }
    dict_release_iterator(iter);
    json_decref(reply);

    return 0;
}

static int depth_send_last(nw_ses *ses, const char *market, const char *interval)
{
    struct dict_depth_key key;
    depth_set_key(&key, market, interval);  

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL)
        return 0;

    struct dict_depth_sub_val *obj = entry->val;
    if (obj->last) {
        json_t *reply = json_object();
        json_object_set_new(reply, "market", json_string(market));
        json_object_set_new(reply, "interval", json_string(interval));
        json_object_set    (reply, "data", obj->last);
        notify_message(ses, CMD_CACHE_DEPTH_UPDATE, reply);
        json_decref(reply);
    }

    return 0;
}

static bool process_cache(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "depth_%s_%s", market, interval);

    struct dict_cache_val *cache = get_cache(key, settings.cache_timeout);
    if (cache == NULL) {
        sdsfree(key);
        return false;
    }

    uint64_t now = current_millis();
    int ttl = cache->time + settings.cache_timeout - now;

    json_t *new_result = json_object();
    json_object_set_new(new_result, "ttl", json_integer(ttl));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", cache->result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));
    json_object_set_new(new_result, "cache_result", reply);

    reply_json(ses, pkg, new_result);
    json_decref(new_result);
    sdsfree(key);

    return true;
}

static json_t *generate_depth_data(json_t *array, int limit) 
{
    if (array == NULL)
        return json_array();

    json_t *new_data = json_array();
    int size = json_array_size(array) > limit ? limit : json_array_size(array);
    for (int i = 0; i < size; ++i) {
        json_t *unit = json_array_get(array, i);
        json_array_append(new_data, unit);
    }

    return new_data;
}

static json_t *pack_depth_result(json_t *result, uint32_t limit)
{
    json_t *asks_array = json_object_get(result, "asks");
    json_t *bids_array = json_object_get(result, "bids");

    json_t *new_result = json_object();
    json_object_set_new(new_result, "asks", generate_depth_data(asks_array, limit));
    json_object_set_new(new_result, "bids", generate_depth_data(bids_array, limit));
    json_object_set    (new_result, "last", json_object_get(result, "last"));
    json_object_set    (new_result, "time", json_object_get(result, "time"));

    return new_result;
}

static void depth_reply(nw_ses *ses, int limit, json_t *result, bool is_error, uint32_t sequence)
{
    json_t *new_result = json_object();

    if (is_error) {
        json_object_set_new(new_result, "error", json_object_get(result, "error"));
        json_object_set_new(new_result, "result", json_object_get(result, "result"));
        json_object_set    (new_result, "id", json_object_get(result, "id"));
    } else {
        json_t *reply = json_object();
        json_t *result_inside = json_object_get(result, "result");
        json_object_set_new(reply, "error", json_null());
        json_object_set_new(reply, "result", pack_depth_result(result_inside, limit));
        json_object_set    (reply, "id", json_object_get(result, "id"));

        json_object_set_new(new_result, "ttl", json_integer(settings.cache_timeout));
        json_object_set_new(new_result, "cache_result", reply);
    }

    rpc_pkg reply_rpc;
    memset(&reply_rpc, 0, sizeof(reply_rpc));
    reply_rpc.command = CMD_CACHE_DEPTH;
    reply_rpc.sequence = sequence;
    reply_json(ses, &reply_rpc, new_result);
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
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
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
        sdsfree(reply_str);
        is_error = true;
    }

    switch (pkg->command) {
    case CMD_ORDER_DEPTH:
         if (state->ses) { // out request
            if (state->ses->id == state->ses_id) {
                depth_reply(state->ses, state->limit, reply, is_error, state->sequence);
            } else {
                sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
                log_error("ses id not equal, reply: %s", reply_str);
                sdsfree(reply_str);
                break;
            }
        }

        if (!is_error) {
            depth_sub_reply(state->market, state->interval, result);

            sds cache_key = sdsempty();
            cache_key = sdscatprintf(cache_key, "depth_%s_%s", state->market, state->interval);
            add_cache(cache_key, result);
            sdsfree(cache_key);
        }

        break;
    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    json_decref(reply);
    nw_state_del(state_context, pkg->sequence);
}

int depth_request(nw_ses *ses, rpc_pkg *pkg, const char *market, int limit, const char *interval)
{
    if (ses != NULL && process_cache(ses, pkg, market, interval))
        return 0;

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(state->interval, interval, INTERVAL_MAX_LEN - 1);
    state->limit = limit;

    if (ses != NULL) {
        state->ses = ses;
        state->ses_id = ses->id;
        state->sequence = pkg->sequence;
    }

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(settings.depth_limit_max));
    json_array_append_new(params, json_string(interval));

    rpc_pkg req_pkg;
    memset(&req_pkg, 0, sizeof(req_pkg));
    if (pkg != NULL) {
        req_pkg.req_id = pkg->req_id;
    }
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

static void on_timer(nw_timer *timer, void *privdata) 
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);

    while ((entry = dict_next(iter)) != NULL) {
        struct dict_depth_key *key = entry->key;
        struct dict_depth_sub_val *val = entry->val;

        if (dict_size(val->sessions) == 0) {
            log_info("detph sessions num is 0, market: %s", key->market);
            continue;
        }

        log_trace("depth sub request, market: %s, interval: %s", key->market, key->interval);
        depth_request(NULL, NULL, key->market, settings.depth_limit_max, key->interval);
    }
    dict_release_iterator(iter);
}

int depth_subscribe(nw_ses *ses, const char *market, const char *interval)
{
    log_info("depth subscribe, market: %s, interval: %s", market, interval);
    struct dict_depth_key key;
    depth_set_key(&key, market, interval);  

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        struct dict_depth_sub_val val;
        memset(&val, 0, sizeof(val));

        val.sessions = dict_create_depth_session();
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_depth_sub, &key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct dict_depth_sub_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);
    depth_send_last(ses, market, interval);

    return 0;  
}

int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval)
{
    log_info("depth unsubscribe, market: %s, interval: %s", market, interval);
    struct dict_depth_key key;
    depth_set_key(&key, market, interval);  

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL)
        return -__LINE__;

    struct dict_depth_sub_val *val = entry->val;
    dict_delete(val->sessions, ses);

    return 0;
}

int depth_unsubscribe_all(nw_ses *ses)
{
    log_info("depth unsubscribe_all, ses_id: %zd", ses->id);
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_depth_sub_val *val = entry->val;
        dict_delete(val->sessions, ses);
    }
    dict_release_iterator(iter);

    return 0;
}

int init_depth(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

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

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = dict_depth_sub_hash_func;
    dt.key_compare     = dict_depth_sub_key_compare;
    dt.key_dup         = dict_depth_sub_key_dup;
    dt.key_destructor  = dict_depth_sub_key_free;
    dt.val_dup         = dict_depth_sub_val_dup;
    dt.val_destructor  = dict_depth_sub_val_free;

    dict_depth_sub = dict_create(&dt, 512);
    if (dict_depth_sub == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    nw_timer_set(&timer, settings.sub_depth_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}


