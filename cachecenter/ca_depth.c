/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_depth.h"
# include "ca_market.h"
# include "ca_server.h"
# include "ca_cache.h"
# include "ca_filter.h"

static rpc_clt *matchengine;
static nw_state *state_context;

static dict_t *dict_depth_sub;
static nw_timer timer;

struct dict_depth_key {
    char    market[MARKET_NAME_MAX_LEN + 1];
    char    interval[INTERVAL_MAX_LEN + 1];
};

struct dict_depth_sub_val {
    dict_t  *sessions; 
    json_t  *last;
    uint64_t time;
};

struct state_data {
    bool    direct_request;
    char    market[MARKET_NAME_MAX_LEN + 1];
    char    interval[INTERVAL_MAX_LEN + 1];
};

static uint32_t dict_depth_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_depth_key));
}

static int dict_depth_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct dict_depth_key));
}

static void *dict_depth_key_dup(const void *key)
{
    struct dict_depth_key *obj = malloc(sizeof(struct dict_depth_key));
    memcpy(obj, key, sizeof(struct dict_depth_key));
    return obj;
}

static void dict_depth_key_free(void *key)
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
    dt.hash_function = ptr_dict_hash_func;
    dt.key_compare   = ptr_dict_key_compare;

    return dict_create(&dt, 16);
}

static void get_sub_key(struct dict_depth_key *key, const char *market, const char *interval)
{
    memset(key, 0, sizeof(struct dict_depth_key));
    sstrncpy(key->market, market, sizeof(key->market));
    sstrncpy(key->interval, interval, sizeof(key->interval));
}

static sds get_cache_key(const char *market, const char *interval)
{
    sds key = sdsempty();
    return sdscatprintf(key, "depth_%s_%s", market, interval);
}

static void on_timeout(nw_state_entry *entry)
{
    profile_inc("query_depth_timeout", 1);
    struct state_data *state = entry->data;
    log_error("query timeout, state id: %u", entry->id);

    sds filter_key = get_cache_key(state->market, state->interval);
    delete_filter_queue(filter_key);
    sdsfree(filter_key);
}

static bool is_json_equal(json_t *old, json_t *new)
{
    if (!old || !new)
        return false;

    char *old_str = json_dumps(old, JSON_SORT_KEYS);
    char *new_str = json_dumps(new, JSON_SORT_KEYS);
    int ret = strcmp(old_str, new_str);
    free(old_str);
    free(new_str);

    return ret == 0;
}

static bool is_depth_equal(json_t *last, json_t *now)
{
    if (!last || !now)
        return false;

    if (!is_json_equal(json_object_get(last, "asks"), json_object_get(now, "asks")))
        return false;
    if (!is_json_equal(json_object_get(last, "bids"), json_object_get(now, "bids")))
        return false;

    return true;
}

static int depth_sub_reply(const char *market, const char *interval, json_t *result)
{
    struct dict_depth_key key;
    get_sub_key(&key, market, interval);  
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL)
        return -__LINE__;

    uint64_t now = current_millisecond();
    struct dict_depth_sub_val *val = entry->val;
    if (is_depth_equal(val->last, result) && now - val->time <= settings.depth_resend_timeout * 1000)
        return 0;

    if (val->last != NULL)
        json_decref(val->last);
    val->last = result;
    val->time = now;
    json_incref(val->last);

    json_t *result_body = json_object();
    json_object_set_new(result_body, "market", json_string(market));
    json_object_set_new(result_body, "interval", json_string(interval));
    json_object_set_new(result_body, "ttl", json_integer(settings.depth_interval * 1000));
    json_object_set    (result_body, "data", result);

    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set_new(reply, "result", result_body);
    json_object_set_new(reply, "id", json_integer(0));

    char *message = json_dumps(reply, 0);
    size_t message_len = strlen(message);
    json_decref(reply);

    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(val->sessions);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        rpc_push_date(ses, CMD_CACHE_DEPTH_UPDATE, message, message_len);
        count += 1;
    }
    dict_release_iterator(iter);
    free(message);
    profile_inc("depth_notify", count);

    return 0;
}

static int depth_send_last(nw_ses *ses, json_t *data, const char *market, const char *interval, uint64_t ttl)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "market", json_string(market));
    json_object_set_new(reply, "interval", json_string(interval));
    json_object_set_new(reply, "ttl", json_integer(ttl));
    json_object_set    (reply, "data", data);

    rpc_push_result(ses, CMD_CACHE_DEPTH_UPDATE, reply);
    json_decref(reply);

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

static void process_cache(struct state_data *state, sds filter_key, json_t *depth, uint64_t update_id)
{
    struct dict_cache_val *cache = get_cache(filter_key);
    if (cache && update_id == cache->update_id) {
        json_t *last = json_object_get(depth, "last");
        update_cache(cache, last);
    } else {
        add_cache(filter_key, depth, update_id);
    }
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    if (pkg->command != CMD_ORDER_DEPTH) {
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        return;
    }

    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL)
        return;

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    struct state_data *state = entry->data;
    sds filter_key = get_cache_key(state->market, state->interval);

    sds reply_str1 = sdsnewlen(pkg->body, pkg->body_size);
    log_error("error depth reply from: %s, market: %s, interval: %s cmd: %u, reply: %s", \
            nw_sock_human_addr(&ses->peer_addr), state->market, state->interval, pkg->command, reply_str1);
    sdsfree(reply_str1);

    bool is_error = false;
    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    uint64_t update_id = 0;
    if (error == NULL || !json_is_null(error) || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error depth reply from: %s, market: %s, interval: %s cmd: %u, reply: %s", \
                nw_sock_human_addr(&ses->peer_addr), state->market, state->interval, pkg->command, reply_str);
        sdsfree(reply_str);
        is_error = true;
        profile_inc("depth_reply_fail", 1);
    } else {
        update_id = json_integer_value(json_object_get(result, "update_id"));
        profile_inc("depth_reply_success", 1);
    }

    if (is_error) {
        if (state->direct_request)
            reply_filter_message(filter_key, is_error, error, result);
    } else {
        process_cache(state, filter_key, result, update_id);
        struct dict_cache_val *cache = get_cache(filter_key);
        if (state->direct_request) {
            reply_filter_message(filter_key, is_error, error, cache->result);
        } else {
            depth_sub_reply(state->market, state->interval, cache->result);
        }
    }

    json_decref(reply);
    sdsfree(filter_key);
    nw_state_del(state_context, pkg->sequence);
}

static bool check_cache(nw_ses *ses, rpc_pkg *pkg, struct dict_cache_val *cache, const char *interval)
{
    uint64_t now = current_millisecond();
    if (cache == NULL || cache->time + settings.depth_interval * 1000 < now)
        return false;

    uint64_t ttl = cache->time + settings.depth_interval * 1000 - now;
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set_new(reply, "id", json_integer(pkg->req_id));
    json_object_set_new(reply, "ttl", json_integer(ttl));
    json_object_set(reply, "result", cache->result);

    rpc_reply_json(ses, pkg, reply);
    json_decref(reply);
    profile_inc("depth_hit_cache", 1);
    return true;
}

int depth_request(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval)
{
    sds cache_key = get_cache_key(market, interval);
    struct dict_cache_val *cache = get_cache(cache_key);
    if (ses) {
        if (check_cache(ses, pkg, cache, interval)) {
            sdsfree(cache_key);
            return 0;
        }

        int ret = add_filter_queue(cache_key, ses, pkg);
        if (ret > 0) {
            sdsfree(cache_key);
            return 0;
        }
    }
    sdsfree(cache_key);

    uint64_t update_id = 0;
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    sstrncpy(state->market, market, sizeof(state->market));
    sstrncpy(state->interval, interval, sizeof(state->interval));
    state->direct_request = (ses != NULL) ? true : false;
    if (cache) {
        update_id = cache->update_id;
    }

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(settings.depth_limit_max));
    json_array_append_new(params, json_string(interval));
    json_array_append_new(params, json_integer(update_id));

    rpc_request_json(matchengine, CMD_ORDER_DEPTH, state_entry->id, 0, params);
    json_decref(params);
    profile_inc("request_depth", 1);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);

    while ((entry = dict_next(iter)) != NULL) {
        struct dict_depth_key *key = entry->key;
        struct dict_depth_sub_val *val = entry->val;
        if (dict_size(val->sessions) == 0 || !market_exist(key->market)) {
            sds cache_key = get_cache_key(key->market, key->interval);
            delete_cache(cache_key);
            sdsfree(cache_key);
            dict_delete(dict_depth_sub, entry->key);
            continue;
        }
        log_trace("depth sub request, market: %s, interval: %s", key->market, key->interval);
        depth_request(NULL, NULL, key->market, key->interval);
    }
    dict_release_iterator(iter);
}

int depth_subscribe(nw_ses *ses, const char *market, const char *interval)
{
    log_info("depth subscribe from: %s, market: %s, interval: %s", nw_sock_human_addr(&ses->peer_addr), market, interval);
    struct dict_depth_key key;
    get_sub_key(&key, market, interval);

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
    if (obj->last) {
        uint64_t now = current_millisecond();
        if (now < (obj->time + settings.depth_interval * 1000)) {
            uint64_t ttl = obj->time + settings.depth_interval * 1000 - now;
            depth_send_last(ses, obj->last, market, interval, ttl);
        } else {
            depth_send_last(ses, obj->last, market, interval, 0);
        }
    }

    return 0;
}

int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval)
{
    log_info("depth unsubscribe from: %s, market: %s, interval: %s", nw_sock_human_addr(&ses->peer_addr), market, interval);
    struct dict_depth_key key;
    get_sub_key(&key, market, interval);

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry) {
        struct dict_depth_sub_val *val = entry->val;
        dict_delete(val->sessions, ses);
        log_trace("sessions size: %d", dict_size(val->sessions));
    }

    return 0;
}

void depth_unsubscribe_all(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_depth_sub_val *val = entry->val;
        dict_delete(val->sessions, ses);
    }
    dict_release_iterator(iter);
}

int init_depth(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

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

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = dict_depth_hash_func;
    dt.key_compare     = dict_depth_key_compare;
    dt.key_dup         = dict_depth_key_dup;
    dt.key_destructor  = dict_depth_key_free;
    dt.val_dup         = dict_depth_sub_val_dup;
    dt.val_destructor  = dict_depth_sub_val_free;

    dict_depth_sub = dict_create(&dt, 512);
    if (dict_depth_sub == NULL)
        return -__LINE__;

    nw_timer_set(&timer, settings.depth_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

size_t depth_subscribe_number(void)
{
    return dict_size(dict_depth_sub);
}

