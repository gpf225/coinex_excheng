/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# include "aw_config.h"
# include "aw_server.h"
# include "aw_depth.h"

# define CLEAN_INTERVAL 60

static dict_t *dict_depth;
static rpc_clt *longpoll = NULL;

struct depth_key {
    char market[MARKET_NAME_MAX_LEN];
    char interval[INTERVAL_MAX_LEN];
    uint32_t limit;
};

struct depth_val {
    dict_t *sessions;
    json_t *last;
    time_t  last_clean;
};

static uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

static int dict_ses_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

static uint32_t dict_depth_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct depth_key));
}

static int dict_depth_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct depth_key));
}

static void *dict_depth_key_dup(const void *key)
{
    struct depth_key *obj = malloc(sizeof(struct depth_key));
    memcpy(obj, key, sizeof(struct depth_key));
    return obj;
}

static void dict_depth_key_free(void *key)
{
    free(key);
}

static void *dict_depth_val_dup(const void *val)
{
    struct depth_val *obj = malloc(sizeof(struct depth_val));
    memcpy(obj, val, sizeof(struct depth_val));
    return obj;
}

static void dict_depth_val_free(void *val)
{
    struct depth_val *obj = val;
    dict_release(obj->sessions);
    if (obj->last)
        json_decref(obj->last);
    free(obj);
}

static json_t *get_list_diff(json_t *list1, json_t *list2, uint32_t limit, int side)
{
    if (list1 == NULL || list2 == NULL)
        return NULL;

    json_t *diff = json_array();
    mpd_t *price1  = NULL;
    mpd_t *amount1 = NULL;
    mpd_t *price2  = NULL;
    mpd_t *amount2 = NULL;

    size_t list1_size = json_array_size(list1);
    size_t list2_size = json_array_size(list2);
    size_t list1_pos = 0;
    size_t list2_pos = 0;

    while (list1_pos < list1_size && list2_pos < list2_size) {
        json_t *unit1 = json_array_get(list1, list1_pos);
        const char *unit1_price  = json_string_value(json_array_get(unit1, 0));
        const char *unit1_amount = json_string_value(json_array_get(unit1, 1));
        if (unit1_price == NULL || unit1_amount == NULL)
            goto error;
        price1 = decimal(unit1_price, 0);
        amount1 = decimal(unit1_amount, 0);
        if (price1 == NULL || amount1 == NULL)
            goto error;

        json_t *unit2 = json_array_get(list2, list2_pos);
        const char *unit2_price  = json_string_value(json_array_get(unit2, 0));
        const char *unit2_amount = json_string_value(json_array_get(unit2, 1));
        if (unit2_price == NULL || unit2_amount == NULL)
            goto error;
        price2 = decimal(unit2_price, 0);
        amount2 = decimal(unit2_amount, 0);
        if (price2 == NULL || amount2 == NULL)
            goto error;

        int cmp = mpd_cmp(price1, price2, &mpd_ctx) * side;
        if (cmp == 0) {
            list1_pos += 1;
            list2_pos += 1;
            if (mpd_cmp(amount1, amount2, &mpd_ctx) != 0) {
                json_array_append(diff, unit2);
            }
        } else if (cmp > 0) {
            list2_pos += 1;
            json_array_append(diff, unit2);
        } else {
            list1_pos += 1;
            json_t *unit = json_array();
            json_array_append_new_mpd(unit, price1);
            json_array_append_new(unit, json_string("0"));
            json_array_append_new(diff, unit);
        }

        mpd_del(price1);
        price1  = NULL;
        mpd_del(amount1);
        amount1 = NULL;
        mpd_del(price2);
        price2  = NULL;
        mpd_del(amount2);
        amount2 = NULL;
    }

    while (list2_size < limit && list1_pos < list1_size) {
        json_t *unit = json_array_get(list1, list1_pos);
        const char *price = json_string_value(json_array_get(unit, 0));
        const char *amount = json_string_value(json_array_get(unit, 1));
        if (price == NULL || amount == NULL)
            goto error;
        json_t *new = json_array();
        json_array_append_new(new, json_string(price));
        json_array_append_new(new, json_string("0"));
        json_array_append_new(diff, new);
        list1_pos += 1;
    }

    for (;list2_pos < list2_size; ++list2_pos) {
        json_array_append(diff, json_array_get(list2, list2_pos));
    }

    if (json_array_size(diff) == 0) {
        json_decref(diff);
        return NULL;
    }

    return diff;

error:
    json_decref(diff);
    if (price1)
        mpd_del(price1);
    if (amount1)
        mpd_del(amount1);
    if (price2)
        mpd_del(price2);
    if (amount2)
        mpd_del(amount2);

    return NULL;
}

static json_t *get_depth_diff(json_t *first, json_t *second, uint32_t limit)
{
    json_t *asks = get_list_diff(json_object_get(first, "asks"), json_object_get(second, "asks"), limit,  1);
    json_t *bids = get_list_diff(json_object_get(first, "bids"), json_object_get(second, "bids"), limit, -1);
    if (asks == NULL && bids == NULL)
        return NULL;
    json_t *diff = json_object();
    if (asks)
        json_object_set_new(diff, "asks", asks);
    if (bids)
        json_object_set_new(diff, "bids", bids);
    return diff;
}

static int broadcast_update(const char *market, dict_t *sessions, bool clean, json_t *result)
{
    json_t *params = json_array();
    json_array_append_new(params, json_boolean(clean));
    json_array_append(params, result);
    json_array_append_new(params, json_string(market));

    dict_iterator *iter = dict_get_iterator(sessions);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        send_notify(entry->key, "depth.update", params);
    }
    dict_release_iterator(iter);
    json_decref(params);
    profile_inc("depth.update", dict_size(sessions));

    return 0;
}

static void depth_set_key(struct depth_key *key, const char *market, const char *interval, uint32_t limit)
{
    memset(key, 0, sizeof(struct depth_key));
    strncpy(key->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key->interval, interval, INTERVAL_MAX_LEN - 1);
    key->limit = limit;
}

static json_t* get_depth_json(struct depth_key *key)
{
    json_t *item = json_array();
    json_array_append_new(item, json_string(key->market));
    json_array_append_new(item, json_string(key->interval));
    json_array_append_new(item, json_integer(key->limit));
    return item;
}

static void longpoll_send_request(int command, json_t *params)
{ 
    if (!rpc_clt_connected(longpoll)) {
        return ;
    }
    static uint32_t sequence = 0;
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = command;
    pkg.sequence  = ++sequence;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(longpoll, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s", nw_sock_human_addr(rpc_clt_peer_addr(longpoll)), pkg.command, pkg.sequence, (char *)pkg.body);

    free(pkg.body);
}

static void longpoll_subscribe(struct depth_key *key)
{
    json_t *params = json_array();
    json_array_append_new(params, get_depth_json(key));

    longpoll_send_request(CMD_LP_DEPTH_SUBSCRIBE, params);   
    json_decref(params);
}

static void longpoll_unsubscribe(struct depth_key *key)
{
    json_t *params = json_array();
    json_array_append_new(params, get_depth_json(key));

    longpoll_send_request(CMD_LP_DEPTH_UNSUBSCRIBE, params);   
    json_decref(params);
}

static void longpoll_subscribe_all(void) 
{
    if (dict_size(dict_depth) == 0) {
        return ;
    }
    
    json_t *params = json_array();
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth);
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_key *key = entry->key;
        json_array_append_new(params, get_depth_json(key));
    }

    longpoll_send_request(CMD_LP_DEPTH_SUBSCRIBE, params);   
    dict_release_iterator(iter);
    json_decref(params);
}

static int on_depth_update(json_t *result)
{
    const char *market = json_string_value(json_object_get(result, "market"));
    const char *interval = json_string_value(json_object_get(result, "interval"));
    const int limit = json_integer_value(json_object_get(result, "limit"));
    json_t *depth_data = json_object_get(result, "data");
    if ( (market == NULL) || (interval == NULL) || (depth_data == NULL) ) {
        return -__LINE__;
    }
    
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);

    dict_entry *entry = dict_find(dict_depth, &key);
    if (entry == NULL) {
        log_warn("depth_item[%s-%s-%d] not subscribed", key.market, key.interval, key.limit);
        return -__LINE__;
    }

    struct depth_val *val = entry->val;
    if (val->last == NULL) {
        val->last = depth_data;
        val->last_clean = time(NULL);
        json_incref(depth_data);
        return broadcast_update(key.market, val->sessions, true, depth_data);
    }

    json_t *diff = get_depth_diff(val->last, depth_data, key.limit);
    if (diff == NULL) {
        return 0;
    }

    json_decref(val->last);
    val->last = depth_data;
    json_incref(depth_data);

    time_t now = time(NULL);
    if (now - val->last_clean >= CLEAN_INTERVAL) {
        val->last_clean = now; 
        broadcast_update(key.market, val->sessions, true, depth_data);
    } else {
        broadcast_update(key.market, val->sessions, false, diff);
    }
    json_decref(diff);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u, reply: %s", 
        nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, reply_str);

    ut_rpc_reply_t *rpc_reply = reply_load(pkg->body, pkg->body_size);
    do {
        if (!reply_valid(rpc_reply)) {
            REPLY_INVALID_LOG(ses, pkg);
            break;
        }
        if (!reply_ok(rpc_reply)) {
            REPLY_ERROR_LOG(ses, pkg);
            break;
        }
        
        if (pkg->command == CMD_LP_DEPTH_UPDATE) {
            int ret = on_depth_update(rpc_reply->result);
            if (ret < 0) {
                log_error("on_depth_update: %d, reply: %s", ret, reply_str);
            }
        } else if (pkg->command == CMD_LP_DEPTH_SUBSCRIBE) {
            log_trace("market depth subscribe success");
        } else if (pkg->command == CMD_LP_DEPTH_UNSUBSCRIBE) {
            log_trace("market depth unsubscribe success");   
        } else {
            log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        }
    } while(0);

    reply_release(rpc_reply);
    sdsfree(reply_str);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    if (result) {
        log_info("connect to longpoll success");
        longpoll_subscribe_all();
    } else {
        log_error("can not connect to longpoll...");
    }
}

int init_depth(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_val_dup;
    dt.val_destructor = dict_depth_val_free;

    dict_depth = dict_create(&dt, 64);
    if (dict_depth == NULL) {
        return -__LINE__;
    }
    
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    longpoll = rpc_clt_create(&settings.longpoll, &ct);
    if (longpoll == NULL) {
        return -__LINE__;
    }
    if (rpc_clt_start(longpoll) < 0){
        return -__LINE__;
    }

    return 0;
}

int depth_subscribe(nw_ses *ses, const char *market, uint32_t limit, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);

    dict_entry *entry = dict_find(dict_depth, &key);
    if (entry == NULL) {
        struct depth_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = dict_ses_hash_func;
        dt.key_compare = dict_ses_key_compare;
        val.sessions = dict_create(&dt, 1024);
        if (val.sessions == NULL) {
            return -__LINE__;
        }

        entry = dict_add(dict_depth, &key, &val);
        if (entry == NULL) {
            return -__LINE__;
        }
        dict_add(val.sessions, ses, NULL);
        log_info("ses:%p subscribe market:%s interval:%s limit:%u", ses, key.market, key.interval, key.limit);
        longpoll_subscribe(&key);
        return 0;
    }

    struct depth_val *obj = entry->val;
    if (dict_size(obj->sessions) == 0) {
        log_info("ses:%p subscribe market:%s interval:%s limit:%u", ses, key.market, key.interval, key.limit);
        longpoll_subscribe(&key);
    }
    dict_add(obj->sessions, ses, NULL);

    return 0;
}

int depth_unsubscribe(nw_ses *ses)
{
    dict_iterator *iter = dict_get_iterator(dict_depth);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *obj = entry->val;
        if (dict_delete(obj->sessions, ses) == 1) {
            if (dict_size(obj->sessions) == 0) {
                struct depth_key *key = entry->key;
                log_info("ses:%p unsubscribe market:%s interval:%s limit:%u", ses, key->market, key->interval, key->limit);
                longpoll_unsubscribe(key);
            }
        }
    }
    dict_release_iterator(iter);

    return 0;
}

int depth_send_clean(nw_ses *ses, const char *market, uint32_t limit, const char *interval)
{
    struct depth_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key.interval, interval, INTERVAL_MAX_LEN - 1);
    key.limit = limit;

    dict_entry *entry = dict_find(dict_depth, &key);
    if (entry == NULL)
        return 0;

    struct depth_val *obj = entry->val;
    if (obj->last) {
        json_t *params = json_array();
        json_array_append_new(params, json_boolean(true));
        json_array_append(params, obj->last);
        json_array_append(params, json_string(market));
        send_notify(ses, "depth.update", params);
        json_decref(params);
        profile_inc("depth.update", 1);
    }

    return 0;
}

size_t depth_subscribe_number(void)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_depth);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *obj = entry->val;
        count += dict_size(obj->sessions);
    }
    dict_release_iterator(iter);

    return count;
}