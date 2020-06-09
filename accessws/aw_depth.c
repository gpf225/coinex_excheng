/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# include "ut_crc32.h"
# include "aw_config.h"
# include "aw_server.h"
# include "aw_depth.h"

# define CLEAN_INTERVAL 60

static dict_t *dict_depth_sub; 
static dict_t *dict_depth_sub_counter; 
static rpc_clt **cachecenter_clt_arr;

struct depth_key {
    char     market[MARKET_NAME_MAX_LEN + 1];
    char     interval[INTERVAL_MAX_LEN + 1];
    uint32_t limit;
};

struct depth_val {
    dict_t  *sessions;
    json_t  *last;
    time_t  last_clean;
};

struct depth_sub_counter{
    uint32_t count;
};

struct depth_session_val {
    bool is_full;
};

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
    if (obj->sessions != NULL)
        dict_release(obj->sessions);
    if (obj->last != NULL)
        json_decref(obj->last);
    free(obj);
}

static void *dict_depth_sub_counter_dup(const void *val)
{
    struct depth_sub_counter *obj = malloc(sizeof(struct depth_sub_counter));
    memcpy(obj, val, sizeof(struct depth_sub_counter));
    return obj;
}

static void dict_depth_sub_counter_free(void *val)
{
    free(val);
}

static void *dict_depth_session_val_dup(const void *val)
{
    struct depth_session_val *obj = malloc(sizeof(struct depth_session_val));
    memcpy(obj, val, sizeof(struct depth_session_val));
    return obj;
}

static void dict_depth_session_val_free(void *val)
{
    free(val);
}

static void depth_set_key(struct depth_key *key, const char *market, const char *interval, uint32_t limit)
{
    memset(key, 0, sizeof(struct depth_key));
    sstrncpy(key->market, market, sizeof(key->market));
    sstrncpy(key->interval, interval, sizeof(key->interval));
    key->limit = limit;
}

static int depth_sub_counter_inc(const char *market, const char *interval) 
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_sub_counter, &key);
    if (entry == NULL) {
        struct depth_sub_counter counter;
        memset(&counter, 0, sizeof(counter));

        entry = dict_add(dict_depth_sub_counter, &key, &counter);
        if (entry == NULL) {
            return -__LINE__;
        }
    }
    struct depth_sub_counter *counter = entry->val;
    ++counter->count;

    return counter->count;
}

static int depth_sub_counter_dec(const char *market, const char *interval) 
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_sub_counter, &key);
    if (entry != NULL) {
        struct depth_sub_counter *counter = entry->val;
        --counter->count;
        return counter->count;
    } else {
        log_error("entry is null");
        return -__LINE__;
    }
}

static json_t *get_list_diff(json_t *list1, json_t *list2, int side)
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

    while (list1_pos < list1_size) {
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

static json_t *get_depth_diff(json_t *first, json_t *second)
{
    json_t *asks = get_list_diff(json_object_get(first, "asks"), json_object_get(second, "asks"), 1);
    json_t *bids = get_list_diff(json_object_get(first, "bids"), json_object_get(second, "bids"), -1);
    if (asks == NULL && bids == NULL)
        return NULL;
    json_t *diff = json_object();
    if (asks)
        json_object_set_new(diff, "asks", asks);
    if (bids)
        json_object_set_new(diff, "bids", bids);
    return diff;
}

static rpc_clt *get_depth_clt(const char *market)
{
    uint32_t hash = dict_generic_hash_function(market, strlen(market));
    return cachecenter_clt_arr[hash % settings.cachecenter_worker_num];
}

static int send_depth_request(int command, struct depth_key *key)
{
    rpc_clt *clt = get_depth_clt(key->market);
    if (!rpc_clt_connected(clt))
        return -__LINE__;

    json_t *params = json_array();
    json_array_append_new(params, json_string(key->market));
    json_array_append_new(params, json_string(key->interval));

    rpc_request_json(clt, command, 0, 0, params);
    json_decref(params);

    return 0;
}

static int broadcast_update(const char *market, dict_t *sessions, bool clean, json_t *full, json_t *diff)
{
    dict_iterator *iter = dict_get_iterator(sessions);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        json_t *params = json_array();
        struct depth_session_val *ses_val = entry->val;
        if (clean || ses_val->is_full) {
            json_array_append_new(params, json_boolean(true));
            json_array_append(params, full);
        } else {
            json_array_append_new(params, json_boolean(false));
            json_array_append(params, diff);
        }
        json_array_append_new(params, json_string(market));
        ws_send_notify(entry->key, "depth.update", params);
        json_decref(params);
    }
    dict_release_iterator(iter);
    profile_inc("depth.update", dict_size(sessions));

    return 0;
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

static uint32_t general_depth_checksum(json_t *bids, json_t *asks)
{
    sds depth_str = sdsempty();
    if (bids != NULL) {
        int size = json_array_size(bids);
        for (int i = 0; i < size; ++i) {
            json_t *unit = json_array_get(bids, i);
            const char *price  = json_string_value(json_array_get(unit, 0));
            const char *amount = json_string_value(json_array_get(unit, 1));
            if (sdslen(depth_str) > 0) {
                depth_str = sdscat(depth_str, ":");
            }
            depth_str = sdscatprintf(depth_str, "%s:%s", price, amount);
        }
    }

    if (asks != NULL) {
        int size = json_array_size(asks);
        for (int i = 0; i < size; ++i) {
            json_t *unit = json_array_get(asks, i);
            const char *price  = json_string_value(json_array_get(unit, 0));
            const char *amount = json_string_value(json_array_get(unit, 1));
            if (sdslen(depth_str) > 0) {
                depth_str = sdscat(depth_str, ":");
            }
            depth_str = sdscatprintf(depth_str, "%s:%s", price, amount);
        }
    }

    uint32_t checksum = generate_crc32c(depth_str, sdslen(depth_str));
    sdsfree(depth_str);
    return checksum;
}

json_t *pack_depth_result(json_t *result, uint32_t limit)
{
    json_t *asks_array = json_object_get(result, "asks");
    json_t *bids_array = json_object_get(result, "bids");
    json_t *limit_asks_array = generate_depth_data(asks_array, limit);
    json_t *limit_bids_array = generate_depth_data(bids_array, limit);
    json_t *new_result = json_object();
    uint32_t checksum = general_depth_checksum(limit_bids_array, limit_asks_array);
    json_object_set_new(new_result, "asks", limit_asks_array);
    json_object_set_new(new_result, "bids", limit_bids_array);
    json_object_set    (new_result, "last", json_object_get(result, "last"));
    json_object_set    (new_result, "time", json_object_get(result, "time"));
    json_object_set_new(new_result, "checksum", json_integer(checksum));
    return new_result;
}

static int notify_depth(const char *market, const char *interval, uint32_t limit, json_t *result)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL)
        return 0;

    json_t *depth_result = pack_depth_result(result, limit);
    struct depth_val *val = entry->val;
    if (val->last == NULL) {
        val->last = depth_result;
        val->last_clean = time(NULL);
        return broadcast_update(key.market, val->sessions, true, depth_result, NULL);
    }
    json_t *diff = get_depth_diff(val->last, depth_result);
    if (diff == NULL) {
        json_decref(depth_result);
        return 0;
    }

    json_decref(val->last);
    val->last = depth_result;

    time_t now = time(NULL);
    if (now - val->last_clean >= CLEAN_INTERVAL) {
        val->last_clean = now; 
        broadcast_update(key.market, val->sessions, true, depth_result, NULL);
    } else {
        json_object_set(diff, "last", json_object_get(result, "last"));
        json_object_set(diff, "time", json_object_get(result, "time"));
        json_object_set(diff, "checksum", json_object_get(depth_result, "checksum"));
        broadcast_update(key.market, val->sessions, false, depth_result, diff);
    }
    json_decref(diff);

    return 0;
}

static int on_order_depth_update(json_t *result)
{
    const char *market = json_string_value(json_object_get(result, "market"));
    const char *interval = json_string_value(json_object_get(result, "interval"));
    int ttl = json_integer_value(json_object_get(result, "ttl"));
    json_t *depth_data = json_object_get(result, "data");

    if (market == NULL || interval == NULL || depth_data == NULL) {
        char *str = json_dumps(result, 0); 
        log_error("depth reply error, result: %s", str);
        free(str);
        return -__LINE__;
    }

    if (ttl > 0)
        update_depth_cache(depth_data, market, interval, ttl);
    for (int i = 0; i < settings.depth_limit.count; ++i)
        notify_depth(market, interval, settings.depth_limit.limit[i], depth_data);

    return 0;
}

static void re_subscribe_depth(void)
{
    dict_iterator *iter = dict_get_iterator(dict_depth_sub_counter);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_key *key = entry->key;
        struct depth_sub_counter *val = entry->val;
        if (val->count > 0)
            send_depth_request(CMD_CACHE_DEPTH_SUBSCRIBE, key);
    }
    dict_release_iterator(iter);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        re_subscribe_depth();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        return;
    }

    json_t *result = json_object_get(reply, "result");
    if (result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_CACHE_DEPTH_UPDATE:
        ret = on_order_depth_update(result);
        if (ret < 0) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("on_order_depth_update fail: %d, reply: %s", ret, reply_str);
            sdsfree(reply_str);
        }
        break;
    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    json_decref(reply);
    return;
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

    dict_depth_sub = dict_create(&dt, 512);
    if (dict_depth_sub == NULL)
        return -__LINE__;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_sub_counter_dup;
    dt.val_destructor = dict_depth_sub_counter_free;

    dict_depth_sub_counter = dict_create(&dt, 512);
    if (dict_depth_sub_counter == NULL)
        return -__LINE__;

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cachecenter_clt_arr = malloc(sizeof(void *) * settings.cachecenter_worker_num);
    for (int i = 0; i < settings.cachecenter_worker_num; ++i) {
        char clt_name[100];
        snprintf(clt_name, sizeof(clt_name), "cachecenter_%d", i);
        char clt_addr[100];
        snprintf(clt_addr, sizeof(clt_addr), "tcp@%s:%d", settings.cachecenter_host, settings.cachecenter_port + i);

        rpc_clt_cfg cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.name = clt_name;
        cfg.addr_count = 1;
        cfg.addr_arr = malloc(sizeof(nw_addr_t));
        if (nw_sock_cfg_parse(clt_addr, &cfg.addr_arr[0], &cfg.sock_type) < 0)
            return -__LINE__;
        cfg.max_pkg_size = 1024 * 1024;

        cachecenter_clt_arr[i] = rpc_clt_create(&cfg, &ct);
        if (cachecenter_clt_arr[i] == NULL)
            return -__LINE__;
        if (rpc_clt_start(cachecenter_clt_arr[i]) < 0)
            return -__LINE__;
    }

    return 0;
}

int depth_subscribe(nw_ses *ses, const char *market, uint32_t limit, const char *interval, bool is_full)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);
    
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        struct depth_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = ptr_dict_hash_func;
        dt.key_compare = ptr_dict_key_compare;
        dt.val_dup = dict_depth_session_val_dup;
        dt.val_destructor = dict_depth_session_val_free;
        val.sessions = dict_create(&dt, 1024);
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_depth_sub, &key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct depth_val *obj = entry->val;
    if (dict_find(obj->sessions, ses) != NULL)
        return 0;

    struct depth_session_val ses_val;
    ses_val.is_full = is_full;
    dict_add(obj->sessions, ses, &ses_val);
    int count = depth_sub_counter_inc(market, interval);
    if (count == 1) {
        ERR_RET(send_depth_request(CMD_CACHE_DEPTH_SUBSCRIBE, &key));
    }
    return 0;  
}

int depth_unsubscribe(nw_ses *ses)
{
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *obj = entry->val;
        if (dict_delete(obj->sessions, ses) == 1) {
            struct depth_key *key = entry->key;
            int count = depth_sub_counter_dec(key->market, key->interval);
            if (count == 0) {
                send_depth_request(CMD_CACHE_DEPTH_UNSUBSCRIBE, key);
            }
        }
    }
    dict_release_iterator(iter);

    return 0;
}

int depth_send_clean(nw_ses *ses, const char *market, uint32_t limit, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL)
        return 0;

    struct depth_val *obj = entry->val;
    time_t now = time(NULL);
    if (now - obj->last_clean >= CLEAN_INTERVAL) {
        return 0;
    }
    
    if (obj->last) {
        json_t *params = json_array();
        json_array_append_new(params, json_boolean(true));
        json_array_append(params, obj->last);
        json_array_append_new(params, json_string(market));
        ws_send_notify(ses, "depth.update", params);
        json_decref(params);
        profile_inc("depth.update", 1);
    }

    return 0;
}

size_t depth_subscribe_number(void)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *obj = entry->val;
        count += dict_size(obj->sessions);
    }
    dict_release_iterator(iter);

    return count;
}

