/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/28, create
 */

# include "aw_config.h"
# include "aw_http.h"
# include "aw_state.h"
# include "aw_server.h"

static nw_timer notify_timer;
static nw_timer market_timer;

static dict_t *dict_market;
static dict_t *dict_session;
static rpc_clt *cache;

struct state_data {
    char market[MARKET_NAME_MAX_LEN];
};

struct market_val {
    int     id;
    json_t *last;
    double  update_time;
};

static uint32_t dict_market_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_market_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *dict_market_key_dup(const void *key)
{
    return strdup(key);
}

static void dict_market_key_free(void *key)
{
    free(key);
}

static void *dict_market_val_dup(const void *key)
{
    struct market_val *obj = malloc(sizeof(struct market_val));
    memcpy(obj, key, sizeof(struct market_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    struct market_val *obj = val;
    if (obj->last)
        json_decref(obj->last);
    free(obj);
}

static void *list_market_dup(void *val)
{
    return strdup(val);
}

static void list_market_free(void *val)
{
    free(val);
}

static uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

static int dict_ses_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

static void dict_ses_val_free(void *val)
{
    if (val) {
        list_release(val);
    }
}

static void cache_send_request(const char *market, int command)
{ 
    if (!rpc_clt_connected(cache))
        return ;

    json_t *params = json_array();
    json_array_append(params, json_string(market));

    static uint32_t sequence = 0;
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = command;
    pkg.sequence  = ++sequence;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(cache, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s", nw_sock_human_addr(rpc_clt_peer_addr(cache)), pkg.command, pkg.sequence, (char *)pkg.body);

    free(pkg.body);
    json_decref(params);
}

static void re_subscribe_status(void)
{
    dict_iterator *iter = dict_get_iterator(dict_market);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        cache_send_request((char *)entry->key, CMD_CACHE_STATUS_UNSUBSCRIBE);
        cache_send_request((char *)entry->key, CMD_CACHE_STATUS_SUBSCRIBE);
    }
    dict_release_iterator(iter);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        re_subscribe_status();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static int on_market_status_reply(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_market, market);
    if (entry == NULL)
        return -__LINE__;
    struct market_val *info = entry->val;

    char *last_str = NULL;
    if (info->last)
        last_str = json_dumps(info->last, JSON_SORT_KEYS);
    char *curr_str = json_dumps(result, JSON_SORT_KEYS);

    if (info->last == NULL || strcmp(last_str, curr_str) != 0) {
        if (info->last)
            json_decref(info->last);
        info->last = result;
        json_incref(result);
        info->update_time = current_timestamp();
    }

    if (last_str != NULL)
        free(last_str);
    free(curr_str);

    return 0;
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

    json_t *error = json_object_get(reply, "error");
    json_t *result_array = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result_array == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        return;
    }

    const char *market = json_string_value(json_array_get(result_array, 0));
    json_t *result = json_array_get(result_array, 1);
    if (market == NULL || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_CACHE_STATUS_UPDATE:
        ret = on_market_status_reply(market, result);
        if (ret < 0) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("on_market_status_reply fail: %d, reply: %s", ret, reply_str);
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

static json_t *get_notify_full(double last_notify)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->last && info->update_time > last_notify) {
            json_object_set(result, entry->key, info->last);
        }
    }
    dict_release_iterator(iter);
    return result;
}

static json_t *get_notify_list(list_t *list, double last_notify)
{
    json_t *result = json_object();
    list_node *node;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        dict_entry *entry = dict_find(dict_market, node->value);
        if (!entry) {
            list_del(list, node);
            continue;
        }
        struct market_val *info = entry->val;
        if (info->last && info->update_time > last_notify) {
            json_object_set(result, entry->key, info->last);
        }
    }
    list_release_iterator(iter);
    return result;
}

static void on_notify_timer(nw_timer *timer, void *privdata)
{
    static double last_notify;
    size_t count = 0;
    json_t *result;
    json_t *full_result = NULL;

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_session);
    while ((entry = dict_next(iter)) != NULL) {
        if (entry->val == NULL) {
            if (full_result == NULL)
                full_result = get_notify_full(last_notify);
            result = full_result;
        } else {
            result = get_notify_list(entry->val, last_notify);
        }
        if (json_object_size(result) != 0) {
            json_t *params = json_array();
            json_array_append(params, result);
            send_notify(entry->key, "state.update", params);
            json_decref(params);
            count += 1;
        }
        if (result != full_result)
            json_decref(result);
    }
    dict_release_iterator(iter);
    if (full_result)
        json_decref(full_result);
    last_notify = current_timestamp();

    if (count) {
        profile_inc("state.update", count);
    }
}

static void on_market_list_callback(json_t *result)
{
    static uint32_t update_id = 0;
    update_id += 1;

    for (size_t i = 0; i < json_array_size(result); ++i) {
        json_t *item = json_array_get(result, i);
        const char *name = json_string_value(json_object_get(item, "name"));
        dict_entry *entry = dict_find(dict_market, name);
        if (entry == NULL) {
            struct market_val val;
            memset(&val, 0, sizeof(val));
            val.id = update_id;
            dict_add(dict_market, (char *)name, &val);
            cache_send_request((char *)name, CMD_CACHE_STATUS_SUBSCRIBE);
            log_info("add market: %s", name);
        } else {
            struct market_val *info = entry->val;
            info->id = update_id;
        }
    }

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->id != update_id) {
            dict_delete(dict_market, entry->key);
            cache_send_request((char *)entry->key, CMD_CACHE_STATUS_UNSUBSCRIBE);
            log_info("del market: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);
}

static void on_market_timer(nw_timer *timer, void *privdata)
{
    json_t *params = json_array();
    send_http_request("market.list", params, on_market_list_callback);
}

int init_state(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_market_hash_func;
    dt.key_compare = dict_market_key_compare;
    dt.key_dup = dict_market_key_dup;
    dt.key_destructor = dict_market_key_free;
    dt.val_dup = dict_market_val_dup;
    dt.val_destructor = dict_market_val_free;

    dict_market = dict_create(&dt, 64);
    if (dict_market == NULL)
        return -__LINE__;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_key_compare;
    dt.val_destructor = dict_ses_val_free;

    dict_session = dict_create(&dt, 64);
    if (dict_session == NULL)
        return -__LINE__;

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cache = rpc_clt_create(&settings.cache, &ct);
    if (cache == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache) < 0)
        return -__LINE__;

    nw_timer_set(&market_timer, settings.market_interval, true, on_market_timer, NULL);
    nw_timer_start(&market_timer);

    nw_timer_set(&notify_timer, settings.state_interval, true, on_notify_timer, NULL);
    nw_timer_start(&notify_timer);
    
    on_market_timer(NULL, NULL);

    return 0;
}

int state_subscribe(nw_ses *ses, json_t *market_list)
{
    dict_delete(dict_session, ses);
    if (json_array_size(market_list) == 0) {
        dict_add(dict_session, ses, NULL);
        return 0;
    }

    list_type type;
    memset(&type, 0, sizeof(type));
    type.dup = list_market_dup;
    type.free = list_market_free;

    list_t *list = list_create(&type);
    if (list == NULL)
        return -__LINE__;
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        const char *name = json_string_value(json_array_get(market_list, i));
        if (name == NULL)
            continue;
        if (dict_find(dict_market, name)) {
            list_add_node_tail(list, (char *)name);
        }
    }
    dict_add(dict_session, ses, list);

    return 0;
}

int state_unsubscribe(nw_ses *ses)
{
    return dict_delete(dict_session, ses);
}

int state_send_last(nw_ses *ses)
{
    dict_entry *entry = dict_find(dict_session, ses);
    if (entry == NULL) {
        return 0;
    }

    json_t *result = json_object();
    if (entry->val == NULL) {
        dict_entry *entry;
        dict_iterator *iter = dict_get_iterator(dict_market);
        while ((entry = dict_next(iter)) != NULL) {
            struct market_val *info = entry->val;
            if (info->last) {
                json_object_set(result, entry->key, info->last);
            }
        }
        dict_release_iterator(iter);
    } else {
        list_node *node;
        list_iter *iter = list_get_iterator(entry->val, LIST_START_HEAD);
        while ((node = list_next(iter)) != NULL) {
            dict_entry *entry = dict_find(dict_market, node->value);
            if (!entry) {
                list_del(entry->val, node);
                continue;
            }
            struct market_val *info = entry->val;
            if (info->last) {
                json_object_set(result, entry->key, info->last);
            }
        }
        list_release_iterator(iter);
    }

    json_t *params = json_array();
    json_array_append_new(params, result);
    send_notify(ses, "state.update", params);
    json_decref(params);

    return 0;
}

size_t state_subscribe_number(void)
{
    return dict_size(dict_session);
}

bool market_exists(const char *market)
{
    return (dict_find(dict_market, market) != NULL);
}
