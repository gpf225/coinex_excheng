/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# include "aw_config.h"
# include "aw_kline.h"
# include "aw_server.h"

static dict_t *dict_kline;
static rpc_clt *cache;

struct kline_key {
    char market[MARKET_NAME_MAX_LEN];
    int interval;
};

struct kline_val {
    dict_t *sessions;
    json_t *last;
};

static uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

static int dict_ses_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

static uint32_t dict_kline_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct kline_key));
}

static int dict_kline_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct kline_key));
}

static void *dict_kline_key_dup(const void *key)
{
    struct kline_key *obj = malloc(sizeof(struct kline_key));
    memcpy(obj, key, sizeof(struct kline_key));
    return obj;
}

static void dict_kline_key_free(void *key)
{
    free(key);
}

static void *dict_kline_val_dup(const void *val)
{
    struct kline_val *obj = malloc(sizeof(struct kline_val));
    memcpy(obj, val, sizeof(struct kline_val));
    return obj;
}

static void dict_kline_val_free(void *val)
{
    struct kline_val *obj = val;
    dict_release(obj->sessions);
    if (obj->last)
        json_decref(obj->last);
    free(obj);
}

static void cache_send_request(const char *market, int interval, int command)
{ 
    if (!rpc_clt_connected(cache))
        return ;

    json_t *params = json_array();
    json_array_append(params, json_string(market));
    json_array_append(params, json_integer(interval));

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

static void re_subscribe_kline(void)
{
    dict_iterator *iter = dict_get_iterator(dict_kline);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct kline_key *key = entry->key;
        struct kline_val *obj = entry->val;

         if (dict_size(obj->sessions) > 0)
            cache_send_request(key->market, key->interval, CMD_CACHE_KLINE_SUBSCRIBE);
    }
    dict_release_iterator(iter);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        re_subscribe_kline();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static int broadcast_update(dict_t *sessions, json_t *result)
{
    dict_iterator *iter = dict_get_iterator(sessions);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        send_notify(entry->key, "kline.update", result);
    }
    dict_release_iterator(iter);
    profile_inc("kline.update", dict_size(sessions));

    return 0;
}

static int kline_compare(json_t *first, json_t *second)
{
    char *first_str = json_dumps(first, 0);
    char *second_str = json_dumps(second, 0);
    int cmp = strcmp(first_str, second_str);
    free(first_str);
    free(second_str);
    return cmp;
}

static int on_market_kline_reply(const char *market, int interval, json_t *result)
{
    struct kline_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    key.interval = interval;

    dict_entry *entry = dict_find(dict_kline, &key);
    if (entry == NULL)
        return -__LINE__;
    struct kline_val *obj = entry->val;

    if (!json_is_array(result))
        return -__LINE__;
    if (json_array_size(result) == 0)
        return 0;

    json_t *last = json_array_get(result, json_array_size(result) - 1);
    if (obj->last == NULL || kline_compare(obj->last, last) != 0) {
        if (obj->last)
            json_decref(obj->last);
        obj->last = last;
        json_incref(last);
        return broadcast_update(obj->sessions, result);
    }

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
    int interval = json_integer_value(json_array_get(result_array, 1));
    json_t *result = json_array_get(result_array, 2);
    if (market == NULL || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_CACHE_KLINE_UPDATE:
        ret = on_market_kline_reply(market, interval, result);
        if (ret < 0) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("on_order_deals_reply fail: %d, reply: %s", ret, reply_str);
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

int init_kline(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_kline_hash_func;
    dt.key_compare = dict_kline_key_compare;
    dt.key_dup = dict_kline_key_dup;
    dt.key_destructor = dict_kline_key_free;
    dt.val_dup = dict_kline_val_dup;
    dt.val_destructor = dict_kline_val_free;

    dict_kline = dict_create(&dt, 64);
    if (dict_kline == NULL)
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

    return 0;
}

int kline_subscribe(nw_ses *ses, const char *market, int interval)
{
    struct kline_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    key.interval = interval;

    dict_entry *entry = dict_find(dict_kline, &key);
    if (entry == NULL) {
        struct kline_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = dict_ses_hash_func;
        dt.key_compare = dict_ses_key_compare;
        val.sessions = dict_create(&dt, 1024);
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_kline, &key, &val);
        if (entry == NULL)
            return -__LINE__;

        cache_send_request(market, interval, CMD_CACHE_KLINE_SUBSCRIBE);
    }

    struct kline_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    return 0;
}

int kline_unsubscribe(nw_ses *ses)
{
    dict_iterator *iter = dict_get_iterator(dict_kline);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct kline_key *key = entry->key;
        struct kline_val *obj = entry->val;
        dict_delete(obj->sessions, ses);

         if (dict_size(obj->sessions) == 0)
            cache_send_request(key->market, key->interval, CMD_CACHE_KLINE_UNSUBSCRIBE);
    }
    dict_release_iterator(iter);

    return 0;
}

size_t kline_subscribe_number(void)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_kline);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct kline_val *obj = entry->val;
        count += dict_size(obj->sessions);
    }
    dict_release_iterator(iter);

    return count;
}

