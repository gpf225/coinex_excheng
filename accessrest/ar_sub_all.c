/*
 * Description: 
 *     History: ouxiangyang, 2019/04/19, create
 */

# include "ar_config.h"
# include "ar_sub_all.h"
# include "ar_server.h"
# include "ar_ticker.h"

static dict_t *dict_deals;
static dict_t *dict_depth;
static dict_t *dict_state;
static rpc_clt *cache;

# define MARKET_NAME_MAX_LEN    16
# define INTERVAL_MAX_LEN       16
# define MAX_DEALS_LIMIT        1000

struct deals_val {
    list_t *deals;
    uint64_t last_id;
};

struct depth_key {
    char     market[MARKET_NAME_MAX_LEN];
    char     interval[INTERVAL_MAX_LEN];
};

struct depth_val {
    json_t   *last;
};

struct state_val {
    json_t *last;
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

// dict deals
static void *dict_deals_val_dup(const void *val)
{
    struct deals_val *obj = malloc(sizeof(struct deals_val));
    memcpy(obj, val, sizeof(struct deals_val));
    return obj;
}

static void dict_deals_val_free(void *val)
{
    struct deals_val *obj = val;
    if (obj->deals)
        list_release(obj->deals);
    free(obj);
}

static void list_free(void *value)
{
    json_decref(value);
}

// dict depth
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
    if (obj->last != NULL)
        json_decref(obj->last);
    free(obj);
}

//dict state
static void *dict_state_val_dup(const void *key)
{
    struct state_val *obj = malloc(sizeof(struct state_val));
    memcpy(obj, key, sizeof(struct state_val));
    return obj;
}

static void dict_state_val_free(void *val)
{
    struct state_val *obj = val;
    if (obj->last)
        json_decref(obj->last);
    free(obj);
}

static void sub_cache_all()
{ 
    if (!rpc_clt_connected(cache))
        return;

    json_t *params = json_array();
    static uint32_t sequence = 0;
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_CACHE_SUBSCRIBE_ALL;
    pkg.sequence  = ++sequence;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(cache, &pkg);
    log_info("send request to %s, cmd: %u, sequence: %u, params: %s", nw_sock_human_addr(rpc_clt_peer_addr(cache)), pkg.command, pkg.sequence, (char *)pkg.body);

    free(pkg.body);
    json_decref(params);
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
    json_object_set    (new_result, "time", json_integer(current_millis()));

    return new_result;
}

// deals reply
static json_t *pack_deals_result(list_t *deals, uint32_t limit, int64_t last_id)
{
    int count = 0;
    json_t *result = json_array();
    list_iter *iter = list_get_iterator(deals, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        json_t *deal = node->value;
        uint64_t id = json_integer_value(json_object_get(deal, "id"));
        if (id <= last_id) {
            break;
        }
        json_t *item = json_object();
        json_object_set(item, "id", json_object_get(deal, "id"));
        json_object_set(item, "time", json_object_get(deal, "time"));
        json_object_set(item, "type", json_object_get(deal, "type"));
        json_object_set(item, "price", json_object_get(deal, "price"));
        json_object_set(item, "amount", json_object_get(deal, "amount"));
        json_array_append_new(result, item);
        count += 1;
        if (count == limit) {
            break;
        }
    }
    list_release_iterator(iter);

    return result;
}

// deals update
static int on_sub_deals_update(json_t *result_array, nw_ses *ses, rpc_pkg *pkg)
{
    const char *market = json_string_value(json_array_get(result_array, 0));
    json_t *result = json_array_get(result_array, 1);
    if (market == NULL || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        return -__LINE__;
    }

    log_trace("deals update, market: %s", market);
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry == NULL) {
        struct deals_val val;
        memset(&val, 0, sizeof(val));

        list_type lt;
        memset(&lt, 0, sizeof(lt));
        lt.free = list_free;

        val.deals = list_create(&lt);
        if (val.deals == NULL)
            return -__LINE__;

        entry = dict_add(dict_deals, (char *)market, &val);
        if (entry == NULL) {
            list_release(val.deals);
            return -__LINE__;
        }
    }

    if (!json_is_array(result))
        return -__LINE__;
    size_t array_size = json_array_size(result);
    if (array_size == 0)
        return 0;

    json_t *first = json_array_get(result, 0);
    uint64_t id = json_integer_value(json_object_get(first, "id"));
    if (id == 0)
        return -__LINE__;

    struct deals_val *obj = entry->val;
    for (size_t i = array_size; i > 0; --i) {
        json_t *deal = json_array_get(result, i - 1);

        id = json_integer_value(json_object_get(deal, "id"));
        if (id > obj->last_id) {
            obj->last_id = id;
            json_incref(deal);
            list_add_node_head(obj->deals, deal);  
        }
    }

    while (obj->deals->len > MAX_DEALS_LIMIT) {
        list_del(obj->deals, list_tail(obj->deals));
    }

    return 0;
}

static bool is_json_equal(json_t *lhs, json_t *rhs)
{
    if (lhs == NULL || rhs == NULL)
        return false;

    char *lhs_str = json_dumps(lhs, JSON_SORT_KEYS);
    char *rhs_str = json_dumps(rhs, JSON_SORT_KEYS);
    int ret = strcmp(lhs_str, rhs_str);
    free(lhs_str);
    free(rhs_str);

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

// depth update
static int on_sub_depth_update(json_t *result, nw_ses *ses, rpc_pkg *pkg)
{
    const char *market = json_string_value(json_object_get(result, "market"));
    const char *interval = json_string_value(json_object_get(result, "interval"));
    json_t *depth_data = json_object_get(result, "data");

    log_trace("depth update, market: %s, interval: %s", market, interval);
    if (market == NULL || interval == NULL || depth_data == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        return -__LINE__;
    }

    struct depth_key key;
    memset(&key, 0, sizeof(struct depth_key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key.interval, interval, INTERVAL_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_depth, &key);
    if (entry == NULL) {
        struct depth_val val;
        memset(&val, 0, sizeof(val));

        entry = dict_add(dict_depth, &key, &val);
        if (entry == NULL)
            return -__LINE__;
    }

    struct depth_val *val = entry->val;
    if (val->last == NULL) {
        json_incref(depth_data);
        val->last = depth_data;
        depth_ticker_update(market, depth_data);

        return 0;
    }

    json_decref(val->last);
    json_incref(depth_data);
    val->last = depth_data;

    if (!is_depth_equal(val->last, depth_data)) {
        depth_ticker_update(market, depth_data);
    }

    return 0;
}

// state update
static int on_sub_state_update(json_t *result_array, nw_ses *ses, rpc_pkg *pkg)
{
    const char *market = json_string_value(json_array_get(result_array, 0));
    json_t *result = json_array_get(result_array, 1);
    if (market == NULL || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        return -__LINE__;
    }

    log_trace("state update, market: %s", market);
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct state_val val;
        memset(&val, 0, sizeof(val));

        val.last = result;
        json_incref(result);

        entry = dict_add(dict_state, (char *)market, &val);
        if (entry == NULL)
            return -__LINE__;

        status_ticker_update(market, result);
        return 0;
    }

    struct state_val *info = entry->val;
    char *last_str = NULL;
    if (info->last)
        last_str = json_dumps(info->last, JSON_SORT_KEYS);
    char *curr_str = json_dumps(result, JSON_SORT_KEYS);

    if (info->last == NULL || strcmp(last_str, curr_str) != 0) {
        if (info->last)
            json_decref(info->last);
        info->last = result;
        json_incref(result);
    }

    if (last_str != NULL)
        free(last_str);
    free(curr_str);

    status_ticker_update(market, result);
    return 0;
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        sub_cache_all();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence);
    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (!reply) {
        log_error("json_loadb fail");
        goto clean;
    }
    json_t *error = json_object_get(reply, "error");
    if (!error) {
        log_error("error param not find");
        goto clean;
    }
    if (!json_is_null(error)) {
        log_error("error is not null");
        goto clean;
    }

    json_t *result = json_object_get(reply, "result");
    if (!result) {
        log_error("result param not find");
        goto clean;
    }

    switch (pkg->command) {
    case CMD_CACHE_DEPTH_UPDATE:
        on_sub_depth_update(result, ses, pkg);
        break;
    case CMD_CACHE_DEALS_UPDATE:
        on_sub_deals_update(result, ses, pkg);
        break;
    case CMD_CACHE_STATUS_UPDATE:
        on_sub_state_update(result, ses, pkg);
        break;
    default:
        break;
    }

clean:
    if (reply)
        json_decref(reply);

    return;
}

// direct reply
void direct_depth_reply(nw_ses *ses, const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    memset(&key, 0, sizeof(struct depth_key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key.interval, interval, INTERVAL_MAX_LEN - 1);

    bool is_reply = false;
    dict_entry *entry = dict_find(dict_depth, &key);
    if (entry != NULL) {
        struct depth_val *val = entry->val;
        if (val->last != NULL) {
            is_reply = true;
            json_t *result = pack_depth_result(val->last, limit);
            reply_json(ses, result, NULL);
            json_decref(result);
        }
    }

    if (!is_reply) {
        reply_result_null(ses);
        log_error("depth not find result, market: %s, interval: %s", market, interval);
    }

    return;
}

void direct_deals_result(nw_ses *ses, const char *market, int limit, uint64_t last_id)
{
    bool is_reply = false;
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry != NULL) {
        struct deals_val *val = entry->val;
        if (val->deals != NULL) {
            is_reply = true;
            json_t *result = pack_deals_result(val->deals, limit, last_id);
            reply_json(ses, result, NULL);
            json_decref(result);
        }
    }

    if (!is_reply) {
        reply_result_null(ses);
        log_error("deals not find result, market: %s", market);
    }

    return;
}

void direct_state_reply(nw_ses *ses, json_t *params, int64_t id)
{
    if (json_array_size(params) != 2) {
        reply_invalid_params(ses);
        return;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market) {
        reply_invalid_params(ses);
        return;
    }

    bool is_reply = false;
    dict_entry *entry = dict_find(dict_state, market);
    if (entry != NULL) {
        struct state_val *val = entry->val;
        if (val->last != NULL) {
            is_reply = true;
            reply_json(ses, val->last, NULL);
        }
    }

    if (!is_reply) {
        reply_result_null(ses);
        log_error("state not find result, market: %s", market);
    }

    return;
}

bool judege_period_is_day(int interval)
{
    if (interval == 86400) {
        return true;
    } else {
        return false;
    }
}

int init_sub_all(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cache = rpc_clt_create(&settings.cache, &ct);
    if (cache == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_market_hash_func;
    dt.key_compare    = dict_market_key_compare;
    dt.key_dup        = dict_market_key_dup;
    dt.key_destructor = dict_market_key_free;
    dt.val_dup        = dict_deals_val_dup;
    dt.val_destructor = dict_deals_val_free;
    dict_deals = dict_create(&dt, 64);
    if (dict_deals == NULL)
        return -__LINE__;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_depth_hash_func;
    dt.key_compare    = dict_depth_key_compare;
    dt.key_dup        = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup        = dict_depth_val_dup;
    dt.val_destructor = dict_depth_val_free;
    dict_depth        = dict_create(&dt, 64);
    if (dict_depth == NULL)
        return -__LINE__;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_market_hash_func;
    dt.key_compare    = dict_market_key_compare;
    dt.key_dup        = dict_market_key_dup;
    dt.key_destructor = dict_market_key_free;
    dt.val_dup        = dict_state_val_dup;
    dt.val_destructor = dict_state_val_free;
    dict_state = dict_create(&dt, 64);
    if (dict_state == NULL)
        return -__LINE__;

    return 0;
}


