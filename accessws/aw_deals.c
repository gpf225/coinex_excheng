/*
 * Description: 
 *     History: ouxiangyang, 2019/04/19, create
 */
# include "aw_config.h"
# include "aw_deals.h"
# include "aw_server.h"
# include "aw_deals.h"
# include "aw_depth.h"
# include "aw_deals.h"

static dict_t *dict_sub_deals;
static dict_t *dict_user;
static dict_t *dict_deals;

static rpc_clt *cache_deals;

struct user_key {
    char market[MARKET_NAME_MAX_LEN + 1];
    uint32_t user_id;
};

struct user_val {
    dict_t *sessions;
};

struct deals_val {
    list_t   *deals;
    uint64_t last_id;
};

struct sub_deals_val {
    dict_t *sessions;
};

static void *dict_market_val_dup(const void *val)
{
    struct sub_deals_val *obj = malloc(sizeof(struct sub_deals_val));
    memcpy(obj, val, sizeof(struct sub_deals_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    struct sub_deals_val *obj = val;
    if (obj->sessions)
        dict_release(obj->sessions);
    free(obj);
}

// dict user
static void *dict_user_val_dup(const void *key)
{
    struct user_val *obj = malloc(sizeof(struct user_val));
    memcpy(obj, key, sizeof(struct user_val));
    return obj;
}

static void dict_user_val_free(void *val)
{
    struct user_val *obj = val;
    dict_release(obj->sessions);
    free(obj);
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

static uint32_t dict_user_key_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct user_key));
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct user_key));
}

static void *dict_user_key_dup(const void *key)
{
    struct user_key *obj = malloc(sizeof(struct user_key));
    memcpy(obj, key, sizeof(struct user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void list_free(void *value)
{
    json_decref(value);
}

int deals_subscribe_user(nw_ses *ses, uint32_t user_id, const char *market)
{
    struct user_key key;
    memset(&key, 0, sizeof(struct user_key));
    sstrncpy(key.market, market, sizeof(key.market));
    key.user_id = user_id;
    dict_entry *entry = dict_find(dict_user, &key);
    if (entry == NULL) {
        struct user_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = ptr_dict_hash_func;
        dt.key_compare = ptr_dict_key_compare;
        val.sessions = dict_create(&dt, 2);
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_user, &key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct user_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    return 0;
}

int deals_subscribe(nw_ses *ses, const char *market, uint32_t user_id)
{
    dict_entry *entry = dict_find(dict_sub_deals, market);
    if (entry == NULL) {
        struct sub_deals_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = ptr_dict_hash_func;
        dt.key_compare = ptr_dict_key_compare;
        val.sessions = dict_create(&dt, 1024);
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_sub_deals, (char *)market, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct sub_deals_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    if (user_id) {
        int ret = deals_subscribe_user(ses, user_id, "");
        if (ret < 0)
            return ret;
    }

    return 0;
}

int deals_unsubscribe_user(nw_ses *ses, uint32_t user_id, const char *market)
{
    if (user_id > 0) {
        struct user_key key;
        memset(&key, 0, sizeof(struct user_key));
        sstrncpy(key.market, market, sizeof(key.market));
        key.user_id = user_id;
        dict_entry *entry = dict_find(dict_user, &key);
        if (entry) {
            struct user_val *obj = entry->val;
            dict_delete(obj->sessions, ses);

            if (dict_size(obj->sessions) == 0) {
                dict_delete(dict_user, &key);
            }
        }
    } else {
        dict_iterator *iter = dict_get_iterator(dict_user);
        dict_entry *entry;
        while ((entry = dict_next(iter)) != NULL) {
            struct user_val *obj = entry->val;
            dict_delete(obj->sessions, ses);
            if (dict_size(obj->sessions) == 0) {
                dict_delete(dict_user, entry->key);
            }
        }
        dict_release_iterator(iter);
    }
    return 0;
}

int deals_unsubscribe(nw_ses *ses, uint32_t user_id)
{
    dict_iterator *iter = dict_get_iterator(dict_sub_deals);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct sub_deals_val *obj = entry->val;
        dict_delete(obj->sessions, ses);
    }
    dict_release_iterator(iter);

    if (user_id) {
        deals_unsubscribe_user(ses, user_id, "");
    }

    return 0;
}

int deals_new(uint32_t user_id, uint64_t id, double timestamp, int type, const char *market, const char *amount, const char *price)
{
    struct user_key key;
    memset(&key, 0, sizeof(struct user_key));
    sstrncpy(key.market, market, sizeof(key.market));
    key.user_id = user_id;
    dict_entry *entry_market = dict_find(dict_user, &key);
    
    memset(&key, 0, sizeof (struct user_key));
    key.user_id = user_id;
    dict_entry *entry_all = dict_find(dict_user, &key);
    if (entry_market == NULL && entry_all == NULL)
        return 0;

    json_t *message = json_object();
    json_object_set_new(message, "id", json_integer(id));
    json_object_set_new(message, "time", json_real(timestamp));
    if (type == MARKET_TRADE_SIDE_SELL) {
        json_object_set_new(message, "type", json_string("sell"));
    } else {
        json_object_set_new(message, "type", json_string("buy"));
    }
    json_object_set_new(message, "amount", json_string(amount));
    json_object_set_new(message, "price", json_string(price));

    json_t *deals = json_array();
    json_array_append_new(deals, message);

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, deals);
    json_array_append_new(params, json_true());

    size_t count = 0;
    if (entry_market) {
        dict_entry *entry;
        struct user_val *obj = entry_market->val;
        dict_iterator *iter = dict_get_iterator(obj->sessions);
        while ((entry = dict_next(iter)) != NULL) {
            ws_send_notify(entry->key, "deals.update", params);
            count += 1;
        }
        dict_release_iterator(iter);
    }

    if (entry_all) {
        dict_entry *entry;
        struct user_val *obj = entry_all->val;
        dict_iterator *iter = dict_get_iterator(obj->sessions);
        while ((entry = dict_next(iter)) != NULL) {
            ws_send_notify(entry->key, "deals.update", params);
            count += 1;
        }
        dict_release_iterator(iter);
    }
    json_decref(params);
    profile_inc("deals.update", count);

    return 0;
}

int deals_sub_send_full(nw_ses *ses, const char *market)
{
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry == NULL)
        return -__LINE__;
    struct deals_val *obj = entry->val;
    if (obj->deals->len == 0)
        return 0;

    json_t *deals = json_array();
    list_iter *iter = list_get_iterator(obj->deals, LIST_START_HEAD);
    list_node *node;

    int count = 0;
    while ((node = list_next(iter)) != NULL) {
        json_array_append(deals, node->value);
        count++;

        if (count > 100) {
            break;
        }
    }
    list_release_iterator(iter);

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, deals);

    ws_send_notify(ses, "deals.update", params);
    json_decref(params);

    return 0;
}

static int deals_sub_update(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_sub_deals, market);
    if (entry == NULL)
        return -__LINE__;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append(params, result);

    struct sub_deals_val *obj = entry->val;
    dict_iterator *iter = dict_get_iterator(obj->sessions);
    while ((entry = dict_next(iter)) != NULL) {
        ws_send_notify(entry->key, "deals.update", params);
    }
    dict_release_iterator(iter);

    json_decref(params);
    profile_inc("deals.update", dict_size(obj->sessions));

    return 0;
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
    uint64_t first_id = json_integer_value(json_object_get(first, "id"));
    if (first_id == 0)
        return -__LINE__;

    struct deals_val *obj = entry->val;
    log_info("deal sub reply, market: %s, array_size: %zd, last_id: %zd, first_id: %zd", market, array_size, obj->last_id, first_id);

    for (size_t i = array_size; i > 0; --i) {
        json_t *deal = json_array_get(result, i - 1);
        uint64_t id = json_integer_value(json_object_get(deal, "id"));
        if (id > obj->last_id) {
            json_incref(deal);
            list_add_node_head(obj->deals, deal);  
        }
    }

    obj->last_id = first_id;

    while (obj->deals->len > settings.deal_max) {
        list_del(obj->deals, list_tail(obj->deals));
    }

    deals_sub_update(market, result);

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
    case CMD_CACHE_DEALS_UPDATE:
        on_sub_deals_update(result, ses, pkg);
        break;
    default:
        break;
    }

clean:
    if (reply)
        json_decref(reply);

    return;
}

static json_t *pack_deals_result(list_t *deals, uint32_t limit, int64_t last_id)
{
    int count = 0;
    json_t *result = json_array();

    list_node *node;
    list_iter *iter = list_get_iterator(deals, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        json_t *deal = node->value;
        if (last_id && json_integer_value(json_object_get(deal, "id")) <= last_id) {
            break;
        }
        json_array_append(result, deal);
        count += 1;
        if (count == limit) {
            break;
        }
    }
    list_release_iterator(iter);

    return result;
}

void direct_deals_reply(nw_ses *ses, json_t *params, int64_t id)
{
    if (json_array_size(params) != 3) {
        ws_send_error_invalid_argument(ses, id);
        return;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market) {
        ws_send_error_invalid_argument(ses, id);
        return;
    }

    int limit = json_integer_value(json_array_get(params, 1));
    if (limit <= 0 || limit > settings.deal_max) {
        log_error("exceed deals max limit, limit: %d, max_limit: %d", limit, settings.deal_max);
        ws_send_error_invalid_argument(ses, id);
        return;
    }

    if (!json_is_integer(json_array_get(params, 2))) {
        ws_send_error_invalid_argument(ses, id);
        return;
    }
    uint64_t last_id = json_integer_value(json_array_get(params, 2));

    bool is_reply = false;
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry != NULL) {
        struct deals_val *val = entry->val;
        if (val->deals != NULL) {
            is_reply = true;
            json_t *result = pack_deals_result(val->deals, limit, last_id);
            ws_send_result(ses, id, result);
            json_decref(result);
        }
    }

    if (!is_reply) {
        ws_send_error_direct_result_null(ses, id);
        log_error("deals not find result, market: %s", market);
    }

    return;
}

int init_deals(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_dup        = dict_market_val_dup;
    dt.val_destructor = dict_market_val_free;
    dict_sub_deals = dict_create(&dt, 64);
    if (dict_sub_deals == NULL)
        return -__LINE__;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_user_key_hash_function;
    dt.key_compare    = dict_user_key_compare;
    dt.key_dup        = dict_user_key_dup;
    dt.key_destructor = dict_user_key_free;
    dt.val_dup        = dict_user_val_dup;
    dt.val_destructor = dict_user_val_free;
    dict_user = dict_create(&dt, 64);
    if (dict_user == NULL)
        return -__LINE__;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_dup        = dict_deals_val_dup;
    dt.val_destructor = dict_deals_val_free;
    dict_deals = dict_create(&dt, 64);
    if (dict_deals == NULL)
        return -__LINE__;

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cache_deals = rpc_clt_create(&settings.cache_deals, &ct);
    if (cache_deals == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache_deals) < 0)
        return -__LINE__;

    return 0;
}

size_t deals_subscribe_number(void)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_sub_deals);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        const struct sub_deals_val *obj = entry->val;
        count += dict_size(obj->sessions);
    }
    dict_release_iterator(iter);

    return count;
}
