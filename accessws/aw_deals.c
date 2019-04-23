/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/28, create
 */

# include "aw_config.h"
# include "aw_deals.h"
# include "aw_server.h"

static dict_t *dict_market;
static dict_t *dict_user;

# define DEALS_QUERY_LIMIT 1000

struct state_data {
    char market[MARKET_NAME_MAX_LEN];
};

struct market_val {
    dict_t *sessions;
};

struct user_key {
    uint32_t user_id;
};

struct user_val {
    dict_t *sessions;
};

static uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

static int dict_ses_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

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

static void *dict_market_val_dup(const void *val)
{
    struct market_val *obj = malloc(sizeof(struct market_val));
    memcpy(obj, val, sizeof(struct market_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    struct market_val *obj = val;
    if (obj->sessions)
        dict_release(obj->sessions);
    free(obj);
}

static uint32_t dict_user_hash_func(const void *key)
{
    return ((struct user_key *)key)->user_id;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    uint32_t user_1 = ((struct user_key *)key1)->user_id;
    uint32_t user_2 = ((struct user_key *)key2)->user_id;
    return user_1 == user_2 ? 0 : 1;
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

int deals_sub_update(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_market, market);
    if (entry == NULL)
        return -__LINE__;
    struct market_val *obj = entry->val;

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append(params, result);

    dict_iterator *iter = dict_get_iterator(obj->sessions);
    while ((entry = dict_next(iter)) != NULL) {
        send_notify(entry->key, "deals.update", params);
    }
    dict_release_iterator(iter);

    json_decref(params);
    profile_inc("deals.update", dict_size(obj->sessions));

    return 0;
}

int init_deals(void)
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
    dt.hash_function = dict_user_hash_func;
    dt.key_compare = dict_user_key_compare;
    dt.key_dup = dict_user_key_dup;
    dt.key_destructor = dict_user_key_free;
    dt.val_dup = dict_user_val_dup;
    dt.val_destructor = dict_user_val_free;

    dict_user = dict_create(&dt, 64);
    if (dict_user == NULL)
        return -__LINE__;

    return 0;
}

static int subscribe_user(nw_ses *ses, uint32_t user_id)
{
    struct user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(dict_user, &key);
    if (entry == NULL) {
        struct user_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = dict_ses_hash_func;
        dt.key_compare = dict_ses_key_compare;
        val.sessions = dict_create(&dt, 1024);
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_user, &key, &val);
        if (entry == NULL)
            return -__LINE__;
    }

    struct user_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    return 0;
}

int deals_subscribe(nw_ses *ses, const char *market, uint32_t user_id)
{
    dict_entry *entry = dict_find(dict_market, market);
    if (entry == NULL) {
        struct market_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = dict_ses_hash_func;
        dt.key_compare = dict_ses_key_compare;
        val.sessions = dict_create(&dt, 1024);
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_market, (char *)market, &val);
        if (entry == NULL)
            return -__LINE__;
    }

    struct market_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    if (user_id) {
        int ret = subscribe_user(ses, user_id);
        if (ret < 0)
            return ret;
    }

    return 0;
}

static int unsubscribe_user(nw_ses *ses, uint32_t user_id)
{
    struct user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(dict_user, &key);
    if (entry) {
        struct user_val *obj = entry->val;
        dict_delete(obj->sessions, ses);
    }

    return 0;
}

int deals_unsubscribe(nw_ses *ses, uint32_t user_id)
{
    dict_iterator *iter = dict_get_iterator(dict_market);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *obj = entry->val;
        dict_delete(obj->sessions, ses);
    }
    dict_release_iterator(iter);

    if (user_id) {
        unsubscribe_user(ses, user_id);
    }

    return 0;
}

int deals_new(uint32_t user_id, uint64_t id, double timestamp, int type, const char *market, const char *amount, const char *price)
{
    struct user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(dict_user, &key);
    if (entry == NULL)
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
    struct user_val *obj = entry->val;
    dict_iterator *iter = dict_get_iterator(obj->sessions);
    while ((entry = dict_next(iter)) != NULL) {
        send_notify(entry->key, "deals.update", params);
        count += 1;
    }
    dict_release_iterator(iter);
    json_decref(params);
    profile_inc("deals.update", count);

    return 0;
}

size_t deals_subscribe_number(void)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_market);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        const struct market_val *obj = entry->val;
        count += dict_size(obj->sessions);
    }
    dict_release_iterator(iter);

    return count;
}

