/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# include "aw_config.h"
# include "aw_server.h"
# include "aw_depth.h"

# define CLEAN_INTERVAL 60

static dict_t *dict_depth_sub; 

struct depth_key {
    char     market[MARKET_NAME_MAX_LEN];
    char     interval[INTERVAL_MAX_LEN];
    uint32_t limit;
};

struct depth_val {
    dict_t  *sessions;
    json_t  *last;
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
    if (obj->sessions != NULL)
        dict_release(obj->sessions);
    if (obj->last != NULL)
        json_decref(obj->last);
    free(obj);
}

static void depth_set_key(struct depth_key *key, const char *market, const char *interval, uint32_t limit)
{
    memset(key, 0, sizeof(struct depth_key));
    strncpy(key->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key->interval, interval, INTERVAL_MAX_LEN - 1);
    key->limit = limit;
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

json_t *get_depth_diff(json_t *first, json_t *second, uint32_t limit)
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

json_t *pack_depth_result(json_t *result, uint32_t limit)
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
        return broadcast_update(key.market, val->sessions, true, depth_result);
    }

    json_t *diff = get_depth_diff(val->last, depth_result, limit);
    if (diff == NULL) {
        json_decref(depth_result);
        return 0;
    }

    json_decref(val->last);
    val->last = depth_result;

    time_t now = time(NULL);
    if (now - val->last_clean >= CLEAN_INTERVAL) {
        val->last_clean = now; 
        broadcast_update(key.market, val->sessions, true, depth_result);
    } else {
        broadcast_update(key.market, val->sessions, false, diff);
    }
    json_decref(diff);

    return 0;
}

int depth_sub_update(const char *market, const char *interval, json_t *depth_data)
{
    for (int i = 0; i < settings.depth_limit.count; ++i)
        notify_depth(market, interval, settings.depth_limit.limit[i], depth_data);

    return 0;
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

    return 0;
}

int depth_subscribe(nw_ses *ses, const char *market, uint32_t limit, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);
    
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        struct depth_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = dict_ses_hash_func;
        dt.key_compare = dict_ses_key_compare;
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

    dict_add(obj->sessions, ses, NULL);
    return 0;  
}

int depth_unsubscribe(nw_ses *ses)
{
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *obj = entry->val;
        dict_delete(obj->sessions, ses);
    }
    dict_release_iterator(iter);

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
