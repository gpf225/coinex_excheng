/*
 * Description: 
 *     History: ouxiangyang, 2019/04/3, create
 */

# include "ar_config.h"
# include "ar_cache.h"

static nw_timer cache_timer;
static dict_t *backend_cache;

static uint32_t cache_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sdslen((sds)key));
}

static int cache_dict_key_compare(const void *key1, const void *key2)
{
    return sdscmp((sds)key1, (sds)key2);
}

static void *cache_dict_key_dup(const void *key)
{
    return sdsdup((const sds)key);
}

static void cache_dict_key_free(void *key)
{
    sdsfree(key);
}

static void *cache_dict_val_dup(const void *val)
{
    struct cache_exp_val *obj = malloc(sizeof(struct cache_exp_val));
    memcpy(obj, val, sizeof(struct cache_exp_val));
    return obj;
}

static void cache_dict_val_free(void *val)
{
    struct cache_exp_val *obj = val;
    if (obj->result != NULL)
        json_decref(obj->result);
    free(val);
}

static json_t* generate_depth_data(json_t *array, int limit) 
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

void dict_replace_exp_cache(sds cache_key, struct cache_exp_val *val)
{
    dict_replace(backend_cache, cache_key, val);
}

int check_exp_cache(nw_ses *ses, sds key, uint32_t cmd, int limit)
{
    dict_entry *entry = dict_find(backend_cache, key);
    if (entry == NULL)
        return 0;

    struct cache_exp_val *cache = entry->val;
    double now = current_millis();

    if (now >= cache->time_exp) {
        dict_delete(backend_cache, key);
        return 0;
    }

    json_t *result = json_object();
    json_object_set_new(result, "code", json_integer(0));
    
    if (cmd == CMD_ORDER_DEPTH) {
        json_t *result_depth = pack_depth_result(cache->result, limit);
        json_object_set_new(result, "data", result_depth);
    } else {
        json_object_set(result, "data", cache->result);
    }
    json_object_set_new(result, "message", json_string("OK"));

    char *result_str = json_dumps(result, 0);
    send_http_response_simple(ses, 200, result_str, strlen(result_str));
    json_decref(result);
    free(result_str);
    profile_inc("hit_cache", 1);

    return 1;
}

static void on_cache_clear_timer(nw_timer *timer, void *privdata)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(backend_cache);

    while ((entry = dict_next(iter)) != NULL) {
        struct cache_exp_val *val = entry->val;
        double now = current_millis();

        if (now > val->time_exp)
            dict_delete(backend_cache, entry->key);
    }
    dict_release_iterator(iter);
} 

int init_cache(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = cache_dict_hash_function;
    dt.key_compare    = cache_dict_key_compare;
    dt.key_dup        = cache_dict_key_dup;
    dt.key_destructor = cache_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;
    backend_cache = dict_create(&dt, 64);
    if (backend_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_clear_timer, NULL);
    nw_timer_start(&cache_timer);

    return 0;
}


