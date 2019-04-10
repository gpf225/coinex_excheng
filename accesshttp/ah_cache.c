/*
 * Description: 
 *     History: ouxiangyang, 2019/04/3, create
 */

# include "ah_config.h"
# include "ah_cache.h"

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
    struct cache_val *obj = malloc(sizeof(struct cache_val));
    memcpy(obj, val, sizeof(struct cache_val));
    return obj;
}

static void cache_dict_val_free(void *val)
{
    struct cache_val *obj = val;
    if (obj->result != NULL)
        json_decref(obj->result);
    free(val);
}

void dict_replace_cache(sds cache_key, struct cache_val *val)
{
    dict_replace(backend_cache, cache_key, val);
}

int check_cache(nw_ses *ses, uint64_t id, sds key, uint32_t cmd, json_t *params)
{
    dict_entry *entry = dict_find(backend_cache, key);
    if (entry == NULL)
        return 0;

    struct cache_val *cache = entry->val;
    double now = current_timestamp();
    if (now >= cache->time_exp) {
        dict_delete(backend_cache, key);
        return 0;
    }

    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", cache->result);
    json_object_set_new(reply, "id", json_integer(id));   

    char *reply_str = json_dumps(reply, 0);
    send_http_response_simple(ses, 200, reply_str, strlen(reply_str));
    json_decref(reply);
    free(reply_str);
    profile_inc("hit_cache", 1);

    return 1;
}

static void on_cache_clear_timer(nw_timer *timer, void *privdata)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(backend_cache);

    while ((entry = dict_next(iter)) != NULL) {
        struct cache_val *val = entry->val;
        double now = current_timestamp();

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

