/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_cache.h"

static dict_t *dict_cache;
static nw_timer cache_timer;

static void *dict_cache_val_dup(const void *val)
{
    struct dict_cache_val *obj = malloc(sizeof(struct dict_cache_val));
    memcpy(obj, val, sizeof(struct dict_cache_val));
    return obj;
}

static void dict_cache_val_free(void *val)
{
    struct dict_cache_val *obj = val;
    if (obj->result != NULL)
    	json_decref(obj->result);
    free(val);
}

static void on_cache_clear_timer(nw_timer *timer, void *privdata)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_cache);

    while ((entry = dict_next(iter)) != NULL) {
        struct dict_cache_val *val = entry->val;
        uint64_t now = current_millisecond();

        if (now - val->time > settings.depth_cache_clear * 1000)
            dict_delete(dict_cache, entry->key);
    }
    dict_release_iterator(iter);
}

int add_cache(sds cache_key, json_t *result, uint64_t update_id)
{
    struct dict_cache_val cache;
    cache.time = current_millisecond();
    cache.result = result;
    cache.update_id = update_id;
    json_incref(result);
    dict_replace(dict_cache, cache_key, &cache);

    return 0;
}

int delete_cache(sds cache_key)
{
    return dict_delete(dict_cache, cache_key);
}

int update_cache(struct dict_cache_val *cache, json_t *last)
{
    json_incref(last);
    json_object_set_new(cache->result, "last", last);
    cache->time = current_millisecond();
    return 0;
}

struct dict_cache_val *get_cache(sds key)
{
    dict_entry *entry = dict_find(dict_cache, key);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

int init_cache(void)
{
	dict_types dt;
    memset(&dt, 0, sizeof(dt));

    dt.hash_function  = sds_dict_hash_function;
    dt.key_compare    = sds_dict_key_compare;
    dt.key_dup        = sds_dict_key_dup;
    dt.key_destructor = sds_dict_key_free;
    dt.val_dup        = dict_cache_val_dup;
    dt.val_destructor = dict_cache_val_free;

    dict_cache = dict_create(&dt, 512);
    if (dict_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_clear_timer, NULL);   
    nw_timer_start(&cache_timer);
    return 0;
}

