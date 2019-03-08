/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/01, create
 */

# include "ar_depth_cache.h"
# include "ar_common.h"

# define DEPTH_LIMIT_MAX_SIZE 32

static dict_t *dict_cache;

static void *dict_depth_cache_val_dup(const void *val)
{
    struct depth_cache_val *obj = malloc(sizeof(struct depth_cache_val));
    memcpy(obj, val, sizeof(struct depth_cache_val));
    return obj;
}

static void depth_depth_cache_val_free(void *val)
{
    struct depth_cache_val *obj = val;
    if (obj->data != NULL) {
        json_decref(obj->data);
    }
    free(obj);
}

int depth_cache_set(const char *market, const char *interval, uint32_t ttl, json_t *result)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        struct depth_cache_val val;
        memset(&val, 0, sizeof(struct depth_cache_val));
        
        entry = dict_add(dict_cache, &key, &val);
        if (entry == NULL) {
            return -__LINE__;
        }
    }
    
    struct depth_cache_val *val = entry->val;
    if (val->data != NULL) {
        json_decref(val->data);
    }

    val->data = result;
    json_incref(val->data);
    val->expire_time = current_millis() + ttl;

    return 0;
}

depth_cache_val* depth_cache_get(const char *market, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        return NULL;
    }

    depth_cache_val *val = entry->val;
    uint64_t now = current_millis();
    if (now >= val->expire_time) {
        return NULL;
    } 
    return val;
}

int init_depth_cache(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_cache_val_dup;
    dt.val_destructor = depth_depth_cache_val_free;

    dict_cache = dict_create(&dt, 256);
    if (dict_cache == NULL) {
        return -__LINE__;
    }

    return 0;
}

void fini_depth_cache(void)
{
    dict_release(dict_cache);
}