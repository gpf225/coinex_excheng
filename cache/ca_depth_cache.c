/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# include "ca_depth_cache.h"
# include "ca_common.h"

# define DEPTH_LIMIT_MAX_SIZE 32
# define DEPTH_LIMIT_EXPIRE_TIME 4000

static int cache_timeout = 1000;
static dict_t *dict_cache;

static void *dict_depth_cache_val_dup(const void *val)
{
    struct depth_cache_val *obj = malloc(sizeof(struct depth_cache_val));
    memcpy(obj, val, sizeof(struct depth_cache_val));
    return obj;
}

static void dict_depth_cache_val_free(void *val)
{
    struct depth_cache_val *obj = val;
    if (obj->data != NULL) {
        json_decref(obj->data);
    }
    free(obj);
}

struct depth_cache_val* depth_cache_set(const char *market, const char *interval, uint32_t limit, json_t *data)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        struct depth_cache_val val;
        memset(&val, 0, sizeof(depth_cache_val));
        val.limit_last_hit_time = current_millis();
    
        entry = dict_add(dict_cache, &key, &val);
        if (entry == NULL) {
            return NULL;
        }
    }

    uint64_t now = current_millis();
    struct depth_cache_val *val = entry->val;
    if (val->limit > limit) {
        if (val->time + cache_timeout > now) {
            return val;
        }
    }
    
    if (val->data != NULL) {
        json_decref(val->data);
    }
    
    val->data = data;
    json_incref(val->data);

    val->limit = limit;
    val->time = now;
    val->ttl = cache_timeout;
    
    return val;
}

depth_cache_val* depth_cache_get(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        return NULL;
    }

    depth_cache_val *val = entry->val;
    if (limit > val->limit) {
        return NULL;
    } 

    uint64_t now = current_millis();
    if (limit == val->limit) {
        val->limit_last_hit_time = now;
    }

    if (now - val->time >= cache_timeout) {
        return NULL;
    } 
    val->ttl = val->time + cache_timeout - now;

    if (limit > val->second_limit) {
        val->second_limit = limit;
    }

    return val;
}

uint32_t depth_cache_get_update_limit(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        return limit;
    }
    
    uint64_t now = current_millis();
    depth_cache_val *val = entry->val;
    if (limit >= val->limit) {
        return limit;
    }
    if (now - val->limit_last_hit_time < DEPTH_LIMIT_EXPIRE_TIME) {
        return val->limit;
    }

    if (val->second_limit >= limit) {
        val->limit = val->second_limit;
        val->second_limit = limit;
        val->limit_last_hit_time = now;
        return val->limit;
    }
    
    val->limit = limit;
    val->limit_last_hit_time = now;
    return limit;
}

int init_depth_cache(int timeout)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_cache_val_dup;
    dt.val_destructor = dict_depth_cache_val_free;

    dict_cache = dict_create(&dt, 256);
    if (dict_cache == NULL) {
        return -__LINE__;
    }

    cache_timeout = timeout;
    return 0;
}

void fini_depth_cache(void)
{
    dict_release(dict_cache);
}