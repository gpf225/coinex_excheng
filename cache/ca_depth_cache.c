/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# include "ca_depth_cache.h"

# define DEPTH_LIMIT_MAX_SIZE 32

static double cache_timeout = 1.0;
static int limit_exipre_time = 10.0;
static dict_t *dict_cache;

typedef struct depth_cache_key {
    char market[MARKET_NAME_MAX_LEN];
    char interval[INTERVAL_MAX_LEN];
}depth_cache_key;

static uint32_t dict_depth_cache_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct depth_cache_key));
}

static int dict_depth_cache_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct depth_cache_key));
}

static void *dict_depth_cache_key_dup(const void *key)
{
    struct depth_cache_key *obj = malloc(sizeof(struct depth_cache_key));
    memcpy(obj, key, sizeof(struct depth_cache_key));
    return obj;
}

static void dict_depth_cache_key_free(void *key)
{
    free(key);
}

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

int depth_cache_set(const char *market, const char *interval, uint32_t limit, json_t *data)
{
    depth_cache_key key;
    memset(&key, 0, sizeof(depth_cache_key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key.interval, interval, INTERVAL_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        struct depth_cache_val val;
        memset(&val, 0, sizeof(depth_cache_val));
        val.time = current_timestamp();
        val.data = data;
        json_incref(val.data);
        val.limit = limit;
        val.limit_last_hit_time = current_timestamp();

        if (dict_add(dict_cache, &key, &val) == NULL) {
            return -__LINE__;
        }
        return 0;
    }
    
    struct depth_cache_val *val = entry->val;
    val->limit = limit;
    val->time = current_timestamp();
    json_decref(val->data);
    val->data = data;
    json_incref(val->data);

    return 0;
}

depth_cache_val* depth_cache_get(const char *market, const char *interval, uint32_t limit)
{
    depth_cache_key key;
    memset(&key, 0, sizeof(depth_cache_key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key.interval, interval, INTERVAL_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        return NULL;
    }

    depth_cache_val *val = entry->val;
    if (limit > val->limit) {
        return NULL;
    }

    if (limit == val->limit) {
        val->limit_last_hit_time = current_timestamp();
    } else if (limit > val->second_limit) {
        val->second_limit = limit;
    }

    return val;
}

uint32_t depth_cache_get_update_limit(depth_cache_val *val, uint32_t limit)
{
    assert(val->limit >= limit);

    double cur = current_timestamp();
    if (cur - val->limit_last_hit_time < limit_exipre_time) {
        return val->limit;
    }

    if (val->second_limit >= limit) {
        val->limit = val->second_limit;
        val->second_limit = limit;
        val->limit_last_hit_time = cur;
        return val->limit;
    }
    
    val->limit = limit;
    val->limit_last_hit_time = cur;
    return limit;
}

int init_depth_cache(double timeout)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_cache_key_hash_func;
    dt.key_compare = dict_depth_cache_key_compare;
    dt.key_dup = dict_depth_cache_key_dup;
    dt.key_destructor = dict_depth_cache_key_free;
    dt.val_dup = dict_depth_cache_val_dup;
    dt.val_destructor = dict_depth_cache_val_free;

    dict_cache = dict_create(&dt, 256);
    if (dict_cache == NULL) {
        return -__LINE__;
    }

    cache_timeout = timeout;
    limit_exipre_time = 10.0 * cache_timeout;
    return 0;
}

void fini_depth_cache(void)
{
    dict_release(dict_cache);
}