/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# include "ca_depth_cache.h"
# include "ca_common.h"

# define DEPTH_LIMIT_MAX_SIZE 32

static int cache_timeout = 1000;
static dict_t *dict_cache;

static struct depth_cache_val *depth_cache_val_create(void)
{
    struct depth_cache_val *val = malloc(sizeof(struct depth_cache_val));
    if (val == NULL) {
        return NULL;
    }
    memset(val, 0, sizeof(struct depth_cache_val));
    val->limits = depth_limit_list_create();
    if (val->limits == NULL) {
        free(val);
        return NULL;
    }
    return val;
}

static void depth_cache_val_free(void *val)
{
    struct depth_cache_val *obj = val;
    if (obj->data != NULL) {
        json_decref(obj->data);
    }
    depth_limit_list_free(obj->limits);
    free(obj);
}

int depth_cache_set(const char *market, const char *interval, uint32_t limit, json_t *data)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_cache, &key);
    if (entry == NULL) {
        struct depth_cache_val *val = depth_cache_val_create();
        if (val == NULL) {
            return -__LINE__;
        }

        if (dict_add(dict_cache, &key, val) == NULL) {
            depth_cache_val_free(val);
            return -__LINE__;
        }
        depth_limit_list_add(val->limits, limit, current_timestamp());
        return 0;
    }
    
    struct depth_cache_val *val = entry->val;
    if (val->data != NULL) {
        json_decref(val->data);
    }
    val->data = data;
    json_incref(val->data);
    val->limit = limit;
    val->time = current_millis();

    return 0;
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
    depth_limit_list_reset(val->limits, limit, current_timestamp());
    if (limit > val->limit) {
        return NULL;
    }

    uint64_t now = current_millis();
    if (now - val->time >= cache_timeout) {
        return NULL;
    } 
    val->ttl = now - val->time - cache_timeout;
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

    depth_cache_val *val = entry->val;
    uint32_t max_limit = 0;
    while (1) {
        max_limit = depth_limit_list_max(val->limits);
        if (max_limit == 0) {
            break; 
        }
        uint32_t last_time = depth_limit_list_get(val->limits, max_limit);
        uint32_t now = current_timestamp();
        uint32_t diff = now - last_time;
        if (diff < 3) {
            // 当某个limit在3秒内有被访问，符合要求
            break;
        } else if (diff > 5) {
            // 当某个limit选项连续5秒都未有请求来到时，从limit列表移除
            depth_limit_list_remove(val->limits, max_limit);
        }
    }

    if (limit > max_limit) {
        return limit;
    }
    
    return max_limit;
}

int init_depth_cache(int timeout)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_destructor = depth_cache_val_free;

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