/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# include "lp_depth_sub.h"
# include "lp_common_struct.h"

static dict_t *dict_depth_sub = NULL;  // map: depth_key => depth_val
static dict_t *dict_depth_item = NULL; // map: depth_key => depth_limit_val

static uint32_t dict_depth_key_hash_func(const void *key)
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
    dict_release(obj->sessions);
    free(obj);
}

static void *dict_depth_limit_val_dup(const void *val)
{
    struct depth_limit_val *obj = malloc(sizeof(struct depth_limit_val));
    memcpy(obj, val, sizeof(struct depth_limit_val));
    return obj;
}

static void dict_depth_limit_val_free(void *val)
{
    free(val);
}

static int init_dict_depth_sub(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_val_dup;
    dt.val_destructor = dict_depth_val_free;

    dict_depth_sub = dict_create(&dt, 32);
    if (dict_depth_sub == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    return 0;
}

static int init_dict_depth_item(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_limit_val_dup;
    dt.val_destructor = dict_depth_limit_val_free;

    dict_depth_item = dict_create(&dt, 32);
    if (dict_depth_item == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    return 0;
}

static dict_t* dict_create_depth_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = common_ses_hash_func;
    dt.key_compare = common_ses_compare;
    return dict_create(&dt, 16);
}

void depth_set_key(struct depth_key *key, const char *market, const char *interval, uint32_t limit)
{
    memset(key, 0, sizeof(struct depth_key));
    strncpy(key->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key->interval, interval, INTERVAL_MAX_LEN - 1);
    key->limit = limit;
}

static int depth_limit_list_find(struct depth_limit_val *val, int limit)
{
    for (int i = 0; i < val->size; ++i) {
        if (val->limits[i] == limit) {
            return i;
        }
    }

    return -1;
}

static void depth_limit_list_add(struct depth_limit_val *val, int limit) 
{
    val->limits[val->size] = limit;
    ++val->size;
    if (limit > val->max) {
        val->max = limit;
    }
}

static void depth_limit_list_remove(struct depth_limit_val *val, int index) 
{
    if (val->size == 1) {
        --val->size;
        return ;
    }

    int remove_val = val->limits[index];
    for (int i = index; i < val->size-1; ++i) {
        val->limits[i] = val->limits[i + 1];
    }

    --val->size;
    if (val->max > remove_val) {
        return ;
    }
    
    int max = 0;
    for (int i = 0; i < val->size; ++i) {
        if (val->limits[i] > max) {
            max = val->limits[i];
        }
    }

    val->max = max;
}

static int depth_item_add(const char *market, const char *interval, int limit) 
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_item, &key);
    if (entry == NULL) {
        struct depth_limit_val val;
        memset(&val, 0, sizeof(val));

        entry = dict_add(dict_depth_item, &key, &val);
        if (entry == NULL) {
            log_fatal("depth add poll item faild, server maybe running in a dangerious way!!!");
            return -__LINE__;;
        }
    }
    
    struct depth_limit_val *val = entry->val;
    if (depth_limit_list_find(val, limit) == -1) {
        log_info("add depth item, %s-%s-%d", market, interval, limit);
        depth_limit_list_add(val, limit);
    }

    return 0;
}

static int depth_item_remove(const char *market, const char *interval, int limit) 
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_item, &key);
    assert(entry != NULL);

    struct depth_limit_val *val = entry->val;
    int index = depth_limit_list_find(val, limit);
    if (index != -1) {
        depth_limit_list_remove(val, index);
        log_info("remove depth item, %s-%s-%d", market, interval, limit);
        if (val->size == 0) {
            dict_delete(dict_depth_item, &key);
        }
        return 1;
    }

    return 0;
}

int depth_subscribe(nw_ses *ses, const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);
    
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry != NULL) {
        // 每个ses只能订阅一次同类型的深度信息
        struct depth_val *val = entry->val;
        if (dict_find(val->sessions, ses) != NULL) {
            log_warn("ses:%p market:%s interval:%s limit:%u has subscribed", ses, market, interval, limit);
            return 0;
        } 
    }

    if (entry == NULL) {
        struct depth_val val;
        memset(&val, 0, sizeof(val));

        val.sessions = dict_create_depth_session();
        if (val.sessions == NULL) {
            log_fatal("dict_create failed, server maybe run in a dangerious way!!!");
            return -__LINE__;
        }
        entry = dict_add(dict_depth_sub, &key, &val);
        if (entry == NULL) {
            log_fatal("dict_add failed, server maybe run in a dangerious way!!!");
            dict_release(val.sessions);
            return -__LINE__;
        }
        depth_item_add(market, interval, limit);
    }

    struct depth_val *obj = entry->val;
    if (dict_add(obj->sessions, ses, NULL) == NULL) {
        log_fatal("dict_add failed, server maybe run in a dangerious way!!!");
        return -__LINE__;
    }
    
    log_info("ses:%p subscribe depth: %s-%s-%d", ses, market, interval, limit);
    return 0;  
}

int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        return 0;
    }
    
    struct depth_val *val = entry->val;
    if (dict_find(val->sessions, ses) == NULL) {
        return 0;
    }
    
    dict_delete(val->sessions, ses);
    if (dict_size(val->sessions) == 0) {
        dict_delete(dict_depth_sub, &key);
        depth_item_remove(market, interval, limit);
    }
    
    log_info("ses:%p unsubscribe depth: %s-%s-%d", ses, market, interval, limit);
    return 0;
}

int depth_unsubscribe_all(nw_ses *ses)
{
    if (dict_size(dict_depth_sub) == 0) {
        log_warn("no subscribers, no need to unsubscribe!!!");
        return 0;
    }
    
    int count = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    while ( (entry = dict_next(iter)) != NULL ) {
        struct depth_val *val = entry->val;
        if (dict_delete(val->sessions, ses) == 1) {
            struct depth_key *key = entry->key;
            log_info("ses:%p unsubscribe depth: %s-%s-%d size:%u", ses, key->market, key->interval, key->limit, dict_size(val->sessions));

            if (dict_size(val->sessions) == 0) {
                depth_item_remove(key->market, key->interval, key->limit);
                dict_delete(dict_depth_sub, key);
            }
            ++count;
        }
    }
    
    dict_release_iterator(iter);
    log_trace("depth_unsubscribe_all %d items has been unsubscribed", count);
    return count;
}

int init_depth_sub(void)
{
    int ret = init_dict_depth_sub();
    if (ret != 0) {
        return ret;
    }
    ret = init_dict_depth_item();
    if (ret != 0) {
        return ret;
    }

    return 0;
}

int fini_depth_sub(void)
{
    dict_release(dict_depth_sub);
    dict_depth_sub = NULL;
    dict_release(dict_depth_item);
    dict_depth_item = NULL;
    return 0;
}

dict_t* depth_get_sub(void)
{
    return dict_depth_sub;
}

dict_t* depth_get_item(void)
{
    return dict_depth_item;
}

size_t depth_subscribe_number(void)
{
    return dict_size(dict_depth_sub);
}

size_t depth_poll_number(void)
{
    return dict_size(dict_depth_item);
}