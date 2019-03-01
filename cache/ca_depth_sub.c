/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 */

# include "ca_depth_sub.h"
# include "ca_depth_limit_list.h"
# include "ca_depth_common.h"

// dict_depth_sub  market+interval => depth_val
// depth_val.sessions  ses => depth_limit_list
static dict_t *dict_depth_sub = NULL;

struct depth_limit_val {
    dict_t *dict_limits;
};

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
    dict_release(obj->dict_limits);
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
    struct depth_limit_val *obj = val;
    free(obj->dict_limits);
    free(obj);
}

static dict_t* dict_create_sub_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_hash_compdestructor
    dt.val_dup = dict_depth_limit_val_dup;
    dt.val_destructor = dict_depth_limit_val_free;
    return dict_create(&dt, 16);
}

static int depth_poll_add(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_poll, &key);
    if (entry == NULL) {
        struct depth_limit_val val;
        memset(&val, 0, sizeof(struct depth_limit_val));
        val.dict_limits = dict_limit_list_create();
        dict_limit_list_inc(val.dict_limits, limit);
        val.max_limit = limit;

        if (dict_add(dict_depth_poll, &key, &val) == NULL) {
            return -__LINE__;     
        }
        return 0;
    }

    struct depth_limit_val *val = entry->val;
    dict_limit_list_inc(val->dict_limits, limit);
    if (limit > val->max_limit) {
        val->max_limit = dict_limit_list_max(val->dict_limits);
    }
    return 0;
}

static int depth_poll_remove(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_poll, &key);
    if (entry == NULL) {
        return -1;
    }

    struct depth_limit_val *val = entry->val;
    uint cur_limit = dict_limit_list_dec(val->dict_limits, limit);
    if (cur_limit > 0 || limit != val->max_limit) {
        return 0;
    }
    
    int max_limit = dict_limit_list_max(val->dict_limits);
    if (max_limit == 0) {
        dict_delete(dict_depth_poll, &key);
        return 0;
    }
    val->max_limit = max_limit;
    
    return 0;
}

static void depth_session_add(dict_t *sessions, nw_ses *ses, uint32_t limi)
{
    dict_entry *entry = dict_find(sessions, ses);
    if (entry == NULL) {
        struct depth_limit_val val;
        memset(&val, 0, sizeof(struct depth_limit_val));
        val.dict_limits = dict_limit_list_create();

        dict_limit_list_add(val.dict_limits, limit);
        if (dict_add(sessions, ses, &val) == NULL) {
            dict_release(val.dict_limits);
            return -__LINE__;
        }
        return 1;
    }

    struct depth_limit_val *val = entry->val;
    if (dict_limit_list_exist(val->dict_limits, limit)) {
        return 0;
    }
    dict_limit_list_add(val->dict_limits, limit, 1);
    return 1;
}

static void depth_val_add(struct depth_key *key, nw_ses *ses, uint32_t limit)
{
    struct depth_val val;
    memset(&val, 0, sizeof(struct depth_val));
    val.sessions = dict_create_sub_session();
    if (val.sessions == NULL) {
        return -__LINE__;
    }
    val.dict_limits = dict_limit_list_create();
    if (val.dict_limits == NULL) {
        dict_release(val.sessions);
        return -__LINE__;
    } 

    struct depth_limit_val limit_val;
    memset(&limit_val, 0, sizeof(struct depth_limit_val));
    limit_val.dict_limits = dict_limit_list_create();
    if (limit_val.dict_limits == NULL) {
        dict_release(val.sessions);
        dict_release(val.dict_limits);
    }
    dict_limit_list_add(limit_val.dict_limits, limit);

    if (dict_add(val.sessions, ses, &limit_val) == NULL) {
        dict_release(limit_val.dict_limits);
        return -__LINE__;
    }

    if (dict_add(dict_depth_sub, key, &val) == NULL) {
        dict_release(val.sessions);
        dict_release(val.dict_limits);
        dict_release(limit_val.dict_limits);
        return -__LINE__;
    }

    dict_limit_list_inc(val.dict_limits, limit);
}

static void depth_val_update(struct depth_key *key, struct depth_val *val, nw_ses *ses, uint32_t limit)
{
    dict_entry *entry = dict_find(val->sessions, ses);
    if (entry == NULL) {
        struct depth_limit_val limit_val;
        memset(&limit_val, 0, sizeof(struct depth_limit_val));
        limit_val.dict_limits = dict_limit_list_create();

        dict_limit_list_add(limit_val.dict_limits, limit);
        if (dict_add(limit_val.sessions, ses, &limit_val) == NULL) {
            dict_release(limit_val.dict_limits);
            return -__LINE__;
        }

        dict_limit_list_inc(val->dict_limits, limit);
        if (limit > val->max_limit) {
            val->max_limit = limit;
        }
        return 0;
    }
}

int depth_subscribe(nw_ses *ses, const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        depth_val_add(&key, ses, limit);
        return 0;
    }

    struct depth_val *val = entry->val;
    entry = dict_find(val->sessions, ses);
    if (entry == NULL) {
        struct depth_limit_list limit_list;
        memset(&limit_list, 0, sizeof(struct depth_limit_list));
        limit_list.dict_limits = dict_limit_list_create();

        dict_limit_list_add(limit_list.dict_limits, limit);
        if (dict_add(limit_list.sessions, ses, &limit_list) == NULL) {
            dict_release(limit_list.dict_limits);
            return -__LINE__;
        }

        dict_limit_list_inc(val->dict_limits, limit);
        if (limit > val->max_limit) {
            val->max_limit = limit;
        }
        return 0;
    }

    struct depth_limit_list *limit_list = entry->val;
    if (dict_limit_list_exist(limit_list->dict_limits, limit)) {
        return 0;
    }
    dict_limit_list_add(limit_list->dict_limits, limit, 1);
    dict_limit_list_inc(val->dict_limits, limit);
    if (limit > val->max_limit) {
        val->max_limit = limit;
    }
    
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
    if (dict_delete(val->sessions, ses) == 1) {
        depth_poll_remove(market, interval, limit);
    }
    
    return 0;
}

int depth_unsubscribe_all(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *val = entry->val;
        if (dict_delete(val->sessions, ses) == 1) {
            struct depth_key *key = entry->key;
            depth_poll_remove(key->market, key->interval, key->limit);
        }
    }
    dict_release_iterator(iter);
    
    return 0;
}

int init_depth_sub(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_val_dup;
    dt.val_destructor = dict_depth_val_free;

    dict_depth_sub = dict_create(&dt, 128);
    if (dict_depth_sub == NULL) {
        return -__LINE__;
    }

    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_limit_val_dup;
    dt.val_destructor = dict_depth_limit_val_free;

    dict_depth_poll = dict_create(&dt, 32);
    if (dict_depth_poll == NULL) {
        return -__LINE__;
    }

    return 0;
}

dict_t *depth_get_sub()
{
    return dict_depth_sub;
}

dict_t *depth_get_poll()
{
    return dict_depth_poll;
}