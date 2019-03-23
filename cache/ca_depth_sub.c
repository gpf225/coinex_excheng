/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 */

# include "ca_depth_sub.h"
# include "ca_depth_cache.h"
# include "ca_depth_update.h"
# include "ca_common.h"
# include "ca_server.h"

# define DEPTH_LIMIT_MAX_SIZE 32

static dict_t *dict_depth_sub = NULL;  // map: depth_key => depth_sub_val
static nw_timer update_timer;

struct depth_sub_val {
    dict_t *sessions; 
    json_t *last;
    double time;
};

static void *dict_depth_sub_val_dup(const void *val)
{
    struct depth_sub_val *obj = malloc(sizeof(struct depth_sub_val));
    memcpy(obj, val, sizeof(struct depth_sub_val));
    return obj;
}

static void dict_depth_sub_val_free(void *val)
{
    struct depth_sub_val *obj = val;
    dict_release(obj->sessions);
    free(obj);
}

static dict_t* dict_create_depth_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_hash_compare;
    return dict_create(&dt, 16);
}

static bool is_json_equal(json_t *lhs, json_t *rhs)
{
    if (lhs == NULL || rhs == NULL) {
        return false;
    }
    char *lhs_str = json_dumps(lhs, JSON_SORT_KEYS);
    char *rhs_str = json_dumps(rhs, JSON_SORT_KEYS);
    int ret = strcmp(lhs_str, rhs_str);
    free(lhs_str);
    free(rhs_str);
    return ret == 0;
}

static bool is_depth_equal(json_t *last, json_t *now)
{
    if (last == NULL || now == NULL) {
        return false;
    }
    
    if (!is_json_equal(json_object_get(last, "asks"), json_object_get(now, "asks"))) {
        return false;
    }
    return is_json_equal(json_object_get(last, "bids"), json_object_get(now, "bids"));
}

static json_t* get_reply_result(const char *market, const char *interval, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "market", json_string(market));
    json_object_set_new(reply, "interval", json_string(interval));
    json_object_set    (reply, "data", result);
    return reply;
}

int depth_sub_reply(const char *market, const char *interval, json_t *result)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        return 0;
    }

    const double now = current_timestamp();
    struct depth_sub_val *val = entry->val;
    if (is_depth_equal(val->last, result)) {
        if (now - val->time <= settings.poll_depth_interval) {
            return 0;
        }
    }
   
    if (val->last != NULL) {
        json_decref(val->last);
    }
    val->last = result;
    json_incref(val->last);
    val->time = now;

    dict_iterator *iter = dict_get_iterator(val->sessions);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        json_t *reply = get_reply_result(market, interval, result);
        notify_message(ses, CMD_LP_DEPTH_UPDATE, reply);
        json_decref(reply);
    }
    dict_release_iterator(iter);
    return 0;
}

static void on_poll_depth_timer(nw_timer *timer, void *privdata) 
{   
    int count = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    while ( (entry = dict_next(iter)) != NULL) {
        struct depth_sub_val *val = entry->val;
        if (dict_size(val->sessions) == 0) {
            continue;
        }
        ++count;
        struct depth_key *key = entry->key;
        struct depth_cache_val *cache_val = depth_cache_get(key->market, key->interval);
        if (cache_val != NULL) {
            depth_sub_reply(key->market, key->interval, cache_val->data);
            continue;
        }
        
        depth_update_sub(key->market, key->interval);
    }
    dict_release_iterator(iter);
    log_trace("depth update sub size:%d", count);
}

int init_depth_sub(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_sub_val_dup;
    dt.val_destructor = dict_depth_sub_val_free;

    dict_depth_sub = dict_create(&dt, 128);
    if (dict_depth_sub == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }
    
    nw_timer_set(&update_timer, settings.poll_depth_interval, true, on_poll_depth_timer, NULL);
    nw_timer_start(&update_timer);

    return 0;
}

int fini_depth_sub(void)
{
    dict_release(dict_depth_sub);
    dict_depth_sub = NULL;
    return 0;
}

int depth_subscribe(nw_ses *ses, const char *market, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        struct depth_sub_val val;
        memset(&val, 0, sizeof(val));

        val.sessions = dict_create_depth_session();
        if (val.sessions == NULL) {
            return -__LINE__;
        }
        entry = dict_add(dict_depth_sub, &key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct depth_sub_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);
    return 0;  
}

int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        return 0;
    }
    
    struct depth_sub_val *val = entry->val;
    dict_delete(val->sessions, ses);
    return 0;
}

int depth_unsubscribe_all(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    while ( (entry = dict_next(iter)) != NULL ) {
        struct depth_sub_val *val = entry->val;
        dict_delete(val->sessions, ses);
    }
    dict_release_iterator(iter);
    return 0;
}

int depth_send_last(nw_ses *ses, const char *market, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        return 0;
    }

    struct depth_sub_val *obj = entry->val;
    if (obj->last == NULL) {
        return 0;
    }
    
    json_t *reply = get_reply_result(market, interval, obj->last);
    int ret = notify_message(ses, CMD_LP_DEPTH_UPDATE, reply);
    json_decref(reply);
    return ret;
}

size_t depth_subscribe_number(void)
{
    return dict_size(dict_depth_sub);
}
