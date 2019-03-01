/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# include "ca_depth_poll.h"
# include "ca_depth_sub.h"
# include "ca_depth_cache.h"
# include "ca_depth_common.h"

static dict_t *dict_wait_queue = NULL;  // 深度订阅等待列表
static nw_timer timer;

static void dict_wait_queue_add(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);
    dict_entry *entry = dict_find(dict_wait_queue, &key);
    if (entry != NULL) {
        return ;
    }

    dict_add(dict_wait_queue, &key, NULL);
}

static void depth_notify1(const char *market, const char *interval, uint32_t limit, json_t *result)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);

    dict_t *dict_depth_sub = depth_get_sub();
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        return ;
    }

    struct depth_val *val = entry->val;
}

static void depth_notify(struct depth_limit_val *val, const char *market, const char *interval, struct depth_cache_val *cache_val)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(val->dict_limits);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t limit = (uintptr_t)entry->val;

    }
}

static int on_poll_depth(nw_timer *timer, void *privdata) 
{
    dict_clear(dict_wait_queue);

    dict_t *dict_depth_poll = depth_get_poll();
    if (dict_size(dict_depth_poll) == 0) {
        log_info("no depth subscribers");
        return 0;
    }

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_poll);
    while ( (entry = dict_next(iter)) != NULL) {
        struct depth_limit_val *val = entry->val;
        struct depth_key *key = entry->key;
        const char *market = key->market;
        const char *interval = key->interval;
        uint32_t limit = val->max_limit;
        log_trace("poll depth: %s-%s-%u", market, interval, limit);

        struct depth_cache_val *cache_val = depth_cache_get(market, interval, limit);
        if (cache_val == NULL) {
            dict_wait_queue_add(market, interval, limit);
            depth_update(market, interval, limit);
            return 0;
        }
        
        uint64_t now = current_millis();
        if ((now - cache_val->time) < settings.cache_timeout) {
            //depth_notify(market, interval, cache_val);
            profile_inc("depth_cache", 1);
            return 0;
        }

        dict_wait_queue_add(market, interval, limit);
        if (cache_val->updating) {
            if (now - cache_val->update_millis < 200) {
                return 0;
            }
            log_warn("%s-%s-%u not reply, update_millis:%"PRIu64" now:%"PRIu64, market, interval, limit, cache_val->update_millis, now);
        }
        cache_val->updating = true;
        cache_val->update_millis = now;
        limit = depth_cache_get_update_limit(cache_val, limit);
        depth_update(market, interval, limit);
    }
    dict_release_iterator(iter);
    
}

int init_depth_poll(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;

    dict_wait_queue = dict_create(&dt, 128);
    if (dict_wait_queue == NULL) {
        return -__LINE__;
    }

    return 0;
}