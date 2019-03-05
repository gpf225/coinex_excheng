/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# include "ca_depth_poll.h"
# include "ca_depth_sub.h"
# include "ca_depth_cache.h"
# include "ca_depth_update.h"
# include "ca_depth_wait_queue.h"
# include "ca_server.h"
# include "ca_common.h"

static dict_t *dict_notified_ses = NULL;
static nw_timer update_timer;

static int depth_notify_sub(const char *market, const char *interval, uint32_t limit, json_t *result, uint32_t result_limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);
    dict_t *dict_depth_sub = depth_get_sub();
    dict_entry *entry = dict_find(dict_depth_sub, &key);
    if (entry == NULL) {
        return 0;
    }

    json_t *reply_result = depth_get_result(result, result_limit, limit);
    struct depth_sub_val *val = entry->val;
    dict_iterator *iter = dict_get_iterator(val->sessions);
    while ( (entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        if (dict_find(dict_notified_ses, ses) != NULL) {
            continue;
        }

        json_t *reply = json_object();
        json_object_set_new(reply, "market", json_string(market));
        json_object_set_new(reply, "interval", json_string(interval));
        json_object_set_new(reply, "limit", json_integer(limit));
        json_object_set_new(reply, "data", reply_result);

        int ret = notify_message(ses, CMD_LP_DEPTH_UPDATE, reply);
        json_decref(reply);
        if (ret != 0) {
            log_error("notify_message failed at ses:%p ret:%d", ses, ret);
            break;
        }
        dict_add(dict_notified_ses, ses, NULL);
    }
    dict_release_iterator(iter);
    return 0;
}

int depth_sub_handle(const char *market, const char *interval, json_t *result, uint32_t result_limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);
    dict_t *depth_item = depth_get_item();
    dict_entry *entry = dict_find(depth_item, &key);
    if (entry == NULL) {
        log_info("%s-%s does not have subscribers", market, interval);
        return 0;
    }

    struct depth_sub_limit_val *limit_val = entry->val;
    for (int i = limit_val->size - 1; i >= 0; --i) {
        int limit = limit_val->limits[i];
        if (result_limit >= limit) {
            depth_notify_sub(market, interval, limit, result, result_limit);
        }
    }

    dict_clear(dict_notified_ses);
    return 0;
}

static void on_poll_depth(nw_timer *timer, void *privdata) 
{
    dict_t *depth_item = depth_get_item();
    if (dict_size(depth_item) == 0) {
        log_info("no depth subscribers.");
        return ;
    }

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(depth_item);
    while ( (entry = dict_next(iter)) != NULL) {
        struct depth_sub_limit_val *list = entry->val;
        struct depth_key *key = entry->key;
        uint32_t limit = list->max;

        struct depth_cache_val *cache_val = depth_cache_get(key->market, key->interval, limit);
        if (cache_val != NULL) {
            depth_sub_handle(key->market, key->interval, cache_val->data,  cache_val->limit);
            continue;
        }
        
        limit = depth_cache_get_update_limit(key->market, key->interval, limit);
        depth_update(NULL, NULL, key->market, key->interval, limit);
    }
    dict_release_iterator(iter);
}

int init_depth_poll(void)
{   
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_hash_compare;
    dict_notified_ses = dict_create(&dt, 32);
    if (dict_notified_ses == NULL) {
        return -__LINE__;
    }

    nw_timer_set(&update_timer, settings.poll_depth_interval, true, on_poll_depth, NULL);
    nw_timer_start(&update_timer);
    return 0;
}