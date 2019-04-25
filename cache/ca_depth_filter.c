/*
 * Description: 
 *     History: ouxiangyang, 2019/04/25, create
 */

# include "ca_depth_filter.h"
# include "ca_depth.h"
# include "ca_server.h"

static dict_t *diect_depth_filter;

static void *dict_depth_filter_val_dup(const void *val)
{
    struct dict_depth_filter_val *obj = malloc(sizeof(struct dict_depth_filter_val));
    memcpy(obj, val, sizeof(struct dict_depth_filter_val));
    return obj;
}

static void depth_depth_filter_val_free(void *val)
{
    struct dict_depth_filter_val *obj = val;
    if (obj->dict_filter_session)
        dict_release(obj->dict_filter_session);
    free(obj);
}

static void dict_depth_filter_list_free(void *val)
{
    list_release(val);
}

static dict_t *dict_create_filter_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = dict_ses_hash_func;
    dt.key_compare     = dict_ses_hash_compare;
    dt.val_destructor  = dict_depth_filter_list_free;

    return dict_create(&dt, 32);
}

static void *list_depth_filter_item_dup(void *val)
{
    struct depth_filter_item *obj = malloc(sizeof(struct depth_filter_item));
    memcpy(obj, val, sizeof(struct depth_filter_item));
    return obj;
}

static void list_depth_filter_item_free(void *val)
{
    free(val);
}

static list_t *create_depth_item_list(void)
{
    list_type type;
    memset(&type, 0, sizeof(struct list_type));
    type.dup     = list_depth_filter_item_dup;
    type.free    = list_depth_filter_item_free;

    return list_create(&type);
}

int add_depth_filter_queue(const char *market, const char *interval, uint32_t limit, nw_ses *ses, rpc_pkg *pkg)
{
    struct dict_depth_key key;
    depth_set_key(&key, market, interval);    

    dict_entry *entry = dict_find(diect_depth_filter, &key);
    if (entry == NULL) {
        struct dict_depth_filter_val val;
        memset(&val, 0, sizeof(struct dict_depth_filter_val));
        val.dict_filter_session = dict_create_filter_session();
        if (val.dict_filter_session == NULL)
            return -__LINE__;

        entry = dict_add(diect_depth_filter, &key, &val);
        if (entry == NULL)
            return -__LINE__;
    }

    struct dict_depth_filter_val *val = entry->val;
    entry = dict_find(val->dict_filter_session, ses);
    if (entry == NULL) {
        list_t *list = create_depth_item_list();
        if (list == NULL)
            return -__LINE__;

        entry = dict_add(val->dict_filter_session, ses, list);
        if (entry == NULL)
            return -__LINE__;
    }

    list_t *list = entry->val;
    struct depth_filter_item item;
    memset(&item, 0, sizeof(struct depth_filter_item));
    memcpy(&item.pkg, pkg, RPC_PKG_HEAD_SIZE);

    list_add_node_tail(list, &item);
    return 0;
}

int depth_filter_remove_all(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(diect_depth_filter);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_depth_filter_val *val = entry->val;
        entry = dict_find(val->dict_filter_session, ses);
        if (entry != NULL) {
            dict_delete(val->dict_filter_session, ses);
        }
    }
    dict_release_iterator(iter);

    return 0;
}

void delete_depth_filter_queue(const char *market, const char *interval)
{
    struct dict_depth_key key;
    depth_set_key(&key, market, interval);    

    dict_entry *entry = dict_find(diect_depth_filter, &key);
    if (entry != NULL) {
        dict_delete(diect_depth_filter, entry->key);
    }
}

static void reply_to_out_ses(const char *market, const char *interval, bool is_error, json_t *reply, nw_ses *ses, list_t *list)
{
    list_node *node = NULL;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        struct depth_filter_item *item = node->value;

        json_t *result = json_object();
        json_object_set    (result, "error", json_object_get(reply, "error"));
        json_object_set    (result, "result", json_object_get(reply, "result"));
        json_object_set_new(result, "id", json_integer(item->pkg.req_id));

        json_t *new_result = result;
        if (!is_error) {
            new_result = json_object();
            json_object_set_new(new_result, "ttl", json_integer(settings.cache_timeout));
            json_object_set_new(new_result, "cache_result", result);
        }

        int ret = reply_json(ses, &item->pkg, new_result);
        if (ret != 0) {
            log_error("send_result fail, market: %s, interval: %s", market, interval);
        }
        json_decref(new_result);  
    }
    list_release_iterator(iter);

    return;
}

void depth_out_reply(const char *market, const char *interval, bool is_error, json_t *reply)
{
    struct dict_depth_key key;
    depth_set_key(&key, market, interval);    

    dict_entry *entry = dict_find(diect_depth_filter, &key);
    if (entry == NULL)
        return;

    struct dict_depth_filter_val *val = entry->val;
    dict_iterator *iter = dict_get_iterator(val->dict_filter_session);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        list_t *list = entry->val;
        reply_to_out_ses(market, interval, is_error, reply, ses, list);
    }
    dict_release_iterator(iter);

    dict_delete(diect_depth_filter, &key);
    return;
}

int init_depth_filter_queue(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_depth_hash_func;
    dt.key_compare    = dict_depth_key_compare;
    dt.key_dup        = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup        = dict_depth_filter_val_dup;
    dt.val_destructor = depth_depth_filter_val_free;

    diect_depth_filter = dict_create(&dt, 256);
    if (diect_depth_filter == NULL) {
        return -__LINE__;
    }

    return 0;
}

