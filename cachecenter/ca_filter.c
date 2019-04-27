/*
 * Description: 
 *     History: ouxiangyang, 2019/04/25, create
 */

# include "ca_filter.h"
# include "ca_depth.h"
# include "ca_server.h"

static dict_t *dict_filter;

struct dict_filter_val {
    dict_t  *dict_filter_session;
};

struct filter_list_item {
    rpc_pkg   pkg;
};

static void *dict_filter_val_dup(const void *val)
{
    struct dict_filter_val *obj = malloc(sizeof(struct dict_filter_val));
    memcpy(obj, val, sizeof(struct dict_filter_val));
    return obj;
}

static void dict_filter_val_free(void *val)
{
    struct dict_filter_val *obj = val;
    if (obj->dict_filter_session)
        dict_release(obj->dict_filter_session);
    free(obj);
}

static void dict_filter_list_free(void *val)
{
    list_release(val);
}

static dict_t *dict_create_filter_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = dict_ses_hash_func;
    dt.key_compare     = dict_ses_hash_compare;
    dt.val_destructor  = dict_filter_list_free;

    return dict_create(&dt, 32);
}

static void *list_filter_item_dup(void *val)
{
    struct filter_list_item *obj = malloc(sizeof(struct filter_list_item));
    memcpy(obj, val, sizeof(struct filter_list_item));
    return obj;
}

static void list_filter_item_free(void *val)
{
    free(val);
}

static list_t *create_item_list(void)
{
    list_type type;
    memset(&type, 0, sizeof(struct list_type));
    type.dup     = list_filter_item_dup;
    type.free    = list_filter_item_free;

    return list_create(&type);
}

int add_filter_queue(sds key, uint32_t limit, nw_ses *ses, rpc_pkg *pkg)
{
    dict_entry *entry = dict_find(dict_filter, key);
    if (entry == NULL) {
        struct dict_filter_val val;
        memset(&val, 0, sizeof(struct dict_filter_val));
        val.dict_filter_session = dict_create_filter_session();
        if (val.dict_filter_session == NULL) {
            return -__LINE__;
        }

        entry = dict_add(dict_filter, key, &val);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    struct dict_filter_val *val = entry->val;
    entry = dict_find(val->dict_filter_session, ses);
    if (entry == NULL) {
        list_t *list = create_item_list();
        if (list == NULL) {
            return -__LINE__;
        }

        entry = dict_add(val->dict_filter_session, ses, list);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    list_t *list = entry->val;
    struct filter_list_item item;
    memset(&item, 0, sizeof(struct filter_list_item));
    memcpy(&item.pkg, pkg, RPC_PKG_HEAD_SIZE);

    list_add_node_tail(list, &item);

    return 0;
}

int remove_all_filter(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_filter);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_filter_val *val = entry->val;
        entry = dict_find(val->dict_filter_session, ses);
        if (entry != NULL) {
            dict_delete(val->dict_filter_session, ses);
        }
    }
    dict_release_iterator(iter);

    return 0;
}

void delete_filter_queue(const char *market, const char *interval)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "%s_%s", market, interval);

    dict_entry *entry = dict_find(dict_filter, key);
    if (entry != NULL) {
        dict_delete(dict_filter, entry->key);
    }

    sdsfree(key);
}

static void reply_to_ses(const char *market, const char *interval, bool is_error, json_t *reply, nw_ses *ses, list_t *list)
{
    list_node *node = NULL;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        struct filter_list_item *item = node->value;

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

void reply_filter_message(const char *market, const char *interval, bool is_error, json_t *reply)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "%s_%s", market, interval);

    dict_entry *entry = dict_find(dict_filter, key);
    if (entry == NULL) {
        sdsfree(key);
        return;
    }

    struct dict_filter_val *val = entry->val;
    dict_iterator *iter = dict_get_iterator(val->dict_filter_session);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        list_t *list = entry->val;
        reply_to_ses(market, interval, is_error, reply, ses, list);
    }
    dict_release_iterator(iter);

    dict_delete(dict_filter, &key);
    sdsfree(key);

    return;
}

int init_filter(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_str_hash_func;
    dt.key_compare    = dict_str_compare;
    dt.key_dup        = dict_str_dup;
    dt.key_destructor = dict_str_free;
    dt.val_dup        = dict_filter_val_dup;
    dt.val_destructor = dict_filter_val_free;

    dict_filter = dict_create(&dt, 256);
    if (dict_filter == NULL) {
        return -__LINE__;
    }

    return 0;
}

