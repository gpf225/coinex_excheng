/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/01/18, create
 */

# include "aw_asset_sub.h"
# include "aw_server.h"

static dict_t *dict_sub;
static dict_t *dict_user;

static dict_t* create_sub_dict()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = uint32_dict_hash_func;
    dt.key_compare   = uint32_dict_key_compare;

    return dict_create(&dt, 1024);
}

static int list_node_compare(const void *value1, const void *value2)
{
    return (uintptr_t)value1 == (uintptr_t)value2 ? 0 : 1;
}

static list_t* create_user_list() 
{
    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare = list_node_compare;
    return list_create(&lt);
}

static void dict_user_val_free(void *val)
{
    list_release(val);
}

static dict_t* create_user_dict()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = ptr_dict_hash_func;
    dt.key_compare    = ptr_dict_key_compare;
    dt.val_destructor = dict_user_val_free;

    return dict_create(&dt, 1024);
}

static dict_t* create_ses_dict()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = ptr_dict_hash_func;
    dt.key_compare   = ptr_dict_key_compare;

    return dict_create(&dt, 1);
}

int init_asset_sub(void)
{
    dict_sub = create_sub_dict();
    if (dict_sub == NULL) {
        return -__LINE__;
    }

    dict_user = create_user_dict();
    if (dict_user == NULL) {
        return -__LINE__;
    }

    return 0;
}

static int add_subscribe(uint32_t user_id, nw_ses *ses)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
        dict_t *clients = create_ses_dict();
        if (clients == NULL) {
            return -__LINE__;
        }
        entry = dict_add(dict_sub, key, clients);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    dict_t *clients = entry->val;
    if (dict_add(clients, ses, NULL) == NULL) {
        return -__LINE__;
    }

    return 0;
}

static int remove_subscribe(uint32_t user_id, nw_ses *ses)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
       return 0;
    }

    dict_t *clients = entry->val;
    dict_delete(clients, ses);
    if (dict_size(clients) == 0) {
        dict_delete(dict_sub, key);
    }

    return 0;
}

int asset_subscribe_sub(nw_ses *ses, json_t *sub_users)
{
    list_t *list = create_user_list();
    if (list == NULL) {
        return -__LINE__;
    }
    for (size_t i = 0; i < json_array_size(sub_users); ++i) {
        uint32_t user_id = json_integer_value(json_array_get(sub_users, i));
        void *value = (void *)(uintptr_t)user_id;
        if (list_add_node_tail(list, value) == NULL) {
            list_release(list);
            return -__LINE__;
        }
    }
    if (dict_add(dict_user, ses, list) == NULL) {
        list_release(list);
        return -__LINE__;
    }

    for (size_t i = 0; i < json_array_size(sub_users); ++i) {
        uint32_t user_id = json_integer_value(json_array_get(sub_users, i));
        int ret = add_subscribe(user_id, ses);
        if (ret < 0) {
            dict_delete(dict_user, ses);
            return ret;
        }
    }

    return 0;
}

int asset_unsubscribe_sub(nw_ses *ses)
{
    dict_entry *entry = dict_find(dict_user, ses);
    if (entry == NULL) {
        return 0;
    }

    list_t *list = entry->val;
    int count = list_len(list);
    list_node *node = NULL;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ( (node = list_next(iter)) != NULL) {
        uint32_t user_id = (uintptr_t)node->value;
        remove_subscribe(user_id, ses);
    }
    list_release_iterator(iter);

    dict_delete(dict_user, ses);
    return count;
}

int asset_on_update_sub(uint32_t user_id, const char *asset, const char *available, const char *frozen, double timestamp)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
        return 0;
    }

    json_t *params = json_array();
    json_t *result = json_object();
    json_t *unit = json_object();

    json_object_set_new(unit, "available", json_string(available));
    json_object_set_new(unit, "frozen", json_string(frozen));
    json_object_set_new(unit, "timestamp", json_real(timestamp));
    json_object_set_new(result, asset, unit);

    json_array_append_new(params, json_integer(user_id));
    json_array_append_new(params, result);

    int count = 0;
    dict_t *clients = entry->val;
    dict_iterator *iter = dict_get_iterator(clients);
    while ( (entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        ws_send_notify(ses, "asset.update_sub", params);
        ++count;
    }
    dict_release_iterator(iter);

    json_decref(params);
    profile_inc("asset.update_sub", count);

    return 0;
}

size_t asset_subscribe_sub_number(void)
{
    return dict_size(dict_sub);
}

