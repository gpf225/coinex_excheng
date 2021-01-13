/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# include "iw_config.h"
# include "iw_order.h"
# include "iw_server.h"

static dict_t *dict_sub;

struct sub_unit {
    void *ses;
    char market[MARKET_NAME_MAX_LEN + 1];
};

static void dict_sub_val_free(void *val)
{
    list_release(val);
}

static int list_node_compare(const void *value1, const void *value2)
{
    return memcmp(value1, value2, sizeof(struct sub_unit));
}

static void *list_node_dup(void *value)
{
    struct sub_unit *obj = malloc(sizeof(struct sub_unit));
    memcpy(obj, value, sizeof(struct sub_unit));
    return obj;
}

static void list_node_free(void *value)
{
    free(value);
}

int init_order(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func;
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_destructor = dict_sub_val_free;

    dict_sub = dict_create(&dt, 1024);
    if (dict_sub == NULL)
        return -__LINE__;

    return 0;
}

int order_subscribe(uint32_t user_id, nw_ses *ses, const char *market)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
        list_type lt;
        memset(&lt, 0, sizeof(lt));
        lt.dup = list_node_dup;
        lt.free = list_node_free;
        lt.compare = list_node_compare;
        list_t *list = list_create(&lt);
        if (list == NULL)
            return -__LINE__;
        entry = dict_add(dict_sub, key, list);
        if (entry == NULL)
            return -__LINE__;
    }

    list_t *list = entry->val;
    struct sub_unit unit;
    memset(&unit, 0, sizeof(unit));
    unit.ses = ses;
    sstrncpy(unit.market, market, sizeof(unit.market));

    if (list_find(list, &unit) != NULL)
        return 0;
    if (list_add_node_tail(list, &unit) == NULL)
        return -__LINE__;

    return 0;
}

int order_unsubscribe(nw_ses *ses)
{
    dict_entry *entry;
    dict_iterator *iter_d = dict_get_iterator(dict_sub);
    while ((entry = dict_next(iter_d)) != NULL) {
        list_t *list = entry->val;
        list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
        list_node *node;
        while ((node = list_next(iter)) != NULL) {
            struct sub_unit *unit = node->value;
            if (unit->ses == ses) {
                list_del(list, node);
            }
        }
        list_release_iterator(iter);
    }
    dict_release_iterator(iter_d);
    
    if (list->len == 0) {
        dict_delete(dict_sub, key);
    }

    return 0;
}

int order_on_update(uint32_t user_id, int event, json_t *order)
{
    const char *market = json_string_value(json_object_get(order, "market"));
    if (market == NULL)
        return -__LINE__;

    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL)
        return 0;

    json_t *params = json_array();
    json_array_append_new(params, json_integer(event));
    json_array_append(params, order);

    int count = 0;
    list_t *list = entry->val;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        struct sub_unit *unit = node->value;
        if (strcmp(unit->market, market) == 0) {
            ws_send_notify(unit->ses, "order.update", params);
            count += 1;
        }
    }
    list_release_iterator(iter);
    json_decref(params);
    profile_inc("order.update", count);

    return 0;
}

int order_on_update_stop(uint32_t user_id, int event, json_t *order)
{
    const char *market = json_string_value(json_object_get(order, "market"));
    if (market == NULL)
        return -__LINE__;

    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL)
        return 0;

    json_t *params = json_array();
    json_array_append_new(params, json_integer(event));
    json_array_append(params, order);

    list_t *list = entry->val;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        struct sub_unit *unit = node->value;
        if (strcmp(unit->market, market) == 0) {
            ws_send_notify(unit->ses, "order.update_stop", params);
            profile_inc("order.update_stop", 1);
        }
    }
    list_release_iterator(iter);
    json_decref(params);

    return 0;
}

size_t order_subscribe_number(void)
{
    return dict_size(dict_sub);
}

