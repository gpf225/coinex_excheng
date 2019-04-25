/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <jansson.h>
# include "ut_dict.h"

json_t *get_order_info()
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(123));
    json_object_set_new(info, "type", json_integer(456));
    json_object_set_new(info, "side", json_integer(789));
    return info;
}

static uint32_t dict_order_hash_function(const void *key)
{
    return dict_generic_hash_function(key, 8);
}

static void *dict_order_key_dup(const void *key)
{
    void *obj = malloc(sizeof(uint64_t));
    memcpy(obj, key, sizeof(uint64_t));
    return obj;
}

static int dict_order_key_compare(const void *key1, const void *key2)
{
    uint64_t *order_id1 = (uint64_t*)key1;
    uint64_t *order_id2 = (uint64_t*)key2;
    return *order_id1 - *order_id2;
}

static void dict_order_key_free(void *key)
{
    free(key);
}

static void dict_order_value_free(void *value)
{
    json_decref((json_t *)value);
}


int main(void)
{
    dict_types types_order;
    memset(&types_order, 0, sizeof(dict_types));
    types_order.hash_function  = dict_order_hash_function;
    types_order.key_compare    = dict_order_key_compare;
    types_order.key_dup        = dict_order_key_dup;
    types_order.key_destructor = dict_order_key_free;
    types_order.val_destructor = dict_order_value_free;

    dict_t *dict_order = dict_create(&types_order, 1024);
    if (dict_order == 0) {
        return -__LINE__;
    }

    /*
    json_t *order_info = get_order_info();
    json_object_set_new(order_info, "finished", json_true());
    dict_entry *entry = dict_add(dict_order, &order_info->id, order_info);
    json_t *order_info2 = entry->val;
    char *data = json_dumps(order_info2, JSON_INDENT(4));
    log_info("add finished order info: %s",  data);
    free(data);
    */
}

