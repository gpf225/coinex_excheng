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
    char *filename = "./marketlist.json";
    json_error_t error;
    json_t *data_obj = json_load_file(filename, 0, &error);
    if (data_obj == NULL) {
        printf("json_load_file from: %s fail: %s in line: %d", filename, error.text, error.line);
        return 0;
    }

    int marketcount = 0;
    int sum[8] = {0};
    json_t *market_list = json_object_get(data_obj, "data");
    const char *key;
    json_t *val;
    json_object_foreach(market_list, key, val) {
        uint32_t hash = dict_generic_hash_function(key, strlen(key));
        int reader_id = hash % 8;
        if (reader_id == 6) {
            printf("%s\n", key);
        }
        sum[reader_id]++;
        marketcount++;
    }    
    json_decref(market_list);

    /*
    json_t *market_list = json_object_get(data_obj, "data");
    size_t marketcount = json_array_size(market_list);
    printf("marketcount: %ld\n", marketcount);

    
    for (int i = 0; i < marketcount; i++) {
        json_t *item = json_array_get(market_list, i);
        const char *market = json_string_value(json_object_get(item, "name"));
        uint32_t hash = dict_generic_hash_function(market, strlen(market));
        int reader_id = hash % 8;
        if (reader_id == 6) {
            printf("%s\n", market);
        }
        sum[reader_id]++;
    }
    json_decref(market_list);
    */

    for (int i = 0; i < 8; i++) {
        printf("%d: %d\n", i, sum[i]);
    }
    printf("marketcount: %d\n", marketcount);
    return 0;
}

