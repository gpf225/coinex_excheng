/*
 * Description: 
 *     History: yang@haipo.me, 2018/01/01, create
 */

# include <stdlib.h>
# include <string.h>
# include "ut_comm_dict.h"

static uint32_t uint32_set_hash_function(const void *key)
{
    return (uint32_t)(uintptr_t)key;
}

static int uint32_set_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

dict_t *uint32_set_create(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = uint32_set_hash_function;
    type.key_compare = uint32_set_key_compare;

    return dict_create(&type, 64);
}

void uint32_set_add(dict_t *set, uint32_t value)
{
    dict_add(set, (void *)(uintptr_t)value, NULL);
}

bool uint32_set_exist(dict_t *set, uint32_t value)
{
    dict_entry *entry = dict_find(set, (void *)(uintptr_t)value);
    if (entry)
        return true;
    return false;
}

size_t uint32_set_num(dict_t *set)
{
    return dict_size(set);
}

void uint32_set_release(dict_t *set)
{
    dict_release(set);
}

uint32_t str_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

void *str_dict_key_dup(const void *key)
{
    return strdup(key);
}

int str_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

void str_dict_key_free(void *key)
{
    free(key);
}

uint32_t sds_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sdslen((sds)key));
}

void *sds_dict_key_dup(const void *key)
{
    return sdsdup((sds)key);
}

int sds_dict_key_compare(const void *key1, const void *key2)
{
    return sdscmp((sds)key1, (sds)key2);
}

void sds_dict_key_free(void *key)
{
    sdsfree((sds)key);
}

uint32_t uint32_dict_hash_func(const void *key)
{
    return (uintptr_t)key;
}

int uint32_dict_key_compare(const void *key1, const void *key2)
{
    return (uintptr_t)key1 == (uintptr_t)key2 ? 0 : 1;
}
