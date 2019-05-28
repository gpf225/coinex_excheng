/*
 * Description: 
 *     History: yang@haipo.me, 2018/01/01, create
 */

# include <stdlib.h>
# include <string.h>
# include "ut_comm_dict.h"

uint32_t uint32_dict_hash_func(const void *key)
{
    return (uint32_t)(uintptr_t)key;
}

int uint32_dict_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

dict_t *uint32_set_create(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = uint32_dict_hash_func;
    type.key_compare = uint32_dict_key_compare;

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

void uint32_set_clear(dict_t *set)
{
    dict_clear(set);
}

void uint32_set_release(dict_t *set)
{
    dict_release(set);
}

dict_t *uint32_mpd_dict_create(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = uint32_dict_hash_func;
    type.key_compare    = uint32_dict_key_compare;
    type.val_dup        = mpd_dict_val_dup;
    type.val_destructor = mpd_dict_val_free;
    return dict_create(&type, 64);
}

void uint32_mpd_dict_plus(dict_t *dict, uint32_t key, mpd_t *val)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)val);
    if (entry) {
        mpd_t *m = entry->val;
        mpd_add(m, m, val, &mpd_ctx);
        return;
    }
    dict_add(dict, (void *)(uintptr_t)key, val);
}

void uint32_mpd_dict_clear(dict_t *dict)
{
    dict_clear(dict);
}

void uint32_mpd_dict_release(dict_t *dict)
{
    dict_release(dict);
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

uint32_t ptr_dict_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

int ptr_dict_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

void *mpd_dict_val_dup(const void *val)
{
    return mpd_qncopy(val);
}

void mpd_dict_val_free(void *val)
{
    mpd_del(val);
}


