/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# include "ca_depth_limit_list.h"

static uint32_t uint32_hash_function(const void *key)
{
    return (uint32_t)(uintptr_t)key;
}

static int uint32_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

dict_t *dict_limit_list_create(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = uint32_hash_function;
    type.key_compare = uint32_key_compare;

    return dict_create(&type, 64);
}

int dict_limit_list_add(dict_t *dict, uint32_t key, uint32_t value)
{
    if (dict_add(dict, (void *)(uintptr_t)key, (void *)(uintptr_t)value) == NULL) {
        return -1;
    }
    return 0;
}

uint32_t dict_limit_list_inc(dict_t *dict, uint32_t key)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)key);
    if (entry != NULL) {
        uint32_t val = (uint32_t)(uintptr_t)entry->val;
        val += 1;
        entry->val = (void *)(uintptr_t)val;
        return val;
    }
    if (dict_limit_list_add(dict, key, 1) == -1) {
        return -1;
    }
    return 1;
}

uint32_t dict_limit_list_dec(dict_t *dict, uint32_t key)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)key);
    if (entry != NULL) {
        uint32_t val = (uint32_t)(uintptr_t)entry->val;
        val -= 1;
        entry->val = (void *)(uintptr_t)val;
        return val;
    }
    return 0;
}

uint32_t dict_limit_list_get(dict_t *dict, uint32_t key)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)key);
    if (entry) {
        return (uint32_t)(uintptr_t)entry->val;
    }
    return 0;
}

bool dict_limit_list_exist(dict_t *dict, uint32_t key)
{
    dict_entry *entry = dict_find(dict, (void *)(uintptr_t)key);
    if (entry)
        return true;
    return false;
}

uint32_t dict_limit_list_max(dict_t *dict)
{
    uint32_t max = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t val = (uint32_t)(uintptr_t)entry->val;
        if (val > max) {
            max = val;
        }
    }
    
    return max;
}
