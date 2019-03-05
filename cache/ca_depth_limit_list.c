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

depth_limit_list_t *depth_limit_list_create(void)
{
    depth_limit_list_t *list = malloc(sizeof(depth_limit_list_t));
    if (list == NULL) {
        return NULL;
    }

    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = uint32_hash_function;
    type.key_compare = uint32_key_compare;

    list->dict_limits = dict_create(&type, 4);
    if (list->dict_limits == NULL) {
        free(list);
        return NULL;
    }

    return list;
} 

void depth_limit_list_free(depth_limit_list_t *list)
{
    dict_release(list->dict_limits);
    free(list);
}

int depth_limit_list_add(depth_limit_list_t *list, uint32_t key, uint32_t value)
{
    if (dict_add(list->dict_limits, (void *)(uintptr_t)key, (void *)(uintptr_t)value) == NULL) {
        return -1;
    }
    return 0;
}

int depth_limit_list_reset(depth_limit_list_t *list, uint32_t key, uint32_t value)
{
    if (dict_replace(list->dict_limits, (void *)(uintptr_t)key, (void *)(uintptr_t)value) < 0) {
        return -1;
    }
    return 0;    
}

uint32_t depth_limit_list_get(depth_limit_list_t *list, uint32_t key)
{
    dict_entry *entry = dict_find(list->dict_limits, (void *)(uintptr_t)key);
    if (entry) {
        return (uint32_t)(uintptr_t)entry->val;
    }
    return 0;
}

uint32_t depth_limit_list_remove(depth_limit_list_t *list, uint32_t key)
{
    uint32_t limit = 0;
    dict_entry *entry = dict_find(list->dict_limits, (void *)(uintptr_t)key);
    if (entry) {
        limit = (uintptr_t)entry->val;
        dict_delete(list->dict_limits, (void *)(uintptr_t)key);
    }  
    return limit;
}

uint32_t depth_limit_list_inc(depth_limit_list_t *list, uint32_t key)
{
    dict_entry *entry = dict_find(list->dict_limits, (void *)(uintptr_t)key);
    if (entry != NULL) {
        uint32_t val = (uint32_t)(uintptr_t)entry->val;
        val += 1;
        entry->val = (void *)(uintptr_t)val;
        return val;
    }
    if (depth_limit_list_add(list, key, 1u) == -1) {
        return -1;
    }
    return 1;
}

uint32_t depth_limit_list_dec(depth_limit_list_t *list, uint32_t key)
{
    dict_entry *entry = dict_find(list->dict_limits, (void *)(uintptr_t)key);
    if (entry != NULL) {
        uint32_t val = (uint32_t)(uintptr_t)entry->val;
        val -= 1;
        entry->val = (void *)(uintptr_t)val;
        return val;
    }
    return 0;
}

bool depth_limit_list_exist(depth_limit_list_t *list, uint32_t key)
{
    dict_entry *entry = dict_find(list->dict_limits, (void *)(uintptr_t)key);
    if (entry) {
        return true;
    }
    return false;
}

uint32_t depth_limit_list_max(depth_limit_list_t *list)
{
    uint32_t max = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(list->dict_limits);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t limit = (uint32_t)(uintptr_t)entry->val;
        if (limit > max) {
            max = limit;
        }
    }
    
    return max;
}

uint32_t depth_limit_list_retrieve(depth_limit_list_t *list, uint32_t* limits)
{
    uint32_t count = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(list->dict_limits);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t limit = (uint32_t)(uintptr_t)entry->val;
        limits[count] = limit;
        ++count;
    }
    return count;
}







