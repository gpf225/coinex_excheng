/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# include "lp_common_struct.h"

uint32_t common_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

int common_ses_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

dict_t* common_create_ses_dict(uint32_t init_size)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = common_ses_hash_func;
    dt.key_compare = common_ses_compare;

    return dict_create(&dt, init_size);
}

uint32_t common_str_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

int common_str_compare(const void *value1, const void *value2)
{
    return strcmp(value1, value2);
}

void *common_str_const_dup(const void *value)
{
    return strdup(value);
}

void *common_str_dup(void *value)
{
    return strdup(value);
}

void common_str_free(void *value)
{
    free(value);
}

list_t* common_create_str_list(void)
{
    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.dup = common_str_dup;
    lt.free = common_str_free;
    lt.compare = common_str_compare;
    return list_create(&lt);
}
