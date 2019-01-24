/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# ifndef _AW_COMMON_STRUCT_H_
# define _AW_COMMON_STRUCT_H_

# include "aw_config.h"

uint32_t common_ses_hash_func(const void *key);
int      common_ses_compare(const void *key1, const void *key2);

uint32_t common_str_hash_func(const void *key);
int      common_str_compare(const void *value1, const void *value2);
void*    common_str_const_dup(const void *value);
void*    common_str_dup(void *value);
void     common_str_free(void *value);

dict_t*  common_create_ses_dict(uint32_t init_size);
list_t*  common_create_str_list(void);


# endif