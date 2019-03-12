/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# ifndef _LP_COMMON_H_
# define _LP_COMMON_H_

# include "lp_config.h"

uint32_t dict_ses_hash_func(const void *key);
int      dict_ses_compare(const void *key1, const void *key2);

uint32_t dict_str_hash_func(const void *key);
int      dict_str_compare(const void *value1, const void *value2);
void*    dict_str_dup(const void *value);
void     dict_str_free(void *value);

void*    list_str_dup(void *value);

dict_t*  create_ses_dict(uint32_t init_size);
list_t*  create_str_list(void);


# endif