/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/27, create
 */

# ifndef _AW_COMMON_H_
# define _AW_COMMON_H_

# include "aw_config.h"
# include <stdbool.h>

uint32_t dict_ses_hash_func(const void *key);
int      dict_ses_compare(const void *key1, const void *key2);

uint32_t dict_str_hash_func(const void *key);
int      dict_str_compare(const void *value1, const void *value2);
void*    dict_str_dup(const void *value);
void     dict_str_free(void *value);

void*    list_str_dup(void *value);

dict_t*  create_ses_dict(uint32_t init_size);
list_t*  create_str_list(void);


bool is_good_limit(int limit);
bool is_good_interval(const char *interval);
bool is_good_market(const char *market);

bool is_empty_string(const char *str);


# endif

