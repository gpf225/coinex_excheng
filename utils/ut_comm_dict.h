/*
 * Description: 
 *     History: yang@haipo.me, 2018/01/01, create
 */

# ifndef UT_COMM_DICT_H
# define UT_COMM_DICT_H

# include <stddef.h>
# include <stdint.h>
# include <stdbool.h>

# include "ut_sds.h"
# include "ut_dict.h"

uint32_t uint32_dict_hash_func(const void *key);
int uint32_dict_key_compare(const void *key1, const void *key2);

dict_t *uint32_set_create(void);
void uint32_set_add(dict_t *set, uint32_t value);
bool uint32_set_exist(dict_t *set, uint32_t value);
size_t uint32_set_num(dict_t *set);
void uint32_set_release(dict_t *set);

uint32_t str_dict_hash_function(const void *key);
void *str_dict_key_dup(const void *key);
int str_dict_key_compare(const void *key1, const void *key2);
void str_dict_key_free(void *key);

uint32_t sds_dict_hash_function(const void *key);
void *sds_dict_key_dup(const void *key);
int sds_dict_key_compare(const void *key1, const void *key2);
void sds_dict_key_free(void *key);

# endif

