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
# include "ut_decimal.h"

uint32_t uint32_dict_hash_func(const void *key);
int uint32_dict_key_compare(const void *key1, const void *key2);

dict_t *uint32_set_create(void);
void uint32_set_add(dict_t *set, uint32_t value);
bool uint32_set_exist(dict_t *set, uint32_t value);
size_t uint32_set_num(dict_t *set);
void uint32_set_clear(dict_t *set);
void uint32_set_release(dict_t *set);

dict_t *uint32_mpd_dict_create(void);
void uint32_mpd_dict_plus(dict_t *dict, uint32_t key, mpd_t *val);
void uint32_mpd_dict_clear(dict_t *dict);
void uint32_mpd_dict_release(dict_t *dict);

uint32_t str_dict_hash_function(const void *key);
void *str_dict_key_dup(const void *key);
int str_dict_key_compare(const void *key1, const void *key2);
void str_dict_key_free(void *key);

uint32_t sds_dict_hash_function(const void *key);
void *sds_dict_key_dup(const void *key);
int sds_dict_key_compare(const void *key1, const void *key2);
void sds_dict_key_free(void *key);

uint32_t ptr_dict_hash_func(const void *key);
int ptr_dict_key_compare(const void *key1, const void *key2);

void *mpd_dict_val_dup(const void *val);
void mpd_dict_val_free(void *val);

# endif

