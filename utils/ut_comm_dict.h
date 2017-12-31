/*
 * Description: 
 *     History: yang@haipo.me, 2018/01/01, create
 */

# ifndef UT_COMM_DICT_H
# define UT_COMM_DICT_H

# include <stddef.h>
# include <stdint.h>
# include <stdbool.h>
# include "ut_dict.h"

dict_t *uint32_set_create(void);
void uint32_set_add(dict_t *set, uint32_t value);
bool uint32_set_exist(dict_t *set, uint32_t value);
size_t uint32_set_num(dict_t *set);
void uint32_set_release(dict_t *set);

# endif

