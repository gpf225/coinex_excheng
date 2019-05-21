/*
 * Description: 
 *     History: yang@haipo.me, 2016/04/09, create
 */

# ifndef _UT_PACK_
# define _UT_PACK_

# include <stdint.h>
# include <stddef.h>

# include "ut_sds.h"

int pack_uint16_le(void **dest, size_t *left, uint16_t num);
int unpack_uint16_le(void **src, size_t *left, uint16_t *num);

int pack_uint32_le(void **dest, size_t *left, uint32_t num);
int unpack_uint32_le(void **src, size_t *left, uint32_t *num);

int pack_uint64_le(void **dest, size_t *left, uint64_t num);
int unpack_uint64_le(void **src, size_t *left, uint64_t *num);

# endif

