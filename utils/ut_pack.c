/*
 * Description: 
 *     History: yang@haipo.me, 2016/04/09, create
 */

# include <string.h>
# include <stdlib.h>
# include <stdbool.h>

# include "ut_misc.h"
# include "ut_pack.h"

int pack_uint16_le(void **dest, size_t *left, uint16_t num)
{
    if (*left < 4)
        return -1;
    *(uint16_t *)(*dest) = htole16(num);
    *dest += 2;
    *left -= 2;
    return 2;
}

int unpack_uint16_le(void **src, size_t *left, uint16_t *num)
{
    if (*left < 4)
        return -1;
    *num = le16toh(*(uint16_t *)(*src));
    *src  += 2;
    *left -= 2;
    return 2;
}

int pack_uint32_le(void **dest, size_t *left, uint32_t num)
{
    if (*left < 4)
        return -1;
    *(uint32_t *)(*dest) = htole32(num);
    *dest += 4;
    *left -= 4;
    return 4;
}

int unpack_uint32_le(void **src, size_t *left, uint32_t *num)
{
    if (*left < 4)
        return -1;
    *num = le32toh(*(uint32_t *)(*src));
    *src  += 4;
    *left -= 4;
    return 4;
}

int pack_uint64_le(void **dest, size_t *left, uint64_t num)
{
    if (*left < 8)
        return -1;
    *(uint64_t *)(*dest) = htole64(num);
    *dest += 8;
    *left -= 8;
    return 8;
}

int unpack_uint64_le(void **src, size_t *left, uint64_t *num)
{
    if (*left < 8)
        return -1;
    *num = le64toh(*(uint64_t *)(*src));
    *src  += 8;
    *left -= 8;
    return 8;
}

int unpack_pass(void **src, size_t *left, size_t size)
{
    if (*left < size)
        return -1;
    *src += size;
    *left -= size;
    return size;
}

