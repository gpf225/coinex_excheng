/*
 * Description: misc functions
 *     History: yang@haipo.me, 2016/03/15, create
 */

# ifndef _MISC_H_
# define _MISC_H_

# include <endian.h>
# include <byteswap.h>
# include <stdbool.h>
# include <jansson.h>

# include "ut_sds.h"
# include "ut_log.h"

int process_exist(const char *fmt, ...);
int process_keepalive(bool debuf);

int set_core_limit(size_t limit);
int set_file_limit(size_t limit);

sds hexdump(const void *mem, size_t len);
sds bin2hex(const void *mem, size_t len);
sds hex2bin(const char *hex);

sds zlib_uncompress(const void *mem, size_t len);
sds zlib_compress(const void *mem, size_t len);

double current_timestamp(void);
uint64_t current_millisecond(void);
char *strftimestamp(time_t t);
char *timeval_str(struct timeval *tv);

int urandom(void *buf, size_t size);
char *human_number(double num);

void reverse_mem(char *mem, size_t len);
void strtolower(char *str);
void strtoupper(char *str);
void strclearblank(char *str);
char *sstrncpy(char *dest, const char *src, size_t n);

time_t get_day_start(time_t timestamp);
time_t get_month_start(time_t timestamp);
time_t get_year_start(time_t timestamp);

time_t get_utc_day_start(time_t timestamp);
time_t get_utc_month_start(time_t timestamp);
time_t get_utc_yearstart(time_t timestamp);

# define log_trace_hex(title, msg, msg_len) do { \
    if (default_dlog_flag & DLOG_TRACE) { \
        sds hex = bin2hex(msg, msg_len); \
        loga("[trace]%s:%i(%s): %s: %s", __FILE__, __LINE__, __func__, title, hex); \
        sdsfree(hex); \
    } \
} while (0)

# define log_trace_hexdump(title, msg, msg_len) do { \
    if (default_dlog_flag & DLOG_TRACE) { \
        sds hex = hexdump(msg, msg_len); \
        loga("[trace]%s:%i(%s): %s\n: %s", __FILE__, __LINE__, __func__, title, hex); \
        sdsfree(hex); \
    } \
} while (0)

# undef ERR_RET
# define ERR_RET(x) do { \
    int __ret = (x); \
    if (__ret < 0) { \
        return __ret; \
    } \
} while (0)

# undef ERR_RET_LN
# define ERR_RET_LN(x) do { \
    if ((x) < 0) { \
        return -__LINE__; \
    } \
} while (0)

# undef htobe16
# undef htobe32
# undef htobe64
# undef be16toh
# undef be32toh
# undef be64toh
# undef htole16
# undef htole32
# undef htole64
# undef le16toh
# undef le32toh
# undef le64toh

# if __BYTE_ORDER == __LITTLE_ENDIAN
#  define htole16(x) (x)
#  define htole32(x) (x)
#  define htole64(x) (x)
#  define le16toh(x) (x)
#  define le32toh(x) (x)
#  define le64toh(x) (x)
#  define htobe16(x) bswap_16(x)
#  define htobe32(x) bswap_32(x)
#  define htobe64(x) bswap_64(x)
#  define be16toh(x) bswap_16(x)
#  define be32toh(x) bswap_32(x)
#  define be64toh(x) bswap_64(x)
# else
#  define htole16(x) bswap_16(x)
#  define htole32(x) bswap_32(x)
#  define htole64(x) bswap_64(x)
#  define le16toh(x) bswap_16(x)
#  define le32toh(x) bswap_32(x)
#  define le64toh(x) bswap_64(x)
#  define htobe16(x) (x)
#  define htobe32(x) (x)
#  define htobe64(x) (x)
#  define be16toh(x) (x)
#  define be32toh(x) (x)
#  define be64toh(x) (x)
# endif

# endif


#ifdef __APPLE__
#include <machine/endian.h>
#include <libkern/OSByteOrder.h>

#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

#define __BIG_ENDIAN BIG_ENDIAN
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __BYTE_ORDER BYTE_ORDER
#else
#include
#include
#endif

#if defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) || defined(__APPLE__)
#define clearenv() 0
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
#define appname getprogname()
#elif defined(_GNU_SOURCE)
const char *appname = program_invocation_name;
#else
const char *appname = argv[0];
#endif

