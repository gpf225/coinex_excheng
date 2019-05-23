/*
 * Description: 
 *     History: yang@haipo.me, 2016/03/27, create
 *              zhoumugui@viabtc.com 2019/03/13, add function connect to redis directly.
 */

# ifndef _UT_REDIS_H_
# define _UT_REDIS_H_

# include <stddef.h>
# include <stdint.h>
# include <hiredis/hiredis.h>

typedef struct redis_cfg {
    char *host;
    char *password;
    int   port;
    uint32_t timeout;
    int db;
} redis_cfg;

redisContext *redis_connect(redis_cfg *cfg);
void *redisCmd(redisContext *c, const char *format, ...) __attribute__ ((format(printf, 2, 3)));
void *redisRawCmd(redisContext *c, const char *cmd);

# endif

