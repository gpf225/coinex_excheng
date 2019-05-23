/*
 * Description: 
 *     History: yang@haipo.me, 2016/03/27, create
 */

# include <stdlib.h>
# include <string.h>
# include <stdarg.h>
# include <errno.h>

# include "ut_log.h"
# include "ut_redis.h"

redisContext *redis_connect(redis_cfg *cfg)
{
    struct timeval timeout;
    timeout.tv_sec = cfg->timeout / 1000;
    timeout.tv_usec = (cfg->timeout % 1000) * 1000;

    redisContext *redis = redisConnectWithTimeout(cfg->host, cfg->port, timeout);
    if (redis == NULL || redis->err) {
        log_error("connect to redis:%s:%d failed", cfg->host, cfg->port);
        if (redis != NULL) {
            redisFree(redis);
        }
        return NULL;
    }
    if (cfg->password != NULL && strlen(cfg->password) != 0) {
        redisReply *reply = redisCommand(redis, "auth %s", cfg->password);
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            log_error("redis auth failed, please checkout your password.");
            if (reply != NULL) {
                freeReplyObject(reply);
            }
            redisFree(redis);
            return NULL;
        }
        freeReplyObject(reply);
    }

    if (cfg->db > 0) {
        redisReply *reply = redisCommand(redis, "SELECT %d", cfg->db);
        if (redis == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply != NULL) {
                freeReplyObject(reply);
            }
            redisFree(redis);
            return NULL;
        }
        freeReplyObject(reply);
    }

    return redis;
}

void *redisCmd(redisContext *c, const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    redisReply *reply = redisvCommand(c, format, ap);
    va_end(ap);

    if (reply == NULL) {
        log_error("redisCommand fail: %d: %s", c->err, strerror(errno));
        return NULL;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        log_error("redisCommand error: %s", reply->str);
        freeReplyObject(reply);
        return NULL;
    }
    return reply;
}

void *redisRawCmd(redisContext *c, const char *cmd)
{
    redisReply *reply = redisCommand(c, cmd);
    if (reply == NULL) {
        log_error("redisCommand: %s fail: %d: %s", cmd, c->err, strerror(errno));
        return NULL;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        log_error("redisCommand: %s error: %s", cmd, reply->str);
        freeReplyObject(reply);
        return NULL;
    }
    return reply;
}

