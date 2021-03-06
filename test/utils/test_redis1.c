/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <jansson.h>
# include "ut_config.h"
# include "ut_dict.h"
# include "ut_misc.h"
# include "ut_redis.h"

static redisContext *get_redis_connection(redis_cfg *redis)
{
    return redis_connect(redis);
}

int main(void)
{
    redis_cfg redis;
    memset(&redis, 0, sizeof(redis_cfg));
    redis.host = "127.0.0.1";
    redis.port = 6379;
    redisContext *context = get_redis_connection(&redis);
    if (context == NULL) {
        printf("redis error\n");
        return 0;
    }

    double start = current_timestamp();
    for (int i = 0; i < 10000; i++) {
        redisReply *reply = redisCmd(context, "HSET test1 %d %d", i, i);
        if (reply == NULL) {
            printf("set error\n");
            break;
        }
        freeReplyObject(reply);
    }
    double end = current_timestamp();
    printf("cost: %f\n", end - start);
    return 0;
}

