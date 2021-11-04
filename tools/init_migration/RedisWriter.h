#ifndef REDISWRITER_H_INCLUDED
#define REDISWRITER_H_INCLUDED

#include "Context.h"
extern "C" {
#include "ut_redis.h"
}

template<class T> class RedisWriter {
public:
    RedisWriter(Context &ctx):ctx_(ctx) {
        redis_ctx_ = ctx_.get_redis_connection();
    }
    virtual char *to_str(T *data) = 0;

    int save(T *data) {
        char *str = to_str(data);
        if (str == NULL) {
            return -1;
        }

        uint16_t num = 1;
        i_save(data,str,&num);

        free(str);

        pipeline_count += num;
        if (pipeline_count >= settings.pipeline_len_max) {
            int ret = pipeline_excute();
            if (ret < 0) {
                return ret;
            }
        }
        return 0;
    }


private:
    virtual int i_save(T *data, const char *str,uint16_t *num) = 0;
    int pipeline_excute() {
        log_trace("pipeline_count: %d", pipeline_count);
        if (pipeline_count == 0)
            return 0;

        int count = 0;
        redisReply *reply = NULL;
        while(redisGetReply(redis_ctx_,(void **)&reply) == REDIS_OK) {
            freeReplyObject(reply);
            count++;
            if (count >= pipeline_count)
                break;
        }

        if (count < pipeline_count) {
            return -1;
        }
        pipeline_count = 0;
        return 0;
    }

protected:
    Context &ctx_;
    redisContext *redis_ctx_;

private:
    int pipeline_count = 0;
};

#endif // REDISWRITER_H_INCLUDED
