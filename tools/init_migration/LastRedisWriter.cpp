
#include "LastRedisWriter.h"

char *LastRedisWriter::to_str(deal_t *data) {
    return mpd_format(data->price, "f", &mpd_ctx);
}

int LastRedisWriter::i_save(deal_t *data, const char *str,uint16_t *num) {
    return redisAppendCommand(redis_ctx_, "SET k:%s:last %s", data->market, str)!=REDIS_OK ? -1 : 0;
}
