
#include "KlineRedisWriter.h"
#include "Utils.h"

char* KlineRedisWriter::to_str(kline_info_t *info) {
    json_t *obj = json_array();
    json_array_append_new_mpd(obj, info->open);
    json_array_append_new_mpd(obj, info->close);
    json_array_append_new_mpd(obj, info->high);
    json_array_append_new_mpd(obj, info->low);
    json_array_append_new_mpd(obj, info->volume ? info->volume : mpd_zero);
    json_array_append_new_mpd(obj, info->deal ? info->deal : mpd_zero);
    char *str = json_dumps(obj, 0);
    json_decref(obj);
    return str;
}

const string generate_redis_key(kline_info_t *kline_info) {
    switch(kline_info->type) {
       case  INTERVAL_MIN:
           return Utils::format_string("k:%s:1m",kline_info->market);
       case INTERVAL_HOUR:
           return Utils::format_string("k:%s:1h",kline_info->market);
       case INTERVAL_DAY:
            return Utils::format_string("k:%s:1d",kline_info->market);
    }
    assert(false);
}


int KlineRedisWriter::i_save(kline_info_t *data, const char *str,uint16_t *) {
    string key = generate_redis_key(data);
    if (redisAppendCommand(redis_ctx_, "HSET %s %ld %s", key.c_str(), data->timestamp, str)!=REDIS_OK)
        return -1;
    return 0;
}
