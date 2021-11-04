#ifndef KLINEREDISWRITER_H_INCLUDED
#define KLINEREDISWRITER_H_INCLUDED

#include "RedisWriter.h"
#include "Kline.h"

class KlineRedisWriter : public RedisWriter<kline_info_t> {
public:
    KlineRedisWriter(Context &ctx):RedisWriter(ctx) {
    }

private:
    char *to_str(kline_info_t *data);
    int i_save(kline_info_t *data, const char *str,uint16_t *num);
};

#endif // KLINEREDISWRITER_H_INCLUDED
