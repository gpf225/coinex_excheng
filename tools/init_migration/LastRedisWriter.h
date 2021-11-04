#ifndef LASTREDISWRITER_H_INCLUDED
#define LASTREDISWRITER_H_INCLUDED

#include "RedisWriter.h"
#include "Deal.h"

class LastRedisWriter : public RedisWriter<deal_t> {
public:
    LastRedisWriter(Context &ctx):RedisWriter(ctx) {
    }
private:
    char *to_str(deal_t *data);
    int i_save(deal_t *data, const char *str,uint16_t *num);
};

#endif // LASTREDISWRITER_H_INCLUDED
