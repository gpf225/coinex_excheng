#ifndef DEALREDISWRITER_H_INCLUDED
#define DEALREDISWRITER_H_INCLUDED

#include "RedisWriter.h"
#include "Deal.h"

class DealRedisWriter : public RedisWriter<deal_t> {
public:
    DealRedisWriter(Context &ctx):RedisWriter(ctx) {
    }
private:
    char *to_str(deal_t *data);
    int i_save(deal_t *data, const char *str,uint16_t *num);
};


#endif // DEALREDISWRITER_H_INCLUDED
