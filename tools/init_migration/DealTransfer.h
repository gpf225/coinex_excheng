#ifndef DEALTRANSFER_H_INCLUDED
#define DEALTRANSFER_H_INCLUDED

#include "Transfer.h"
#include "Deal.h"
#include "DealWriter.h"
#include "DealRedisWriter.h"
#include "LastRedisWriter.h"

struct DealBookSlice {
    vector<shared_ptr<deal_t>> deals_;
};


class DealTransfer : public SliceTransfer {
public:
    DealTransfer(Context &ctx):SliceTransfer(ctx,"deal-transfer") {
    }

    const uint64_t get_deal_id_start() const {
        return deal_id_start_;
    }
private:
    int init();
    int handle();
    int save();
    int save_to_redis();

    int convert(src_ns::Deal &src_deal,vector<deal_t*> &deals);
    void assign_slice(deal_t *deal);

    void assign_market(deal_t *deal);
private:
    uint64_t deal_id_start_ = 0;
    DealBookSlice deal_slices[DB_HISTORY_COUNT][HISTORY_HASH_NUM];

    shared_ptr<DealWriter> writer_;
    shared_ptr<DealRedisWriter> redis_writer_;
    shared_ptr<LastRedisWriter> last_writer_;

    map<string,vector<deal_t*>> market_deals_;
};

#endif // DEALTRANSFER_H_INCLUDED
