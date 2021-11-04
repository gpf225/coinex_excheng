#ifndef TRANSFER_H_INCLUDED
#define TRANSFER_H_INCLUDED

#include "Context.h"
#include "TimeCounter.h"
#include "Statistic.h"

class Transfer {
protected:
    Transfer(Context &ctx,const string &name):ctx_(ctx),name_(name) {
        stat_ = make_shared<Statistic>();
    }
public:
    int run();
protected:
    virtual int init() = 0;
    virtual int handle() = 0;
    virtual int save() = 0;
    virtual void report();

    Context &ctx_;
    string name_;

    shared_ptr<Statistic> stat_;
};

class SliceTransfer : public Transfer {
public:
    SliceTransfer(Context &ctx,const string &name):Transfer(ctx,name) {
    }
    const uint8_t assign_db(uint32_t user_id) const {
        return (user_id % (DB_HISTORY_COUNT * HISTORY_HASH_NUM)) / HISTORY_HASH_NUM;
    }
    const uint16_t assign_slice(uint32_t user_id) {
        return user_id % HISTORY_HASH_NUM;
    }
};

#endif // TRANSFER_H_INCLUDED
