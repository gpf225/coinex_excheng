#ifndef RUNNER_H
#define RUNNER_H

#include "Context.h"
#include "OrderTransfer.h"
#include "DealTransfer.h"
#include "AccountTransfer.h"
#include "KlineTransfer.h"

class Runner
{
public:
    Runner();
    virtual ~Runner();
    int init();
    int run();

protected:
private:
    shared_ptr<Context> ctx_;

    shared_ptr<OrderTransfer> order_transfer_;
    shared_ptr<DealTransfer> deal_transfer_;
    shared_ptr<AccountTransfer> account_transfer_;
    shared_ptr<KlineTransfer> kline_transfer_;

    void clear_data();
    void flush_redis();

    int init_slice_history();
    int new_slice_order_table();

    int commit();
};


#endif // RUNNER_H
