
#ifndef ORDERTRANSFER_H
#define ORDERTRANSFER_H

#include <memory>
#include <vector>
#include "Context.h"
#include "Order.h"
#include "OrderWriter.h"
#include "Utils.h"
#include "Transfer.h"

using namespace std;

struct OrderBookSlice {
    vector<shared_ptr<order_t>> orders_;
    vector<shared_ptr<stop_t>> stops_;
};

struct OrderStatistic : public Statistic {
    uint32_t order_num_ = 0;
    uint32_t stop_num_ = 0;
};
class OrderTransfer : public SliceTransfer {
public:
    OrderTransfer(Context &ctx);
    virtual ~OrderTransfer();

    int run();

    const uint64_t get_order_id_start() const {
        return order_id_start_;
    }
protected:

private:
    int init();
    int handle();
    int handle_slice(uint8_t slice_id);

    int convert(src_ns::Order &order);
    int save();

    void assign_slice(order_t *order);
    void assign_slice(stop_t *order);

    void report();

    uint64_t order_id_start_ = 0;

    OrderBookSlice order_slices[DB_HISTORY_COUNT][HISTORY_HASH_NUM];
    map<string,shared_ptr<TypeOrder>> orders_; /// all generated new order,include order_t and stop_t

    shared_ptr<OrderWriter> order_writer_;
    shared_ptr<StopWriter> stop_writer_;
};

#endif // ORDERTRANSFER_H
