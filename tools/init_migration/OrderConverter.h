#ifndef ORDERCONVERTER_H_INCLUDED
#define ORDERCONVERTER_H_INCLUDED

#include "Order.h"
#include "Context.h"

class OrderConverter {
public:
    static const Context *ctx_;
    static order_t* convert_to_order(src_ns::Order &order);
    static stop_t* convert_to_stop(src_ns::Order &order);

    static int side_map(src_ns::Order &order);
    static int type_map(src_ns::Order &order);
    static int order_type_map(src_ns::Order &order);

    static int order_deal_flag_map(src_ns::Order &order);

    static int order_status_map(src_ns::Order &order);

    static int stop_status_map(src_ns::Order &order);
};

#endif // ORDERCONVERTER_H_INCLUDED
