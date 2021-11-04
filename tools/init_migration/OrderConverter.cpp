
#include "OrderConverter.h"
#include "Exception.h"


const Context *OrderConverter::ctx_ = nullptr;

int OrderConverter::side_map(src_ns::Order &order) {
    switch(order.side) {
        case 1 : return MARKET_ORDER_SIDE_BID;
        case -1 : return MARKET_ORDER_SIDE_ASK;
    }
    throw ValidDataException("unexpected side code");
}

int OrderConverter::type_map(src_ns::Order &order) {
    return stod(order.stop_price)==0 ? MARKET_ORDER_TYPE_ORDER : MARKET_ORDER_TYPE_STOP;
}

int OrderConverter::order_type_map(src_ns::Order &order) {
    switch(order.order_type) {
        case 1: return MARKET_ORDER_TYPE_LIMIT;
        case 3: return MARKET_ORDER_TYPE_MARKET;
    }
    throw ValidDataException("unexpected order type");
}

int OrderConverter::order_deal_flag_map(src_ns::Order &order) {
    switch(order.order_status) {
        case 4: return 2;
        case 3:
        case 5:
            return 1;
        default:
            return 0;
    }
}

int OrderConverter::order_status_map(src_ns::Order &order) {
    switch(order.order_status) {
        case 8: return 2;
        case 5:
        case 6:
            return 3;
        default:
            return 1;
    }
}

int OrderConverter::stop_status_map(src_ns::Order &order) {
    return order_status_map(order);
}


order_t* OrderConverter::convert_to_order(src_ns::Order &src_order) {
    order_t *order = new order_t();

    order->user_id = src_order.user_id;
    order->account = src_order.appl_id;

    order->type         = order_type_map(src_order);
    order->side         = side_map(src_order);
    order->create_time  = src_order.timestamp/1000000;
    order->update_time  = (src_order.match_time==0 ? src_order.timestamp : src_order.match_time)/1000000;

    const Market *market = ctx_->get_market(src_order.contract_id);
    if (market==nullptr)
        return nullptr;
    order->market       = strdup(market->name_.c_str());
    order->price        = decimal(src_order.price.c_str(),0);
    order->amount       = decimal(src_order.quantity.c_str(),0);

    order->taker_fee    = decimal(src_order.taker_fee_ratio.c_str(),0);
    order->maker_fee    = decimal(src_order.maker_fee_ratio.c_str(),0);


    order->deal_stock   = decimal(src_order.filled_quantity.c_str(),0);
    order->deal_money   = decimal(src_order.filled_currency.c_str(),0);
    order->client_id = strdup(src_order.client_order_id.c_str());
    order->last_deal_time = src_order.match_time/1000000;

    order->deal_flag = order_deal_flag_map(src_order);
    order->status = order_status_map(src_order);

    return order;
}

stop_t* OrderConverter::convert_to_stop(src_ns::Order &src_order) {
    stop_t* stop = new stop_t();

    stop->user_id = src_order.user_id;
    stop->account = src_order.appl_id;
    stop->type         = order_type_map(src_order);
    stop->side         = side_map(src_order);
    stop->create_time  = src_order.timestamp/1000000;
    stop->update_time  = (src_order.match_time==0 ? src_order.timestamp : src_order.match_time)/1000000;

    const Market *market = ctx_->get_market(src_order.contract_id);
    if (market==nullptr)
        return nullptr;
    stop->market       = strdup(market->name_.c_str());
    stop->stop_price   = decimal(src_order.stop_price.c_str(),0);
    stop->price        = decimal(src_order.price.c_str(),0);
    stop->amount       = decimal(src_order.quantity.c_str(),0);

    stop->taker_fee    = decimal(src_order.taker_fee_ratio.c_str(),0);
    stop->maker_fee    = decimal(src_order.maker_fee_ratio.c_str(),0);

    stop->state = stop_status_map(src_order);

    return stop;
}
