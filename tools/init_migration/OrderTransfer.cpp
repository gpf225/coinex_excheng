
#include "OrderTransfer.h"
#include <string>

#include "Utils.h"
#include "Order.h"
#include "FieldSelector.h"
#include "OrderConverter.h"
#include "Config.h"
#include "Progress.h"

OrderTransfer::OrderTransfer(Context &ctx):SliceTransfer(ctx,"order-transfer") {
    order_writer_ = make_shared<OrderWriter>(ctx);
    stop_writer_ = make_shared<StopWriter>(ctx);

    stat_ = make_shared<OrderStatistic>();
}

OrderTransfer::~OrderTransfer() {
}

int OrderTransfer::init() {
    OrderConverter::ctx_ = &ctx_;
    return 0;
}

int OrderTransfer::handle_slice(uint8_t slice_id) {
    string sql = Utils::format_string("select * from core_order_%d",slice_id);
    int ret = mysql_real_query(ctx_.src_db_conn_, sql.c_str(), sql.length());
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(ctx_.src_db_conn_), mysql_error(ctx_.src_db_conn_));
        return -1;
    }

    MYSQL_RES *result = mysql_store_result(ctx_.src_db_conn_);
    uint64_t row_count = mysql_num_rows(result);
    stat_->total_ += row_count;
    FieldSelector selector(result);

    using Order=src_ns::Order;
    vector<shared_ptr<Order>> orders;

    Progress progress(row_count);
    uint64_t cnt = 0;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)))  {
       progress.step_it(++cnt);

       shared_ptr<Order> order(new Order());
       order->appl_id = strtoul(row[selector["appl_id"]],nullptr,0);
       order->timestamp = strtoull(row[selector["timestamp"]],nullptr,0);
       order->user_id = strtoul(row[selector["user_id"]],nullptr,0);
       order->contract_id = strtoul(row[selector["contract_id"]],nullptr,0);
       order->uuid = row[selector["uuid"]];
       order->side = strtol(row[selector["side"]],nullptr,0);
       order->price = row[selector["price"]];
       order->quantity = row[selector["quantity"]];
       order->order_type = strtoul(row[selector["order_type"]],nullptr,0);
       order->stop_price = row[selector["stop_price"]];
       order->order_status = strtol(row[selector["order_status"]],nullptr,0);
       order->maker_fee_ratio = row[selector["maker_fee_ratio"]];
       order->taker_fee_ratio = row[selector["taker_fee_ratio"]];
       order->client_order_id = row[selector["client_order_id"]];
       order->filled_currency = row[selector["filled_currency"]];
       order->filled_quantity = row[selector["filled_quantity"]];
       order->match_time = strtoull(row[selector["match_time"]],nullptr,0);
       order->order_sub_type = strtol(row[selector["order_sub_type"]],nullptr,0);

        orders.push_back(order);
    }

    for (auto order : orders) {
        int ret = convert(*order.get());
        if (ret) {
            stat_->fail_++;
            log_error("convert order fail,uuid:%s",order->uuid.c_str());
        }
        else {
            stat_->success_++;
        }
        switch(OrderConverter::type_map(*order.get())) {
        case MARKET_ORDER_TYPE_ORDER: ((OrderStatistic*)stat_.get())->order_num_++; break;
        case MARKET_ORDER_TYPE_STOP: ((OrderStatistic*)stat_.get())->stop_num_++; break;
        default:
            stat_->error_num_++;break;
        }
    }

    return 0;
}


void OrderTransfer::assign_slice(order_t *order) {
    uint8_t db_id = assign_db(order->user_id);
    uint16_t slice_id = SliceTransfer::assign_slice(order->user_id);
    order_slices[db_id][slice_id].orders_.push_back(shared_ptr<order_t>(order));
}


void OrderTransfer::assign_slice(stop_t *order) {
    uint8_t db_id = assign_db(order->user_id);
    uint16_t slice_id = SliceTransfer::assign_slice(order->user_id);
    order_slices[db_id][slice_id].stops_.push_back(shared_ptr<stop_t>(order));
}

int OrderTransfer::convert(src_ns::Order &src_order) {
    switch(OrderConverter::type_map(src_order)) {
    case MARKET_ORDER_TYPE_ORDER:{
        order_t *order = OrderConverter::convert_to_order(src_order);
        if (order==nullptr) {
            return -1;
        }
        order->id = ++this->order_id_start_;
        assign_slice(order);
        orders_[src_order.uuid] = make_shared<TypeOrder>(1,order);
    }
        break;
    case MARKET_ORDER_TYPE_STOP: {
        stop_t *stop = OrderConverter::convert_to_stop(src_order);
        if (stop==nullptr) {
            return -1;
        }
        stop->id = ++this->order_id_start_;
        assign_slice(stop);

        order_t *order = OrderConverter::convert_to_order(src_order);
        if (order==nullptr) {
            return -1;
        }
        order->id = ++this->order_id_start_;
        stop->real_order_id = order->id;
        assign_slice(order);
        orders_[src_order.uuid] = make_shared<TypeOrder>(1,order);
    }
        break;
    }
    return 0;
}


int OrderTransfer::save() {
    for (uint8_t db_id=0;db_id<DB_HISTORY_COUNT;db_id++) {
        for (uint16_t slice_id = 0;slice_id<HISTORY_HASH_NUM;slice_id++) {
            OrderBookSlice &obs = order_slices[db_id][slice_id];
            if (!obs.orders_.empty()) {
                CLargeStringArray data;
                for (auto order : obs.orders_) {
                    string value = order_writer_->generate_value_list(*order.get());
                    data.Add(Utils::dup_string(value));
                }
                order_writer_->batch_insert(db_id,slice_id,data,ctx_.settings_.row_limit);
            }

            if (!obs.stops_.empty()) {
                CLargeStringArray data;
                for (auto stop: obs.stops_) {
                    string value = stop_writer_->generate_value_list(*stop.get());
                    data.Add(Utils::dup_string(value));
                }
                stop_writer_->batch_insert(db_id,slice_id,data,ctx_.settings_.row_limit);
            }
        }
    }

    return 0;
}

void OrderTransfer::report() {
    Transfer::report();
    ctx_.reporter_->add(Utils::format_string("- normal number:%d",((OrderStatistic*)stat_.get())->order_num_));
    ctx_.reporter_->add(Utils::format_string("- stop number:%d",((OrderStatistic*)stat_.get())->stop_num_));
}

int OrderTransfer::handle() {
    for (uint8_t i=0;i<SRC_SLICE_NUM;i++) {
        log_stderr("handle slice %d/%d...",i+1,SRC_SLICE_NUM);
        if (handle_slice(i))
            return -1;
    }
    int ret = save();
    return ret;
}

int OrderTransfer::run() {
    if (Transfer::run())
        return -1;

    ctx_.orders_ = &orders_;
    return 0;
}

