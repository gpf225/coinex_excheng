#ifndef ORDERWRITER_H_INCLUDED
#define ORDERWRITER_H_INCLUDED

#include "Writer.h"

class OrderWriter : public Writer {
    static const uint8_t FIELD_NUM = 25;
    const FieldTypeFormat _fields_[FIELD_NUM] = {
        {"account","%d"},
        {"create_time","%f"},
        {"user_id","%llu"},
        {"market","'%s'"},
        {"order_id","%llu"},
        {"side","%d"},
        {"price","%s"},
        {"amount","%s"},
        {"t","%d"},
        {"maker_fee","%s"},
        {"taker_fee","%s"},
        {"client_id","'%s'"},
        {"deal_money","%s"},
        {"deal_stock","%s"},
        {"finish_time","%f"},
        {"deal_flag","%d"},
        {"status","%d"},
        {"option","%d"},
        {"source","'%s'"},
        {"fee_asset","'%s'"},
        {"fee_discount","%s"},
        {"money_fee","%s"},
        {"stock_fee","%s"},
        {"deal_fee","%s"},
        {"asset_fee","%s"}
    };

public:
    OrderWriter(const Context &ctx):Writer(ctx){
        fields_ = const_cast<FieldTypeFormat*>(&_fields_[0]);
        field_num_ = FIELD_NUM;
        field_list_ = generate_field_list();

        table_tag_ = "order_history";
    }
    const string generate_value_list(const order_t &order) {
        return Utils::format_string(generate_value_format().c_str(),
          order.account,
          order.create_time,
          order.user_id,
          order.market,
          order.id,
          order.side,
          order.price ? Utils::mpd_to_string(order.price).c_str() : "NULL",
          order.amount ? Utils::mpd_to_string(order.amount).c_str() : "NULL",
          order.type,
          order.maker_fee ? Utils::mpd_to_string(order.maker_fee).c_str() : "NULL",
          order.taker_fee ? Utils::mpd_to_string(order.taker_fee).c_str() : "NULL",
          order.client_id ? order.client_id : "",
          order.deal_money ? Utils::mpd_to_string(order.deal_money).c_str() : "NULL",
          order.deal_stock ? Utils::mpd_to_string(order.deal_stock).c_str() : "NULL",
          order.update_time,/// todo: may get finish time from deal record
          order.deal_flag,
          order.status,
          order.option,
          order.source ? order.source : "",
          order.fee_asset ? order.fee_asset : "",
          order.fee_discount ? Utils::mpd_to_string(order.fee_discount).c_str() : "1",
          order.money_fee ? Utils::mpd_to_string(order.money_fee).c_str() : "0",
          order.stock_fee ? Utils::mpd_to_string(order.stock_fee).c_str() : "0",
          "0",
          order.asset_fee ? Utils::mpd_to_string(order.asset_fee).c_str() : "0"
          );
    }

};

class StopWriter : public Writer {
    static const uint8_t FIELD_NUM = 20;
    const FieldTypeFormat _fields_[FIELD_NUM] = {
        {"account","%d"},
        {"create_time","%f"},
        {"user_id","%llu"},
        {"market","'%s'"},
        {"order_id","%llu"},
        {"side","%d"},
        {"price","%s"},
        {"amount","%s"},
        {"stop_price","%s"},
        {"t","%d"},
        {"status","%d"},
        {"maker_fee","%s"},
        {"taker_fee","%s"},
        {"client_id","'%s'"},
        {"fee_asset","'%s'"},
        {"fee_discount","%s"},
        {"finish_time","%f"},
        {"real_order_id","%llu"},
        {"option","%d"},
        {"source","'%s'"},
    };

public:
    StopWriter(const Context &ctx):Writer(ctx){
        fields_ = const_cast<FieldTypeFormat*>(&_fields_[0]);
        field_num_ = FIELD_NUM;
        field_list_ = generate_field_list();
        table_tag_ = "stop_history";
    }
    const string generate_value_list(const stop_t &stop) {
        return Utils::format_string(generate_value_format().c_str(),
          stop.account,
          stop.create_time,
          stop.user_id,
          stop.market,
          stop.id,
          stop.side,
          stop.price ? Utils::mpd_to_string(stop.price).c_str() : "NULL",
          stop.amount ? Utils::mpd_to_string(stop.amount).c_str() : "NULL",
          stop.stop_price ? Utils::mpd_to_string(stop.stop_price).c_str() : "NULL",
          stop.type,
          stop.state,
          stop.maker_fee ? Utils::mpd_to_string(stop.maker_fee).c_str() : "NULL",
          stop.taker_fee ? Utils::mpd_to_string(stop.taker_fee).c_str() : "NULL",
          stop.client_id ? stop.client_id : "",
          stop.fee_asset ? stop.fee_asset : "",
          stop.fee_discount ? Utils::mpd_to_string(stop.fee_discount).c_str() : "1",
          stop.update_time,
          stop.real_order_id,
          stop.option,
          stop.source ? stop.source : ""
          );
    }
};

#endif // ORDERWRITER_H_INCLUDED
