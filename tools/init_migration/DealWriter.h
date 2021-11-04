#ifndef DEALWRITER_H_INCLUDED
#define DEALWRITER_H_INCLUDED

#include "Writer.h"

class DealWriter : public Writer {
    static const uint8_t FIELD_NUM = 18;
    const FieldTypeFormat _fields_[FIELD_NUM] = {
        {"time","%f"},
        {"account","%d"},
        {"user_id","%llu"},
        {"deal_user_id","%llu"},
        {"deal_account","%d"},
        {"market","'%s'"},
        {"deal_id","%llu"},
        {"order_id","%llu"},
        {"deal_order_id","%llu"},
        {"side","%d"},
        {"role","%d"},
        {"price","%s"},
        {"amount","%s"},
        {"deal","%s"},
        {"fee","%s"},
        {"deal_fee","%s"},
        {"fee_asset","'%s'"},
        {"deal_fee_asset","'%s'"}
    };

public:
    DealWriter(const Context &ctx):Writer(ctx){
        fields_ = const_cast<FieldTypeFormat*>(&_fields_[0]);
        field_num_ = FIELD_NUM;
        field_list_ = generate_field_list();
        table_tag_ = "user_deal_history";
    }

    const string generate_value_list(const deal_t &deal) {
        return Utils::format_string(generate_value_format().c_str(),
          deal.time,
          deal.account,
          deal.user_id,
          deal.deal_user_id,
          deal.deal_account,
          deal.market,
          deal.deal_id,
          deal.order_id,
          deal.deal_order_id,
          deal.side,
          deal.role,
          deal.price ? Utils::mpd_to_string(deal.price).c_str() : "NULL",
          deal.amount ? Utils::mpd_to_string(deal.amount).c_str() : "NULL",
          deal.deal ? Utils::mpd_to_string(deal.deal).c_str() : "NULL",
          deal.fee ? Utils::mpd_to_string(deal.fee).c_str() : "NULL",
          deal.deal_fee ? Utils::mpd_to_string(deal.deal_fee).c_str() : "NULL",
          deal.fee_asset ? deal.fee_asset : "",
          deal.deal_fee_asset ? deal.deal_fee_asset : ""
          );
    }
};


#endif // DEALWRITER_H_INCLUDED
