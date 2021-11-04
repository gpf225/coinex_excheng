
#include <algorithm>
#include "DealTransfer.h"
#include "FieldSelector.h"
#include "Progress.h"

int DealTransfer::init() {
    writer_ = make_shared<DealWriter>(ctx_);
    redis_writer_ = make_shared<DealRedisWriter>(ctx_);
    last_writer_ = make_shared<LastRedisWriter>(ctx_);

    return 0;
}

int DealTransfer::handle() {
    string sql = "select * from core_match order by match_time";
    int ret = mysql_real_query(ctx_.src_db_conn_, sql.c_str(), sql.length());
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(ctx_.src_db_conn_), mysql_error(ctx_.src_db_conn_));
        return -1;
    }

    MYSQL_RES *result = mysql_store_result(ctx_.src_db_conn_);
    stat_->total_ = mysql_num_rows(result);
    FieldSelector selector(result);

    using Deal=src_ns::Deal;
    vector<shared_ptr<Deal>> deals;

    Progress progress(stat_->total_);
    uint64_t cnt = 0;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)))  {
        progress.step_it(++cnt);

        shared_ptr<Deal> deal(new Deal());
        deal->appl_id = strtoul(row[selector["appl_id"]],nullptr,0);
        deal->match_time = strtoull(row[selector["match_time"]],nullptr,0);
        deal->contract_id = strtoul(row[selector["contract_id"]],nullptr,0);
        deal->exec_id = row[selector["exec_id"]];
        deal->bid_user_id = strtoul(row[selector["bid_user_id"]],nullptr,0);
        deal->ask_user_id = strtoul(row[selector["ask_user_id"]],nullptr,0);
        deal->bid_order_id = row[selector["bid_order_id"]];
        deal->ask_order_id = row[selector["ask_order_id"]];
        deal->match_price = row[selector["match_price"]];
        deal->match_qty = row[selector["match_qty"]];
        deal->match_amt = row[selector["match_amt"]];
        deal->bid_fee = row[selector["bid_fee"]];
        deal->ask_fee = row[selector["ask_fee"]];
        deal->is_taker = strtol(row[selector["is_taker"]],nullptr,0);
        deal->update_time = strtoull(row[selector["update_time"]],nullptr,0);

        deals.push_back(deal);
    }


    for (auto deal : deals) {
        vector<deal_t*> d;
        int ret = convert(*deal.get(),d);
        if (ret) {
            stat_->fail_++;
            log_error("convert deal fail bid_order_id:%s,ask_order_id:%s",deal->bid_order_id.c_str(),deal->ask_order_id.c_str());
            continue;
        }
        else
            stat_->success_++;
        for_each(d.begin(),d.end(),[&](deal_t *e){
                 assign_slice(e);
                 });
        assign_market(d[0]); ///< only get one of two opponent deals
    }

    if (save())
        return -1;
    if (save_to_redis())
        return -1;

    return 0;
}


void DealTransfer::assign_slice(deal_t *deal) {
    uint8_t db_id = assign_db(deal->user_id);
    uint16_t slice_id = SliceTransfer::assign_slice(deal->user_id);
    deal_slices[db_id][slice_id].deals_.push_back(shared_ptr<deal_t>(deal));
}


void DealTransfer::assign_market(deal_t *deal) {
    auto it = market_deals_.find(deal->market);
    if (it==market_deals_.end()) {
        vector<deal_t*> v;
        v.push_back(deal);
        market_deals_[deal->market] = v;
    }
    else {
        it->second.push_back(deal);
    }
}

int DealTransfer::save() {
    for (uint8_t db_id=0;db_id<DB_HISTORY_COUNT;db_id++) {
        for (uint16_t slice_id = 0;slice_id<HISTORY_HASH_NUM;slice_id++) {
            DealBookSlice &dbs = deal_slices[db_id][slice_id];
            if (dbs.deals_.empty())
                continue;
            CLargeStringArray data;
            for (auto deal : dbs.deals_) {
                string value = writer_->generate_value_list(*deal.get());
                data.Add(Utils::dup_string(value));
            }
            writer_->batch_insert(db_id,slice_id,data,ctx_.settings_.row_limit);
        }
    }
    return 0;
}

int DealTransfer::convert(src_ns::Deal &deal,vector<deal_t*> &deals) {
    deal_t *deal1 = new deal_t();
    deal1->account = deal.appl_id;
    deal1->time = deal.match_time/1000000;
    const Market *market = ctx_.get_market(deal.contract_id);
    if (market==nullptr)
        return -1;
    deal1->market  = strdup(market->name_.c_str());

    deal1->user_id = deal.bid_user_id;
    deal1->deal_user_id = deal.ask_user_id;
    order_t *bid_order = (order_t*)ctx_.get_type_order(deal.bid_order_id);
    if (bid_order==nullptr) {
        delete deal1;
        log_error("order %s not found",deal.bid_order_id.c_str());
        return -1;
    }
    deal1->order_id = bid_order->id;

    order_t *ask_order = (order_t*)ctx_.get_type_order(deal.ask_order_id);
    if (ask_order==nullptr) {
        delete deal1;
        log_error("order %s not found",deal.ask_order_id.c_str());
        return -1;
    }
    deal1->deal_id = ++deal_id_start_;
    deal1->deal_order_id = ask_order->id;

    deal1->price = decimal(deal.match_price.c_str(),0);
    deal1->amount = decimal(deal.match_qty.c_str(),0);
    deal1->deal = decimal(deal.match_amt.c_str(),0);
    deal1->fee = decimal(deal.bid_fee.c_str(),0);
    deal1->deal_fee = decimal(deal.ask_fee.c_str(),0);
    deal1->side = 2;
    deal1->role = deal.is_taker==1 ?  2:1;
    deals.push_back(deal1);

    deal_t *deal2 = new deal_t();
    deal2->account = deal.appl_id;
    deal2->time = deal.match_time/1000000;
    deal2->market  = strdup(market->name_.c_str());
    deal2->deal_id = ++deal_id_start_;
    deal2->user_id = deal.ask_user_id;
    deal2->deal_user_id = deal.bid_user_id;
    deal2->order_id = ask_order->id;
    deal2->deal_order_id = bid_order->id;
    deal2->price = decimal(deal.match_price.c_str(),0);
    deal2->amount = decimal(deal.match_qty.c_str(),0);
    deal2->deal = decimal(deal.match_amt.c_str(),0);
    deal2->fee = decimal(deal.bid_fee.c_str(),0);
    deal2->deal_fee = decimal(deal.ask_fee.c_str(),0);
    deal2->side = 1;
    deal2->role = deal.is_taker==1 ? 1:2;
    deals.push_back(deal2);
    return 0;
}

int DealTransfer::save_to_redis() {
    for (auto market_deal : market_deals_) {
        vector<deal_t*> &v = market_deal.second;
        if (v.empty())
            continue;
        auto it = v.begin();
        int skip = v.size() - ctx_.settings_.deal_summary_max;
        while(skip>0) {
            it++;
            skip--;
        }
        while(it!=v.end()) {
            redis_writer_->save(*it);
            it++;
        }
        it--;
        last_writer_->save(*it);
    }
    return 0;
}
