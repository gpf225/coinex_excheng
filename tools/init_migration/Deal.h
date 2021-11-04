#ifndef DEAL_H_INCLUDED
#define DEAL_H_INCLUDED

#include <string>
using namespace std;

extern "C" {
#include "ut_decimal.h"
}

namespace src_ns {
    struct Deal {
        uint8_t appl_id; /// account
        uint64_t match_time; /// 成交时间
        uint32_t contract_id; /// 交易对ID、合约号
        string exec_id; /// 成交编号
        uint32_t bid_user_id; /// 买方账号ID
        uint32_t ask_user_id; /// 卖方账号ID
        string bid_order_id; /// 买方委托号
        string ask_order_id;/// 卖方委托号
        string match_price; ///  成交价 price
        string match_qty; /// 成交数量 amount
        string match_amt; /// 成交金额  deal
        string bid_fee; /// 买方手续费
        string ask_fee; /// 卖方手续费
        uint8_t is_taker; /// Taker方向 side	1buy -1sell
        uint64_t update_time; /// 最近更新时间 time
    };
}

struct deal_t {
    double time = 0; /// match time
    uint32_t user_id = 0;
    uint32_t account = 0;
    uint32_t deal_user_id = 0;
    uint32_t deal_account = 0;

    char *market = nullptr;
    uint64_t deal_id = 0;
    uint64_t order_id =0 ;
    uint64_t deal_order_id = 0;
    int side = 0;
    int role = 0;
    mpd_t *price = nullptr;
    mpd_t *amount = nullptr;
    mpd_t *deal = nullptr;
    char *fee_asset = nullptr;
    mpd_t *fee = nullptr;
    char *deal_fee_asset = nullptr;
    mpd_t *deal_fee = nullptr;

    ~deal_t() {
        if (market) free(market);
        if (price) mpd_del(price);
        if (amount) mpd_del(amount);
        if (deal) mpd_del(deal);
        if (fee_asset) free(fee_asset);
        if (fee) mpd_del(fee);
        if (deal_fee_asset) free(deal_fee_asset);
        if (deal_fee) mpd_del(deal_fee);
    }
};




#endif // DEAL_H_INCLUDED
