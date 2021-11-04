#ifndef ORDER_H_INCLUDED
#define ORDER_H_INCLUDED


#include<string>

extern "C" {
#include "ut_decimal.h"
}

using namespace std;
namespace src_ns {
    struct Order {
        int appl_id; /// 应用标识 --->account
        uint64_t  timestamp; /// 委托时间 unit:us
        uint32_t user_id; /// 用户ID
        uint32_t contract_id; /// market id
        string uuid; /// order id
        int8_t side ; /// 1:buy -1: sell
        string price;
        string quantity;
        int8_t order_type; /// 委托类型，1-限价 3-市价
        string stop_price;
        uint8_t order_status; /// 委托状态 0-未申报,1-正在申报,2-已申报未成交,3-部分成交,4-全部成交,5-部分撤单,6-全部撤单,7-撤单中,8-失效,11-缓存高于条件的委托,12-缓存低于条件的委托
        string maker_fee_ratio; /// Maker手续费率
        string taker_fee_ratio; /// Taker手续费率
        string client_order_id; /// 客户订单编号
        string filled_currency; /// 成交金额
        string filled_quantity; /// 成交量
        uint64_t match_time; /// 成交时间 unit :us
        uint8_t order_sub_type; /// 委托子类型, 1-被动委托；2-最近价触发条件委托；3-指数触发条件委托；4-标记价触发条件委托
    };
}

const uint8_t MARKET_ORDER_TYPE_LIMIT =   1;
const uint8_t MARKET_ORDER_TYPE_MARKET =   2;

const uint8_t MARKET_ORDER_SIDE_ASK  =  1;
const uint8_t MARKET_ORDER_SIDE_BID  =  2;

const uint8_t MARKET_ORDER_TYPE_ORDER = 1;
const uint8_t MARKET_ORDER_TYPE_STOP = 2;

typedef struct order_t {
    uint64_t        id = 0;
    uint32_t        type = 0;
    uint32_t        side = 0;
    double          create_time = 0;
    double          update_time = 0;
    uint32_t        user_id = 0;
    uint32_t        account = 0 ;
    uint32_t        option = 0 ;
    char            *market = nullptr;
    char            *source = nullptr;
    char            *fee_asset = nullptr;
    char            *client_id = nullptr;
    mpd_t           *fee_discount = nullptr;
    mpd_t           *price = nullptr;
    mpd_t           *amount = nullptr;
    mpd_t           *taker_fee = nullptr;
    mpd_t           *maker_fee = nullptr;
    mpd_t           *left = nullptr;
    mpd_t           *frozen = nullptr;
    mpd_t           *deal_stock = nullptr;
    mpd_t           *deal_money = nullptr;
    mpd_t           *money_fee = nullptr;
    mpd_t           *stock_fee = nullptr;
    mpd_t           *asset_fee = nullptr;
    mpd_t           *last_deal_amount = nullptr;
    mpd_t           *last_deal_price = nullptr;
    double          last_deal_time = 0;
    uint64_t        last_deal_id = 0;
    uint32_t        last_role = 0;
    mpd_t           *fee_price = nullptr;
    char            deal_flag = 0;
    char            status = 0;

    ~order_t() {
        if (market) free(market);
        if (source) free(source);
        if (client_id) free(client_id);
        if (fee_asset) free(fee_asset);
        if (fee_discount) mpd_del(fee_discount);
        if (price) mpd_del(price);
        if (amount) mpd_del(amount);
        if (taker_fee) mpd_del(taker_fee);
        if (maker_fee) mpd_del(maker_fee);
        if (left) mpd_del(left);
        if (frozen) mpd_del(frozen);
        if (deal_stock) mpd_del(deal_stock);
        if (deal_money) mpd_del(deal_money);
        if (money_fee) mpd_del(money_fee);
        if (stock_fee) mpd_del(stock_fee);
        if (asset_fee) mpd_del(asset_fee);
        if (last_deal_amount) mpd_del(last_deal_amount);
        if (last_deal_price) mpd_del(last_deal_price);
    }
} order_t;

typedef struct stop_t {
    uint64_t        id = 0;
    uint32_t        type  = 0;
    uint32_t        side = 0;
    double          create_time = 0;
    double          update_time = 0;
    uint32_t        user_id = 0;
    uint32_t        account = 0;
    uint32_t        option = 0;
    uint32_t        state = 0;
    char            *market = nullptr;
    char            *source = nullptr;
    char            *fee_asset = nullptr;
    char            *client_id = nullptr;
    mpd_t           *fee_discount = nullptr;
    mpd_t           *stop_price = nullptr;
    mpd_t           *price = nullptr;
    mpd_t           *amount = nullptr;
    mpd_t           *taker_fee = nullptr;
    mpd_t           *maker_fee = nullptr;
    uint64_t        real_order_id = 0;

    ~stop_t() {
        if (market) free(market);
        if (source) free(source);
        if (fee_asset) free(fee_asset);
        if (client_id) free(client_id);
        if (fee_discount) mpd_del(fee_discount);
        if (stop_price) mpd_del(stop_price);
        if (price) mpd_del(price);
        if (amount) mpd_del(amount);
        if (taker_fee) mpd_del(taker_fee);
        if (maker_fee) mpd_del(maker_fee);
    }
} stop_t;

#endif // ORDER_H_INCLUDED
