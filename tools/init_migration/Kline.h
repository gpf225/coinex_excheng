#ifndef KLINE_H_INCLUDED
#define KLINE_H_INCLUDED

#include <string>
using namespace std;

extern "C" {
#include "ut_decimal.h"
}

namespace src_ns {

struct Kline {
  uint8_t appl_id ;/// 应用标识
  uint32_t contract_id;/// 交易对ID
  uint64_t range; /// K线类型
  uint64_t time; /// 行情时间
  string open_price; /// 开盘价
  string close_price; /// 收盘价
  string high_price; /// 最高价
  string low_price; /// 最低价
  string volume; /// 成交量
};
}


enum {
    INTERVAL_SEC,
    INTERVAL_MIN,
    INTERVAL_HOUR,
    INTERVAL_DAY,
};

struct kline_info_t {
    char* market = 0;
    uint8_t type = 0;  /// see above enum
    time_t timestamp = 0;

    mpd_t *open = nullptr;
    mpd_t *close = nullptr;
    mpd_t *high = nullptr;
    mpd_t *low = nullptr;
    mpd_t *volume = nullptr;
    mpd_t *deal = nullptr;

    ~kline_info_t() {
        if (market) free(market);

        if (open) mpd_del(open);
        if (close) mpd_del(close);
        if (high) mpd_del(high);
        if (low) mpd_del(low);
        if (volume) mpd_del(volume);
        if (deal) mpd_del(deal);
    }
};


#endif // KLINE_H_INCLUDED
