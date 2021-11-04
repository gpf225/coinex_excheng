#ifndef ACCOUNT_H_INCLUDED
#define ACCOUNT_H_INCLUDED

#include<string>
using namespace std;

extern "C" {
#include "ut_mysql.h"
}

namespace src_ns {
    struct Account {
        uint8_t appl_id;
        uint32_t user_id; /// 用户ID
        uint32_t currency_id; /// 币种ID
        string total_money; /// 总资产，包含已冻结
        string order_frozen_money;/// 委托冻结金额
        uint64_t update_time; /// 最近更新时间
        uint8_t is_forbid; ///
    };
}

# define BALANCE_TYPE_AVAILABLE 1
# define BALANCE_TYPE_FROZEN    2
# define BALANCE_TYPE_LOCK      3

struct balance_t {
    uint32_t user_id = 0;
    uint8_t account = 0;
    char *asset = nullptr;
    uint8_t t = 0; ///  see above BALANCE_TYPE_
    mpd_t *balance = nullptr;
    double update_time = 0;

    ~balance_t() {
        if (asset) free(asset);
        if (balance) mpd_del(balance);
    }
};

#endif // ACCOUNT_H_INCLUDED
