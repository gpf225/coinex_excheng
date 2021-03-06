/*
 * Description: 
 *     History: yang@haipo.me, 2017/09/13, create
 */


# ifndef _UT_DEFINE_H_
# define _UT_DEFINE_H_

enum {
    ORDER_EVENT_PUT     = 1,
    ORDER_EVENT_UPDATE  = 2,
    ORDER_EVENT_FINISH  = 3,
};

enum {
    STOP_EVENT_PUT      = 1,
    STOP_EVENT_ACTIVE   = 2,
    STOP_EVENT_CANCEL   = 3,
};

# define MARKET_ORDER_TYPE_LIMIT     1
# define MARKET_ORDER_TYPE_MARKET    2

# define MARKET_ORDER_SIDE_ASK       1
# define MARKET_ORDER_SIDE_BID       2

# define MARKET_TRADE_SIDE_SELL      1
# define MARKET_TRADE_SIDE_BUY       2

# define MARKET_ROLE_MAKER           1
# define MARKET_ROLE_TAKER           2

# define MARKET_STOP_STATUS_ACTIVE   1
# define MARKET_STOP_STATUS_FAIL     2
# define MARKET_STOP_STATUS_CANCEL   3

# define STOP_STATE_LOW              1
# define STOP_STATE_HIGH             2

# define MARKET_NAME_MAX_LEN         30
# define ASSET_NAME_MAX_LEN          30

# define HISTORY_HASH_NUM            100

# define INTERVAL_MAX_LEN            16
# define BUSINESS_NAME_MAX_LEN       31
# define SOURCE_MAX_LEN              31
# define CLIENT_ID_MAX_LEN           32

# define OPTION_CHECK_MASK              0xff
# define OPTION_SUGGEST_STOCK_FEE       0x1
# define OPTION_SUGGEST_MONEY_FEE       0x2
# define OPTION_UNLIMITED_MIN_AMOUNT    0x4
# define OPTION_IMMEDIATED_OR_CANCEL    0x8
# define OPTION_FILL_OR_KILL            0x10
# define OPTION_HIDDEN                  0x20
# define OPTION_STOP_ORDER              0x40
# define OPTION_MAKER_ONLY              0x80
 
# define TOPIC_DEAL                  "deals"
# define TOPIC_STOP                  "stops"
# define TOPIC_ORDER                 "orders"
# define TOPIC_INDEX                 "indexs"
# define TOPIC_BALANCE               "balances"
# define TOPIC_NOTICE                "notices"

# define TOPIC_HIS_DEAL              "his_deals"
# define TOPIC_HIS_STOP              "his_stops"
# define TOPIC_HIS_ORDER             "his_orders"
# define TOPIC_HIS_BALANCE           "his_balances"

# endif

