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

# define MARKET_ORDER_TYPE_LIMIT    1
# define MARKET_ORDER_TYPE_MARKET   2

# define MARKET_ORDER_SIDE_ASK      1
# define MARKET_ORDER_SIDE_BID      2

# define MARKET_TRADE_SIDE_SELL     1
# define MARKET_TRADE_SIDE_BUY      2

# define MARKET_ROLE_MAKER          1
# define MARKET_ROLE_TAKER          2

# define MARKET_STOP_STATUS_ACTIVE  1
# define MARKET_STOP_STATUS_FAIL    2
# define MARKET_STOP_STATUS_CANCEL  3

# define MARKET_NAME_MAX_LEN   		30
# define ASSET_NAME_MAX_LEN    		30

# define HISTORY_HASH_NUM           100

# endif

