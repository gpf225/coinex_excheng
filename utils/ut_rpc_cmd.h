/*
 * Description: 
 *     History: yang@haipo.me, 2016/09/30, create
 */

# ifndef _UT_RPC_CMD_H_
# define _UT_RPC_CMD_H_

// monitor
# define CMD_MONITOR_INC            1
# define CMD_MONITOR_SET            2
# define CMD_MONITOR_LIST_SCOPE     3
# define CMD_MONITOR_LIST_KEY       4
# define CMD_MONITOR_LIST_HOST      5
# define CMD_MONITOR_QUERY          6
# define CMD_MONITOR_DAILY          7

// balance
# define CMD_BALANCE_QUERY          101
# define CMD_BALANCE_UPDATE         102
# define CMD_BALANCE_HISTORY        103
# define CMD_ASSET_LIST             104
# define CMD_ASSET_SUMMARY          105

// trade
# define CMD_ORDER_PUT_LIMIT        201
# define CMD_ORDER_PUT_MARKET       202
# define CMD_ORDER_QUERY            203
# define CMD_ORDER_CANCEL           204
# define CMD_ORDER_BOOK             205
# define CMD_ORDER_BOOK_DEPTH       206
# define CMD_ORDER_DETAIL           207
# define CMD_ORDER_HISTORY          208
# define CMD_ORDER_DEALS            209
# define CMD_ORDER_DETAIL_FINISHED  210

// market
# define CMD_MARKET_STATUS          301
# define CMD_MARKET_KLINE           302
# define CMD_MARKET_DEALS           303
# define CMD_MARKET_LAST            304
# define CMD_MARKET_STATUS_TODAY    305
# define CMD_MARKET_USER_DEALS      306
# define CMD_MARKET_LIST            307
# define CMD_MARKET_SUMMARY         308

# endif
