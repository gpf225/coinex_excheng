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
# define CMD_ASSET_LIST             101
# define CMD_ASSET_SUMMARY          102
# define CMD_ASSET_QUERY            103
# define CMD_ASSET_UPDATE           104
# define CMD_ASSET_HISTORY          105
# define CMD_ASSET_LOCK             106
# define CMD_ASSET_UNLOCK           107
# define CMD_ASSET_QUERY_LOCK       108
# define CMD_ASSET_BACKUP           109

// trade
# define CMD_ORDER_PUT_LIMIT        201
# define CMD_ORDER_PUT_MARKET       202
# define CMD_ORDER_CANCEL           203
# define CMD_ORDER_BOOK             204
# define CMD_ORDER_DEPTH            205
# define CMD_ORDER_PENDING          206
# define CMD_ORDER_PENDING_DETAIL   207
# define CMD_ORDER_DEALS            208
# define CMD_ORDER_FINISHED         209
# define CMD_ORDER_FINISHED_DETAIL  210
# define CMD_ORDER_PUT_STOP_LIMIT   211
# define CMD_ORDER_PUT_STOP_MARKET  212
# define CMD_ORDER_CANCEL_STOP      213
# define CMD_ORDER_PENDING_STOP     214
# define CMD_ORDER_FINISHED_STOP    215
# define CMD_ORDER_STOP_BOOK        216

// market
# define CMD_MARKET_LIST            301
# define CMD_MARKET_SUMMARY         302
# define CMD_MARKET_STATUS          303
# define CMD_MARKET_LAST            305
# define CMD_MARKET_KLINE           306
# define CMD_MARKET_DEALS           307
# define CMD_MARKET_DEALS_EXT       308
# define CMD_MARKET_USER_DEALS      309

// config
# define CMD_CONFIG_UPDATE_ASSET    401
# define CMD_CONFIG_UPDATE_MARKET   402

// longpoll
# define CMD_LP_DEPTH_SUBSCRIBE         501
# define CMD_LP_DEPTH_SUBSCRIBE_ALL     502
# define CMD_LP_DEPTH_UNSUBSCRIBE       503
# define CMD_LP_DEPTH_UPDATE            504

# define CMD_LP_MARKET_SUBSCRIBE        505
# define CMD_LP_MARKET_UNSUBSCRIBE      506
# define CMD_LP_MARKET_UPDATE           507

# define CMD_LP_STATE_SUBSCRIBE         508
# define CMD_LP_STATE_UNSUBSCRIBE       509
# define CMD_LP_STATE_UPDATE            510

# endif
