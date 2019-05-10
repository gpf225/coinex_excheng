/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/29, create
 */

# ifndef _ME_TRADE_H_
# define _ME_TRADE_H_

# include "me_market.h"

extern dict_t *dict_market;

int init_trade(void);
int update_trade(void);

market_t *get_market(const char *name);
json_t *get_market_last_info(void);
mpd_t *get_fee_price(market_t *m, const char *asset);

# endif

