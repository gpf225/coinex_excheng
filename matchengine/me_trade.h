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
bool check_market_account(uint32_t account, market_t *m);
json_t *get_market_last_info(void);
void get_fee_price(market_t *m, const char *asset, mpd_t *fee_price);
json_t *get_market_config(void);
json_t *get_market_detail(const char *market);

# endif

