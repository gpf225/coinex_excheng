/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# ifndef _LP_MARKET_H_
# define _LP_MARKET_H_

# include "lp_config.h"

int init_market(void);

bool market_exists(const char *market);
json_t* get_market_array(void);
dict_t* get_market(void);

int market_subscribe(nw_ses *ses);
int market_unsubscribe(nw_ses *ses);
void on_market_update(json_t *params);
size_t market_subscribe_number(void);
int market_send_last(nw_ses *ses);
void fini_market(void);
# endif