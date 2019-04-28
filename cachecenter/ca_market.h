/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/10, create
 */

# ifndef _CA_MARKET_H_
# define _CA_MARKET_H_

# include "ca_config.h"

int init_market(void);
dict_t *get_market(void);
bool market_exist(const char *market);

#endif