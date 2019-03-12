/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# ifndef _AW_MARKET_H_
# define _AW_MARKET_H_

# include "aw_config.h"

int init_market(void);

bool market_exists(const char *market);
json_t* get_market_array(void);
dict_t* get_market(void);
void fini_market(void);

# endif