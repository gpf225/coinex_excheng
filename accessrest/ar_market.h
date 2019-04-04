/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/18, create
 */

#ifndef _AR_MARKET_H_
#define _AR_MARKET_H_

# include "ar_config.h"

int init_market(void);
void fini_market(void);

json_t* get_market_info_list(void);

dict_t *get_market(void);
/*ouxy
json_t *get_market_list(void);
*/
bool market_exist(const char *market);

#endif

