/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# ifndef _GW_DISPATCHER_H_
# define _GW_DISPATCHER_H_

# include "hw_config.h"

int init_dispatcher(void);
void fini_dispatcher(void);

int dispatch_deal(json_t *msg);
int dispatch_stop(json_t *msg);
int dispatch_order(json_t *msg);
int dispatch_balance(json_t *msg);

# endif

