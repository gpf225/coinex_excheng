/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# ifndef _LP_STATE_H_
# define _LP_STATE_H_

# include "lp_config.h"

int init_state(void);
int state_subscribe(nw_ses *ses);
int state_unsubscribe(nw_ses *ses);
int state_send_last(nw_ses *ses);
size_t state_subscribe_number(void);
void fini_state(void);

# endif