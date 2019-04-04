/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# ifndef _HW_MESSAGE_H_
# define _HW_MESSAGE_H_

# include "hw_config.h"

int init_message(void);

int64_t get_last_order_offset(void);
int64_t get_last_stop_offset(void);
int64_t get_last_deal_offset(void);
int64_t get_last_balance_offset(void);
void message_stop(int flag);
sds message_status(sds reply);

# endif