/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# ifndef _HW_PERSIST_H_
# define _HW_PERSIST_H_

# include "hw_config.h"

int init_persist(void);
int64_t load_last_order_offset(void);
int64_t load_last_stop_offset(void);
int64_t load_last_deal_offset(void);
int64_t load_last_balance_offset(void);

# endif