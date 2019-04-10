/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# ifndef _DM_ORDER_H_
# define _DM_ORDER_H_

# include "stdint.h"

int order_migrate(uint32_t user_id, double migrate_start_time, double migrate_end_time);
double order_get_end_time(uint32_t user_id, double migrate_start_time, int least_day_per_user, int max_order_per_user);

# endif