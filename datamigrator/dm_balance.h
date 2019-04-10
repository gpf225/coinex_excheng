/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# ifndef _DM_BALANCE_H_
# define _DM_BALANCE_H_

# include "stdint.h"

int balance_migrate(uint32_t user_id, double migrate_start_time, double migrate_end_time);

# endif