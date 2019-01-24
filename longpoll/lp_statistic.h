/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/02, create
 */

# ifndef _LP_STATISTIC_H_
# define _LP_STATISTIC_H_

# include "lp_config.h"

int init_statistic(void);

void stat_market_poll(void);
void stat_market_update(uint32_t count);

void stat_state_poll(void);
void stat_state_update(uint32_t count);

void stat_depth_poll(uint32_t count);
void stat_depth_update(void);

# endif