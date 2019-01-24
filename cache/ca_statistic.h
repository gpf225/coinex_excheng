/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# ifndef _CA_STATISTIC_H_
# define _CA_STATISTIC_H_

# include "ut_sds.h"

void stat_depth_inc(void);
void stat_depth_cache_hit(void);
void stat_depth_cache_miss(void);
void stat_depth_cache_update(void);
sds stat_status(sds reply);

# endif