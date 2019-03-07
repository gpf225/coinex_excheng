/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/06, create
 */

# ifndef _CA_STATISTIC_H_
# define _CA_STATISTIC_H_

int init_statistic(void);
void stat_depth_req(void);
void stat_depth_cached(void);
void stat_depth_update_wait(void);
void stat_depth_update(void);
void stat_depth_update_timeout(void);
void stat_depth_update_released(void);

# endif