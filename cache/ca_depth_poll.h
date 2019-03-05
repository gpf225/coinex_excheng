/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# ifndef _CA_DEPTH_POLL_H_
# define _CA_DEPTH_POLL_H_

# include "ca_config.h"

int init_depth_poll(void);
int depth_sub_handle(const char *market, const char *interval, json_t *result, uint32_t result_limit);
# endif