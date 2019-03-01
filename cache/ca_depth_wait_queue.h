/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 */

# ifndef _CA_DEPTH_WAIT_QUEUE_H_
# define _CA_DEPTH_WAIT_QUEUE_H_

# include "ca_config.h"

int depth_wait_queue_add(const char *market, const char *interval, uint32_t limit, nw_ses *ses, rpc_pkg *pkg);

# endif