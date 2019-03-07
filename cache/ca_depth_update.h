/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# ifndef _CA_DEPTH_UPDATE_H_
# define _CA_DEPTH_UPDATE_H_

# include "ca_config.h"

int init_depth_update(void);
int depth_update_http(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval, uint32_t limit);
int depth_update_rest(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval, uint32_t limit);
int depth_update_sub(const char *market, const char *interval, uint32_t limit);

# endif