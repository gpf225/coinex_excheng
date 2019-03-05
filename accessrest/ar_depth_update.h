/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/01, create
 */

# ifndef _AR_DEPTH_UPDATE_H_
# define _AR_DEPTH_UPDATE_H_

# include "ar_config.h"

int init_depth_update(void);
int depth_update(nw_ses *ses, const char *market, const char *interval, uint32_t limit);

# endif