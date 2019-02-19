/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/18, create
 */

# ifndef _AR_DEPTH_H_
# define _AR_DEPTH_H_

# include "ar_config.h"

int init_depth(void);
json_t* depth_get_json(const char *market, int limit);

# endif