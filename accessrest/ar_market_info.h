/*
 * Copyright (c) 2018, Mugui Zhou <zhoumugui@viabtc.com>
 * All rights reserved.
 *
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/18, create
 */

#ifndef _AR_MARKET_INFO_H_
#define _AR_MARKET_INFO_H_

# include "ar_config.h"

int init_market_info(void);

json_t* get_market_info_list(void);


#endif