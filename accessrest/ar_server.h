/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _AR_SERVER_H_
# define _AR_SERVER_H_

# include "ar_config.h"

int init_server(void);
int reply_json(nw_ses *ses, json_t *data, sds cache_key);
int reply_result_null(nw_ses *ses);
int reply_invalid_params(nw_ses *ses);

# endif

