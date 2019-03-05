/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _AR_SERVER_H_
# define _AR_SERVER_H_

# include "ar_config.h"

int init_server(void);
int send_result(nw_ses *ses, json_t *data);
int reply_internal_error(nw_ses *ses);
int reply_time_out(nw_ses *ses);

# endif

