/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _MI_EXCHANGE_H_
# define _MI_EXCHANGE_H_

# include "mi_config.h"

int init_exchange();

json_t *exchange_list(void);
bool exchange_is_supported(const char *name);
int exchange_parse_response(const char *name, json_t *response, mpd_t **price, double *timestamp);

# endif

