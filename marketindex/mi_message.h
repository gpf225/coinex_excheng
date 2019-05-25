/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _MI_MESSAGE_H_
# define _MI_MESSAGE_H_

# include "mi_config.h"

int init_message(void);
int push_index_message(const char *market, const mpd_t *price, json_t *detail);

# endif

