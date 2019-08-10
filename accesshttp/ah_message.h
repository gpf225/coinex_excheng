/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _AH_MESSAGE_H_
# define _AH_MESSAGE_H_

# include "ah_config.h"

int init_message(void);
int push_notify_message(json_t *message);

# endif

