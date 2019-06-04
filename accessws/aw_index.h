/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _AW_INDEX_H_
# define _AW_INDEX_H_

# include "aw_config.h"

int init_index(void);

int index_subscribe(nw_ses *ses);
int index_unsubscribe(nw_ses *ses);
int index_on_update(const char *market, const char *price);
size_t index_subscribe_number(void);

# endif
