/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _AR_TICKER_H_
# define _AR_TICKER_H_

int init_ticker(void);

json_t *get_market_list(void);
json_t *get_market_ticker(const void *market);
json_t *get_market_ticker_all(void);

# endif

