/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _AR_TICKER_H_
# define _AR_TICKER_H_

int init_ticker(void);

json_t *get_market_ticker(const void *market);
json_t *get_market_ticker_all(void);
int depth_ticker_update(const char *market, json_t *result);
int status_ticker_update(const char *market, json_t *result);

# endif

