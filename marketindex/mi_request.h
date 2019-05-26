/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _MI_REQUEST_H_
# define _MI_REQUEST_H_

int init_request(void);
int fini_request(void);

typedef void (*request_callback)(uint32_t id, const char *exchange, json_t *reply);
int send_request(uint32_t id, const char *exchange, const char *url, double timeout, request_callback callback);

# endif

