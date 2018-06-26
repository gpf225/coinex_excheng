/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _UT_PROFILE_H_
# define _UT_PROFILE_H_

int profile_init(const char *scope, const char *host);
void profile_set(const char *key, uint64_t val);
void profile_inc(const char *key, uint64_t val);

# endif

