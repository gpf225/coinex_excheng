/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# ifndef _HW_WRITER_H_
# define _HW_WRITER_H_

# include "hw_config.h"

int init_writer(void);
void fini_writer(void);

int submit_job(int db, sds sql);
sds writer_status(sds reply);

# endif

