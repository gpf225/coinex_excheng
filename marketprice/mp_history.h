/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/16, create
 */

# ifndef _MP_HISTORY_H_
# define _MP_HISTORY_H_

# include "mp_config.h"
# include "mp_kline.h"

int init_history(void);
int fini_history(void);
int append_kline_history(const char *market, int type, time_t timestamp, struct kline_info *kinfo);

# endif

