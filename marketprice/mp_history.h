/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/16, create
 */

# ifndef _MP_HISTORY_H_
# define _MP_HISTORY_H_

# include "mp_config.h"

int init_history(void);
int fini_history(void);
int append_kline_history(const char *market, int type, time_t timestamp, mpd_t *open, mpd_t *close, mpd_t *high, mpd_t *low, mpd_t *volume, mpd_t *deal);

# endif

