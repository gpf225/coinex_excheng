# ifndef _MI_HISTORY_H_
# define _MI_HISTORY_H_

int init_history(void);
int fini_history(void);

sds history_status(sds reply);
int append_index_history(const char *market, const mpd_t *price, const char *detail);

# endif
