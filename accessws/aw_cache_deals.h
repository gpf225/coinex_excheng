# ifndef _AW_CACHE_DEALS_H_
# define _AW_CACHE_DEALS_H_

int init_cache_deals(void);
void direct_deals_reply(nw_ses *ses, json_t *params, int64_t id);
int deals_sub_send_full(nw_ses *ses, const char *market);

# endif

