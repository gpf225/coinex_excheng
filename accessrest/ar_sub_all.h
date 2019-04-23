# ifndef _AH_SUB_ALL_H_
# define _AH_SUB_ALL_H_

int init_sub_all(void);
void direct_depth_reply(nw_ses *ses, const char *market, const char *interval, uint32_t limit);
void direct_deals_result(nw_ses *ses, const char *market, int limit, uint64_t last_id);
bool judege_period_is_day(int interval);


# endif

