# ifndef _AH_SUB_ALL_H_
# define _AH_SUB_ALL_H_

int init_sub_all(void);
bool judege_state_period_is_day(json_t *params);
void direct_state_reply(nw_ses *ses, json_t *params, int64_t id);
void direct_deals_reply(nw_ses *ses, json_t *params, int64_t id);

# endif

