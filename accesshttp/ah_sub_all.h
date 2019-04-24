# ifndef _AH_SUB_ALL_H_
# define _AH_SUB_ALL_H_

int init_sub_all(void);
bool judege_state_period_is_day(json_t *params);
void on_direct_reply(nw_ses *ses, json_t *params, uint32_t cmd, int64_t id);
void direct_state_reply(nw_ses *ses, json_t *params, int64_t id);

# endif

