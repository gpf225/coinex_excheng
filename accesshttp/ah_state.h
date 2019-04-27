# ifndef _AH_STATE_H_
# define _AH_STATE_H_

int init_state(void);
bool judege_state_period_is_day(json_t *params);
void direct_state_reply(nw_ses *ses, json_t *params, int64_t id);

# endif

