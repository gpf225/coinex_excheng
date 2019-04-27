# ifndef _AR_STATE_H_
# define _AR_STATE_H_

int init_state(void);
void direct_state_reply(nw_ses *ses, json_t *params, int64_t id);
bool judege_period_is_day(int interval);

# endif

