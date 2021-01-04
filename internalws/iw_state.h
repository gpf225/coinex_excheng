# ifndef _IW_STATE_H_
# define _IW_STATE_H_

int init_state(void);
bool judege_state_period_is_day(json_t *params);
void direct_state_reply(nw_ses *ses, json_t *params, int64_t id);
int state_send_last(nw_ses *ses);
int state_subscribe(nw_ses *ses, json_t *market_list);
int state_unsubscribe(nw_ses *ses);
size_t state_subscribe_number(void);
bool market_exists(const char *market);

# endif

