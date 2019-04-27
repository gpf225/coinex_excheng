# ifndef _AW_CACHE_STATE_H_
# define _AW_CACHE_STATE_H_

int init_cache_state(void);
bool judege_state_period_is_day(json_t *params);
void direct_state_reply(nw_ses *ses, json_t *params, int64_t id);
json_t *get_state_notify_full(double last_notify);
json_t *get_state_notify_list(list_t *list, double last_notify);
json_t *get_state(const char *market);

# endif

