# ifndef _AH_SUB_ALL_H_
# define _AH_SUB_ALL_H_

int init_sub_all(void);
void direct_depth_reply(nw_ses *ses, json_t *params, int64_t id);
void direct_deals_reply(nw_ses *ses, json_t *params, int64_t id);
int deals_sub_send_full(nw_ses *ses, const char *market);

bool judege_state_period_is_day(json_t *params);
void direct_state_reply(nw_ses *ses, json_t *params, int64_t id);
json_t *get_state_notify_full(double last_notify);
json_t *get_state_notify_list(list_t *list, double last_notify);
json_t *get_state(const char *market);
int depth_send_clean(nw_ses *ses, const char *market, uint32_t limit, const char *interval);

# endif

