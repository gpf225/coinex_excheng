/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/28, create
 */

# ifndef _AW_STATE_H_
# define _AW_STATE_H_

int init_state(void);

int state_subscribe(nw_ses *ses, json_t *market_list);
int state_unsubscribe(nw_ses *ses);
int state_send_last(nw_ses *ses);
size_t state_subscribe_number(void);
bool market_exists(const char *market);

# endif

