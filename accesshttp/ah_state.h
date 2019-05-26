# ifndef _AH_STATE_H_
# define _AH_STATE_H_

# include "ah_config.h"

int init_state(void);
int direct_state_reply(nw_ses *ses, json_t *params, int64_t id);
bool judege_state_period_is_day(json_t *params);

# endif

