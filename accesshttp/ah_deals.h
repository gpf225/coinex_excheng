# ifndef _AH_SUB_ALL_H_
# define _AH_SUB_ALL_H_

# include "ah_config.h"

int init_deals(void);
int direct_deals_reply(nw_ses *ses, json_t *params, int64_t id);

# endif

