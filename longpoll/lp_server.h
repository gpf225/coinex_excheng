/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# ifndef _LP_SERVER_H_
# define _LP_SERVER_H_

# include "nw_ses.h"
# include <jansson.h>

int init_server(void);

int notify_message(nw_ses *ses, int command, json_t *message);

# endif