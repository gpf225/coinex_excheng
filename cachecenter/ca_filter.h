
# ifndef _CA_FILTER_H_
# define _CA_FILTER_H_

# include "ca_config.h"

int init_filter(void);
int add_filter_queue(sds key, nw_ses *ses, rpc_pkg *pkg);
int remove_all_filter(nw_ses *ses);
void delete_filter_queue(sds key);
void reply_filter_message(sds key, bool is_error, json_t *reply);

# endif