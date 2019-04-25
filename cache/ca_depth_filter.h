
# ifndef _CA_DEPTH_FILTER_H_
# define _CA_DEPTH_FILTER_H_

# include "ca_config.h"

struct dict_depth_filter_val {
    dict_t  *dict_filter_session;
};

struct depth_filter_item {
    rpc_pkg   pkg;
};

int init_depth_filter_queue(void);
int add_depth_filter_queue(const char *market, const char *interval, uint32_t limit, nw_ses *ses, rpc_pkg *pkg);
int depth_filter_remove_all(nw_ses *ses);
void delete_depth_filter_queue(const char *market, const char *interval);
void depth_out_reply(const char *market, const char *interval, bool is_error, json_t *reply);

# endif