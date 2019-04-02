# ifndef _CA_DEPTH_H_
# define _CA_DEPTH_H_

int init_depth(void);
int unsubscribe_depth_all(nw_ses *ses);
int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval);
int depth_subscribe(nw_ses *ses, const char *market, const char *interval);
int depth_request(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval);
int depth_send_last(nw_ses *ses, const char *market, const char *interval);
void add_subscribe_depth_ses(nw_ses *ses);

void add_subscribe_depth_all_ses(nw_ses *ses);
void del_subscribe_depth_all_ses(nw_ses *ses);
void re_subscribe_depth_all(void);

# endif