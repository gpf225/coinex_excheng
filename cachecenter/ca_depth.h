# ifndef _CA_DEPTH_H_
# define _CA_DEPTH_H_

int init_depth(void);
int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval);
int depth_subscribe(nw_ses *ses, const char *market, const char *interval);
int depth_request(nw_ses *ses, rpc_pkg *pkg, const char *market, const char *interval);
size_t depth_subscribe_number(void);
void depth_unsubscribe_all(nw_ses *ses);

# endif
