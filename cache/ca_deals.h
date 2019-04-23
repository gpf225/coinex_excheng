# ifndef _CA_DEALS_REQUEST_H_
# define _CA_DEALS_REQUEST_H_

int init_deals(void);
void deals_unsubscribe_all(nw_ses *ses);
void deals_unsubscribe(nw_ses *ses, const char *market);
int deals_subscribe(nw_ses *ses, const char *market);
void deals_sub_send_full(nw_ses *ses);

# endif

