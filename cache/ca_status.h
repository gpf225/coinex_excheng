# ifndef _CA_STATUS_REQUEST_H_
# define _CA_STATUS_REQUEST_H_

int init_status(void);
void status_unsubscribe(nw_ses *ses, const char *market, int period);
void status_unsubscribe_all(nw_ses *ses);
int status_subscribe(nw_ses *ses, const char *market, int period);

# endif

