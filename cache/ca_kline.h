# ifndef _CA_KLINE_H_
# define _CA_KLINE_H_

int init_kline(void);
void unsubscribe_kline_all(nw_ses *ses);
void kline_unsubscribe(nw_ses *ses, const char *market, int interval);
int kline_subscribe(nw_ses *ses, const char *market, int interval);
void unsubscribe_kline_all(nw_ses *ses);
int kline_request(nw_ses *ses, rpc_pkg *pkg, const char *market, time_t start, time_t end, int interval);

# endif

