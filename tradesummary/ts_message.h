# ifndef _TS_MESSAGE_H_
# define _TS_MESSAGE_H_

int init_message(void);

json_t *get_trade_rank(json_t *market_list, time_t start_time, time_t end_time);

# endif
