# ifndef _DR_HISTORY_H_
# define _DR_HISTORY_H_

struct deals_info {
    uint32_t 	user_id;
    char        *volume_bid;
    char        *volume_ask;
    char        *deal_bid;
    char        *deal_ask;
    char        *volume_taker_bid;
    char        *volume_taker_ask;
    uint32_t    trade_num_taker_bid;
    uint32_t    trade_num_taker_ask;
    uint32_t    trade_num_total;
    dict_t      *dict_fee;
};

int init_history(void);
void dump_deals_to_db(list_t *list_deals, const char *market, const char *stock, time_t start);
void dump_fee_to_db(dict_t *dict_fee, time_t start);
int fini_history(void);

# endif

