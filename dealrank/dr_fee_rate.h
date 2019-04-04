# ifndef _DR_FEE_RATE_H_
# define _DR_FEE_RATE_H_

#define MAX_GEAR 21

struct dict_fee_rate_key {
    char        market[MARKET_NAME_MAX_LEN];
    char        stock[STOCK_NAME_MAX_LEN];
};

struct dict_fee_rate_val {
    mpd_t *volume_gear[MAX_GEAR];
};

int init_fee_rate(void);
dict_t *get_fee_rate_dict(void);
int fee_rate_process(const char *market, const char *stock, mpd_t *fee_rate, mpd_t *volume);

# endif

