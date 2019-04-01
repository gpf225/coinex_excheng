# ifndef _DR_FEE_RATE_H_
# define _DR_FEE_RATE_H_

struct dict_fee_rate_key {
    char        market[MARKET_NAME_MAX_LEN];
    char        stock[STOCK_NAME_MAX_LEN];
};

struct dict_fee_rate_val {
    uint32_t gear[20];
};

int init_fee_rate(void);
dict_t *get_fee_rate_dict(void);
int fee_rate_process(const char *market, const char *stock, const char *fee_rate_str);

# endif

