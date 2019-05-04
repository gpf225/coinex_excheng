# ifndef _DR_MESSAGE_H_
# define _DR_MESSAGE_H_

int init_message(void);
void store_message(uint32_t ask_user_id, uint32_t bid_user_id, uint32_t taker_user_id, double timestamp, const char *market, const char *stock, 
        const char *amount, const char *price, const char *ask_fee_asset, const char *bid_fee_asset, const char *ask_fee,
        const char *bid_fee, const char *ask_fee_rate, const char *bid_fee_rate);
# endif

