# ifndef _DR_OPERLOG_H_
# define _DR_OPERLOG_H_

sds history_status(sds reply);
int init_operlog(void);
int fini_operlog(void);
void delete_operlog(void);
int append_deal_msg(bool del_operlog, uint32_t ask_user_id, uint32_t bid_user_id, uint32_t taker_user_id, double timestamp, const char *market, const char *stock, const char *amount, 
        const char *price, const char *ask_fee_asset, const char *bid_fee_asset, const char *ask_fee, const char *bid_fee, const char *ask_fee_rate, const char *bid_fee_rate);

# endif
