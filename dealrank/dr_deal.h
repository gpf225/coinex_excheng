# ifndef _DR_DEAL_H_
# define _DR_DEAL_H_

# include "dr_config.h"

int dump_deal_and_fee(time_t start);
int deal_top_data(json_t **result, time_t start, time_t end, const char *market, int data_type, int top_num);
int deals_process(bool is_taker, uint32_t user_id, time_t timestamp, int type, const char *market, const char *stock, 
        const char *volume_str, const char *price_str, const char *fee_asset, const char *fee);
void clear_fee_dict(void);
int init_deal(void);

# endif

