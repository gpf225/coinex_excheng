/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/08, create
 */

# ifndef _ME_MESSAGE_H_
# define _ME_MESSAGE_H_

# include "me_config.h"
# include "me_market.h"

int init_message(void);
int fini_message(void);

int push_balance_message(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change, mpd_t *result);
int push_order_message(uint32_t event, order_t *order, market_t *market);
int push_stop_message(uint32_t event, stop_t *stop, market_t *market, int status);
int push_deal_message(double t, uint64_t id, market_t *market, int side, order_t *ask, order_t *bid,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *ask_fee_asset, mpd_t *ask_fee, const char *bid_fee_asset, mpd_t *bid_fee);

int push_his_balance_message(json_t *msg);
int push_his_order_message(json_t *msg);
int push_his_stop_message(json_t *msg);
int push_his_deal_message(json_t *msg);

bool is_message_block(void);
sds message_status(sds reply);

# endif

