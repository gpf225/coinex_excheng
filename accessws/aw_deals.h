/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/28, create
 */

# ifndef _AW_DEALS_H_
# define _AW_DEALS_H_

int init_deals(void);
int deals_sub_update(const char *market, json_t *result);

int deals_subscribe(nw_ses *ses, const char *market, uint32_t user_id);
int deals_unsubscribe(nw_ses *ses, uint32_t user_id);
int deals_new(uint32_t user_id, uint64_t id, double timestamp, int type, const char *market, const char *amount, const char *price);
size_t deals_subscribe_number(void);

# endif

