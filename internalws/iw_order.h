/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# ifndef _IW_ORDER_H_
# define _IW_ORDER_H_

int init_order(void);

int order_subscribe(uint32_t user_id, nw_ses *ses, const char *market);
int order_unsubscribe(nw_ses *ses);
int order_on_update(uint32_t user_id, int event, json_t *order);
int order_on_update_stop(uint32_t user_id, int event, json_t *order);
size_t order_subscribe_number(void);
void fini_order(void);

# endif

