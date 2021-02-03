/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# ifndef _IW_KLINE_H_
# define _IW_KLINE_H_

int init_kline(void);

int kline_subscribe(nw_ses *ses, const char *market, int interval);
int kline_unsubscribe(nw_ses *ses);
size_t kline_subscribe_number(void);

# endif

