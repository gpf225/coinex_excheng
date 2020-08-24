/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2020/08/24, create
 */

# ifndef _MP_REQUEST_H_
# define _MP_REQUEST_H_

typedef int (*request_callback)(json_t *reply);

int init_request(void);
int add_request(const char *method, request_callback callback);
json_t *get_index_list(void);
json_t *get_market_list(void);

# endif
