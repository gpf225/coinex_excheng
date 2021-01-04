/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# ifndef IW_HTTP_H
# define IW_HTTP_H

int init_http(void);

typedef void (*result_callback)(json_t *result);
int send_http_request(const char *method, json_t *params, result_callback callback);

# endif

