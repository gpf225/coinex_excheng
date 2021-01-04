/*
 * Description: 
 *     History: damonyang@tencent.com, 2018/01/28, create
 */

# ifndef AW_HTTP_H
# define AW_HTTP_H

int init_http(void);

typedef void (*result_callback)(json_t *result);
int send_http_request(const char *method, json_t *params, result_callback callback);

# endif

