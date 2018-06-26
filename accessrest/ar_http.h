/*
 * Description: 
 *     History: damonyang@tencent.com, 2018/01/28, create
 */

# ifndef AR_HTTP_H
# define AR_HTTP_H

# include "ar_config.h"

int init_http(void);

typedef void (*result_callback)(json_t *result);
int send_http_request(const char *method, json_t *params, result_callback callback);

# endif

