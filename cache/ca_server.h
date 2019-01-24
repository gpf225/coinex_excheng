/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# ifndef _RH_SERVER_H_
# define _RH_SERVER_H_

int init_server(void);

int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result);
int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message);
int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg);

# endif

