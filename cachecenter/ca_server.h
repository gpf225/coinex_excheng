/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# ifndef _RH_SERVER_H_
# define _RH_SERVER_H_

int init_server(void);

int push_data(nw_ses *ses, uint32_t command, char *data, size_t len);
int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json);
int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result);
int reply_time_out(nw_ses *ses, rpc_pkg *pkg);
int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message);
int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg);
int depth_subscribe_all(nw_ses *ses);
dict_t *get_sub_all_dict();

# endif

