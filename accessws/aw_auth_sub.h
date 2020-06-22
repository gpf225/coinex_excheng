/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/01/18, create
 */

# ifndef _AW_AUTH_SUB_H_
# define _AW_AUTH_SUB_H_

int init_auth_sub(void);

int send_auth_sub_request(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params);
size_t pending_auth_sub_request(void);

# endif

