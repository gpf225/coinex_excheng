/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# ifndef _IW_SIGN_H_
# define _IW_SIGN_H_

int init_sign(void);

int send_sign_request(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params);
size_t pending_sign_request(void);

# endif

