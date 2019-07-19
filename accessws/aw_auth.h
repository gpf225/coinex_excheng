/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# ifndef _AW_AUTH_H_
# define _AW_AUTH_H_

int init_auth(void);

int send_auth_request(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params);
size_t pending_auth_request(void);
nw_ses *get_auth_user_ses(uint32_t user_id);
void auth_user_remove(uint32_t user_id);

# endif

