/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/01/22, create
 */

# ifndef _AW_SUB_USER_H_
# define _AW_SUB_USER_H_

# include "aw_config.h"

int sub_user_init(void);

int sub_user_add(uint32_t user_id, nw_ses *ses, json_t *params);
int sub_user_remove(uint32_t user_id, nw_ses *ses);
int sub_user_has(uint32_t user_id, nw_ses *ses, uint32_t sub_user_id);


# endif