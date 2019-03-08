/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/01/18, create
 */

# ifndef _AW_ASSET_SUB_H_
# define _AW_ASSET_SUB_H_

# include "aw_config.h"

int init_asset_sub(void);

int asset_subscribe_sub(nw_ses *ses, json_t *sub_users);
int asset_unsubscribe_sub(nw_ses *ses);
int asset_on_update_sub(uint32_t user_id, const char *asset);
size_t asset_subscribe_sub_number(void);

# endif

