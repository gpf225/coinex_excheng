/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# ifndef _IW_ASSET_H_
# define _IW_ASSET_H_

int init_asset(void);

// empty asset means subscribe all asset
int asset_subscribe(uint32_t user_id, nw_ses *ses, const char *asset);
int asset_unsubscribe(nw_ses *ses);
int asset_on_update(uint32_t user_id, uint32_t account, const char *asset, const char *available, const char *frozen, double timestamp);
size_t asset_subscribe_number(void);
void fini_asset(void);
# endif

