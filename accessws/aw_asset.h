/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# ifndef _AW_ASSET_H_
# define _AW_ASSET_H_

int init_asset(void);

// empty asset means subscribe all asset
int asset_subscribe(uint32_t user_id, nw_ses *ses, const char *asset, bool delay);
int asset_unsubscribe(uint32_t user_id, nw_ses *ses);
int asset_on_update(uint32_t user_id, uint32_t account, const char *asset, const char *available, const char *frozen);
size_t asset_subscribe_number(void);
void fini_asset(void);
# endif

