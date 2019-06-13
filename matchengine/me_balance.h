/*
 * Description: user balance manage
 *     History: yang@haipo.me, 2017/03/15, create
 */

# ifndef _ME_BALANCE_H_
# define _ME_BALANCE_H_

# include "me_config.h"

# define BALANCE_TYPE_AVAILABLE 1
# define BALANCE_TYPE_FROZEN    2
# define BALANCE_TYPE_LOCK      3

extern dict_t *dict_balance;

struct balance_key {
    uint32_t    type;
    char        asset[ASSET_NAME_MAX_LEN];
};

int init_balance(void);

mpd_t *balance_get(uint32_t user_id, uint32_t account, uint32_t type, const char *asset);
void   balance_del(uint32_t user_id, uint32_t account, uint32_t type, const char *asset);
mpd_t *balance_set(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount);
mpd_t *balance_add(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount);
mpd_t *balance_sub(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount);

mpd_t *balance_reset(uint32_t user_id, uint32_t account, const char *asset);
mpd_t *balance_freeze(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount);
mpd_t *balance_unfreeze(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount);

mpd_t *balance_available(uint32_t user_id, uint32_t account, const char *asset);
mpd_t *balance_total(uint32_t user_id, uint32_t account, const char *asset);

json_t *balance_query_list(uint32_t user_id, uint32_t account, json_t *params);
json_t *balance_query_lock_list(uint32_t user_id, uint32_t account, json_t *params);
json_t *balance_query_all(uint32_t user_id);
json_t *balance_query_users(uint32_t account, json_t *params);

json_t *balance_get_summary(const char *asset);

# endif

