/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/18, create
 */

# ifndef _ME_UPDATE_H_
# define _ME_UPDATE_H_

extern dict_t *dict_update;

struct update_key {
    uint32_t    user_id;
    char        asset[ASSET_NAME_MAX_LEN + 1];
    char        business[BUSINESS_NAME_MAX_LEN + 1];
    uint64_t    business_id;
};

struct update_val {
    double      create_time;
};

int init_update(void);
int update_user_balance(bool real, uint32_t user_id, const char *asset, const char *business, uint64_t business_id, mpd_t *change, json_t *detail);
int update_user_lock(bool real, uint32_t user_id, const char *asset, const char *business, uint64_t business_id, mpd_t *amount);
int update_user_unlock(bool real, uint32_t user_id, const char *asset, const char *business, uint64_t business_id, mpd_t *amount);
int update_add(uint32_t user_id, const char *asset, const char *business, uint64_t business_id, double create_time);

# endif

