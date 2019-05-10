# ifndef _ME_ASSET_H_
# define _ME_ASSET_H_

# include "me_config.h"

struct asset_type {
    int prec_save;
    int prec_show;
    mpd_t *min;
};

int init_asset(void);
int update_asset(void);

bool asset_exist(uint32_t account, const char *asset);
struct asset_type *get_asset_type(uint32_t account, const char *asset);

int asset_prec_save(uint32_t account, const char *asset);
int asset_prec_show(uint32_t account, const char *asset);

int make_asset_backup(json_t *params);

# endif
