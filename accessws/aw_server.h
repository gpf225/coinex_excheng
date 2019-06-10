/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/21, create
 */

# ifndef _AW_SERVER_H_
# define _AW_SERVER_H_

# include "aw_config.h"

struct clt_info {
    bool        auth;
    uint32_t    user_id;
    char        *source;
    char        *remote;
};

int init_server(void);
void update_depth_cache(json_t *depth_data, const char *market, const char *interval, int ttl);

# endif

