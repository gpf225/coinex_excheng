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
    double      visit_limit_start;
    int         visit_limit_count;
};

int init_server(void);
int ws_send_error_unknown_sub_user(nw_ses *ses, uint64_t id);
int ws_send_error_direct_result_null(nw_ses *ses, int64_t id);
void update_depth_cache(json_t *depth_data, const char *market, const char *interval, int ttl);

# endif

