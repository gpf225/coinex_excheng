/*
 * Description: 
 *     History: yangxiaoqiang, 2021/01/04, create
 */

# ifndef _IW_SERVER_H_
# define _IW_SERVER_H_

# include "iw_config.h"

struct clt_info {
    char        *source;
    char        *remote;
};

int init_server(void);
int ws_send_error_unknown_sub_user(nw_ses *ses, uint64_t id);
int ws_send_error_direct_result_null(nw_ses *ses, int64_t id);
void update_depth_cache(json_t *depth_data, const char *market, const char *interval, int ttl);

# endif

