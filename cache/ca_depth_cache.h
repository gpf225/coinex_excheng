/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# ifndef _CA_DEPTH_CACHE_H_
# define _CA_DEPTH_CACHE_H_

# include "ca_config.h"
# include "ca_depth_limit_list.h"

typedef struct depth_cache_val {
    uint32_t limit;
    uint64_t time;
    uint64_t ttl;
    json_t *data;
    depth_limit_list_t *limits;
}depth_cache_val;

int init_depth_cache(int timeout);

int depth_cache_set(const char *market, const char *interval, uint32_t limit, json_t *data);
depth_cache_val* depth_cache_get(const char *market, const char *interval, uint32_t limit);
uint32_t depth_cache_get_update_limit(const char *market, const char *interval, uint32_t limit);

void fini_depth_cache(void);

# endif