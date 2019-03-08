/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# ifndef _CA_DEPTH_CACHE_H_
# define _CA_DEPTH_CACHE_H_

# include "ca_config.h"

typedef struct depth_cache_val {
    uint64_t time;
    uint64_t ttl;
    json_t *data;
}depth_cache_val;

int init_depth_cache(int timeout);
void fini_depth_cache(void);

struct depth_cache_val* depth_cache_set(const char *market, const char *interval, json_t *data);
depth_cache_val* depth_cache_get(const char *market, const char *interval);

# endif