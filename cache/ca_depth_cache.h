/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# ifndef _CA_DEPTH_CACHE_H_
# define _CA_DEPTH_CACHE_H_

# include "ca_config.h"

typedef struct depth_cache_val {
    uint32_t limit;
    double time;
    json_t *data;
    double limit_last_hit_time;
    uint32_t second_limit;
}depth_cache_val;

int init_depth_cache(double timeout);

int depth_cache_set(const char *market, const char *interval, uint32_t limit, json_t *data);
struct depth_cache_val* depth_cache_get(const char *market, const char *interval, uint32_t limit);
uint32_t depth_cache_get_update_limit(depth_cache_val *val, uint32_t limit);

# endif