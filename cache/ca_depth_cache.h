/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# ifndef _CA_DEPTH_CACHE_H_
# define _CA_DEPTH_CACHE_H_

# include "ca_config.h"

typedef struct depth_cache_val {
    double time;
    json_t *data;
}depth_cache_val;

int init_depth_cache(void);

int depth_cache_set(const char *market, const char *interval, uint32_t limit, json_t *data);
struct depth_cache_val* depth_cache_get(const char *market, const char *interval, uint32_t limit);

# endif