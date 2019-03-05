/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/01, create
 */

# ifndef _AR_DEPTH_CACHE_H_
# define _AR_DEPTH_CACHE_H_

# include "ar_config.h"

typedef struct depth_cache_val {
    uint32_t limit;
    uint64_t expire_time;
    json_t *data;
}depth_cache_val;

int init_depth_cache();

int depth_cache_set(const char *market, const char *interval, uint32_t limit, uint32_t ttl, json_t *data);

struct depth_cache_val *depth_cache_get(const char *market, const char *interval, uint32_t limit);
void fini_depth_cache(void);

# endif