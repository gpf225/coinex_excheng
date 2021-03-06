# ifndef _AH_CACHE_H_
# define _AH_CACHE_H_

# include "ah_config.h"

struct cache_val {
    uint64_t    time_cache;
    json_t      *result;
};

int init_cache(void);
int check_cache(nw_ses *ses, uint64_t id, sds key);
int check_depth_cache(nw_ses *ses, uint64_t id, sds key, int limit);
void replace_cache(sds cache_key, struct cache_val *val);
json_t *generate_depth_data(json_t *array, int limit);
json_t *pack_depth_result(json_t *result, uint32_t limit);

#endif
