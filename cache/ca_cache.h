/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# ifndef _CA_CACHE_H_
# define _CA_CACHE_H_

struct dict_cache_val {
    double      time;
    json_t      *result;
};

int init_cache(void);
int add_cache(sds cache_key, json_t *result);
struct dict_cache_val *get_cache(sds key, int cache_time);

# endif