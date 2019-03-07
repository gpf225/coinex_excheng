/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/15, create
 */

# ifndef _CA_DEPTH_CACHE_H_
# define _CA_DEPTH_CACHE_H_

# include "ca_config.h"

/**
* depth_cache_val对象用于保存对应深度的缓存信息，以market+interval来唯一标志一个缓存对象。
* limit字段表示当前缓存所采用的limit值，低于该limit的请求可以复用该缓存，高于该limit的请求需要向下层服务获取数据。
* time表示缓存的初始时间，主要用于过期检验。
* data保存了当前缓存的数据。
* 
* 缓存更新根据market+interval以及最大的limit来更新深度信息，较小的limit所需的数据可以从较大limit数据裁剪得到。
* limit参数即表示当前缓存所采用的limit值，也表示当前请求最大的limit值。
* 当最大的limit项长时间不再有请求时，每次还根据limit拉取深度无疑是一种浪费，
* limit_last_hit_time字段记录最大limit项请求的最后一次访问时间，当该时间距离当前时间过大时就不再根据limit来更新深度，而是根据second_limit字段来更新深度，
* 此时second_limit成了当前最大的limit。
*
* second_limit字段保存当前第二大的limit值，当最大项limit过期时，就采用第二大的limit值。
*/
typedef struct depth_cache_val {
    uint32_t limit;
    uint64_t time;
    uint64_t ttl;
    json_t *data;
    uint64_t limit_last_hit_time;
    uint32_t second_limit;
}depth_cache_val;

int init_depth_cache(int timeout);

struct depth_cache_val* depth_cache_set(const char *market, const char *interval, uint32_t limit, json_t *data);
depth_cache_val* depth_cache_get(const char *market, const char *interval, uint32_t limit);
uint32_t depth_cache_get_update_limit(const char *market, const char *interval, uint32_t limit);

void fini_depth_cache(void);

# endif