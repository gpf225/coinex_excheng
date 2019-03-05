/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/01, create
 *
 *     缓存深度请求，当有符合要求的深度请求正在更新数据时，不再转发请求至cache层获取数据，而是将该请求保存起来，等待深度数据返回。
 *     根据market+interval为key来缓存信息，每个缓存项都包含一个的字典（nw_ses为key, depth_wait_item为value的字典）。
 *     当有深度数据返回时，响应符合要求的请求。
 */

# ifndef _CA_DEPTH_WAIT_QUEUE_H_
# define _CA_DEPTH_WAIT_QUEUE_H_

# include "ar_config.h"

struct depth_wait_val {
    dict_t *dict_wait_session;
};

struct depth_wait_item {
    uint32_t limit;
    uint32_t sequence;
};

int init_depth_wait_queue(void);
void fini_depth_wait_queue(void);

// 将对应的请求加入等待队列，加入成功返回1，如果已存在返回0，返回负数表示失败。
int depth_wait_queue_add(const char *market, const char *interval, uint32_t limit, nw_ses *ses, uint32_t sequence);
// 将对应的请求移出请求队列，成功返回1，如果该请求未存在返回0，返回负数表示失败。
int depth_wait_queue_remove(const char *market, const char *interval, uint32_t limit, nw_ses *ses, uint32_t sequence);
// 移出ses连接上的所有请求
int depth_wait_queue_remove_all(nw_ses *ses);
struct depth_wait_val *depth_wait_queue_get(const char *market, const char *interval);

# endif