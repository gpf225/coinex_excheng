/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 */

# ifndef _CA_DEPTH_WAIT_QUEUE_H_
# define _CA_DEPTH_WAIT_QUEUE_H_

# include "ca_config.h"

struct depth_wait_val {
    dict_t *dict_wait_session;
};

struct depth_wait_item {
    uint32_t limit;
    uint32_t sequence;
    int wait_type;
    rpc_pkg pkg;
};

int init_depth_wait_queue(void);
void fini_depth_wait_queue(void);

// 将对应的请求加入等待队列，加入成功返回1，如果已存在返回0，返回负数表示失败。
int depth_wait_queue_add(const char *market, const char *interval, uint32_t limit, nw_ses *ses, uint32_t sequence, rpc_pkg *pkg, int wait_type);
// 将对应的请求移出请求队列，成功返回1，如果该请求未存在返回0，返回负数表示失败。
int depth_wait_queue_remove(const char *market, const char *interval, uint32_t limit, nw_ses *ses, uint32_t sequence);
// 移出ses连接上的所有请求
int depth_wait_queue_remove_all(nw_ses *ses);
struct depth_wait_val *depth_wait_queue_get(const char *market, const char *interval);
# endif