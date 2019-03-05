/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 *
 *    该文件管理深度的订阅信息。
 */

# ifndef _CA_DEPTH_SUB_H_
# define _CA_DEPTH_SUB_H_

# include "ca_config.h"

# define DEPTH_LIMIT_MAX_SIZE 32

struct depth_sub_val {
    dict_t *sessions; 
};

/* 
* 每组(market, interval, limit)唯一标志一种不同的深度信息，将limit字段分离出来，通过(market, interval)来标志一种不同的深度信息，
* 在拉取深度信息的时候只拉取limit最大的列表，少于limit的深度信息通过最大列表来计算，减少实际拉取数据的次数。
*/
struct depth_sub_limit_val {
    int limits[DEPTH_LIMIT_MAX_SIZE];
    int max;
    int size;
};

int init_depth_sub(void);
int fini_depth_sub(void);

/*
* ses对象订阅深度信息, 每个(ses, market, interval, limit)组合只能被成功订阅一次，重复订阅不会改变任何状态
* 返回0表示成功，返回其它负数表示失败。
*/
int depth_subscribe(nw_ses *ses, const char *market, const char *interval, uint32_t limit);

 /*
 * ses对象取消深度信息订阅，每个(ses, market, interval, limit)组合只能被成功取消一次，重复取消不会改变任何状态.
 * 返回0表示成功，返回其它负数表示失败。
 */
int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval, uint32_t limit);

/* 返回一个正整数表示ses对象订阅的深度数量，返回0表示ses未订阅任何深度信息。重复调用不会改变任何状态，重复调用将返回0。*/
int depth_unsubscribe_all(nw_ses *ses);

dict_t* depth_get_sub(void);
dict_t* depth_get_item(void);

size_t depth_subscribe_number(void);
size_t depth_poll_number(void);

# endif