/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 *
 *    该文件管理深度的订阅信息。
 */

# ifndef _CA_DEPTH_SUB_H_
# define _CA_DEPTH_SUB_H_

# include "ca_config.h"

int init_depth_sub(void);
int fini_depth_sub(void);

int depth_subscribe(nw_ses *ses, const char *market, const char *interval);
int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval);
int depth_unsubscribe_all(nw_ses *ses);

int depth_sub_reply(const char *market, const char *interval, json_t *result);
int depth_send_last(nw_ses *ses, const char *market, const char *interval);

size_t depth_subscribe_number(void);
size_t depth_poll_number(void);

# endif