/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# ifndef _AW_DEPTH_H_
# define _AW_DEPTH_H_

# include "aw_config.h"

int init_depth(void);

int depth_subscribe(nw_ses *ses, const char *market, uint32_t limit, const char *interval, bool is_full);
int depth_send_clean(nw_ses *ses, const char *market, uint32_t limit, const char *interval);
int depth_unsubscribe(nw_ses *ses);
size_t depth_subscribe_number(void);
size_t depth_subscribe_market_interval_limit(void);
size_t depth_subscribe_market_interval(void);
json_t *pack_depth_result(json_t *result, uint32_t limit);

# endif