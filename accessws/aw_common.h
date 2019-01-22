/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/27, create
 */

# ifndef _AW_COMMON_H_
# define _AW_COMMON_H_

# include <stdbool.h>

bool is_good_limit(int limit);
bool is_good_interval(const char *interval);
bool is_good_market(const char *market);

bool is_empty_string(const char *str);


# endif

