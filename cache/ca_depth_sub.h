/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 *
 *    该文件管理深度的订阅信息。
 */

# ifndef _CA_DEPTH_SUB_H_
# define _CA_DEPTH_SUB_H_

# include "ca_config.h"

dict_t *depth_get_sub();
dict_t *depth_get_poll();

# endif