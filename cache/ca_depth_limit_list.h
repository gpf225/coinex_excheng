/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# ifndef _CA_DEPTH_LIMIT_LIST_H_
# define _CA_DEPTH_LIMIT_LIST_H_

# include "ca_config.h"

dict_t *dict_limit_list_create(void);

// 将key-value键值对加入字典，成功返回0，失败返回-1
int dict_limit_list_add(dict_t *dict, uint32_t key, uint32_t value);

// 将key对应的值增加1，然后返回增加后的值；如果key不存在则将新插入一条值为1的记录， 然后返回1.
uint32_t dict_limit_list_inc(dict_t *dict, uint32_t key);
// 将key对应的值减少1，然后返回减少后的值；如果key不存在则不做任何操作，返回0
uint32_t dict_limit_list_dec(dict_t *dict, uint32_t key);
// 返回key对应的值，如果key不存在则返回0
uint32_t dict_limit_list_get(dict_t *dict, uint32_t key);
// 返回true表示key存在
bool dict_limit_list_exist(dict_t *dict, uint32_t key);
// 获取dict中的最大值。如果返回0，则表示dict已经不包含任何limit信息
uint32_t dict_limit_list_max(dict_t *dict);

# endif