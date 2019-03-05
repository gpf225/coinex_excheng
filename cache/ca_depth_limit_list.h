/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# ifndef _CA_DEPTH_LIMIT_LIST_H_
# define _CA_DEPTH_LIMIT_LIST_H_

# include "ca_config.h"

typedef struct depth_limit_list_t {
    dict_t *dict_limits;
}depth_limit_list_t;

depth_limit_list_t *depth_limit_list_create(void);
void depth_limit_list_free(depth_limit_list_t *list);

// 将key-value键值对加入字典，成功返回0，失败返回-1,如果key已存在，则不做任何操作，直接返回-1
int depth_limit_list_add(depth_limit_list_t *list, uint32_t key, uint32_t value);
// 将key设置为value指定的值，如果key不存在则插入一条记录，成功返回0，失败返回-1
int depth_limit_list_reset(depth_limit_list_t *list, uint32_t key, uint32_t value);
// 返回key对应的值，如果key不存在则返回0
uint32_t depth_limit_list_get(depth_limit_list_t *list, uint32_t key);
// 移除key对应的值，并返回被移除的值，如果key不存在，返回0
uint32_t depth_limit_list_remove(depth_limit_list_t *list, uint32_t key);

// 将key对应的值增加1，然后返回增加后的值；如果key不存在则将新插入一条值为1的记录， 然后返回1.
uint32_t depth_limit_list_inc(depth_limit_list_t *list, uint32_t key);
// 将key对应的值减少1，然后返回减少后的值；如果key不存在则不做任何操作，返回0
uint32_t depth_limit_list_dec(depth_limit_list_t *list, uint32_t key);

// 返回true表示key存在
bool depth_limit_list_exist(depth_limit_list_t *list, uint32_t key);
// 获取dict中的最大值。如果返回0，则表示dict已经不包含任何limit信息
uint32_t depth_limit_list_max(depth_limit_list_t *list);

uint32_t depth_limit_list_retrieve(depth_limit_list_t *list, uint32_t* limits);

# endif