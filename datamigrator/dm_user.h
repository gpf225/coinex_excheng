/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# ifndef _DM_USER_H_
# define _DM_USER_H_

# include "dm_config.h"

# define MAX_USER_SIZE 1000 

typedef struct user_list_t {
    uint32_t users[MAX_USER_SIZE];
    uint32_t size;
}user_list_t;


int init_user(uint32_t start_uid, uint32_t last_uid);
user_list_t *get_next_user_list(void);
void user_list_free(user_list_t *obj);
bool user_has_more(void);

# endif