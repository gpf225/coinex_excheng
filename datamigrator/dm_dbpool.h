/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# ifndef _DM_DBPOOL_H_
# define _DM_DBPOOL_H_

# include "dm_config.h"

int init_dbpool(void);
MYSQL* get_old_db_connection(void);
MYSQL* get_new_db_connection(uint32_t user_id);


# endif