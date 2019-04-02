/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/04/01, create
 */

# ifndef _DM_MIGRATE_H_
# define _DM_MIGRATE_H_

# include "dm_config.h"

int init_migrate(void);

void migrate_cancel(void);

sds migrate_status(sds reply);

# endif