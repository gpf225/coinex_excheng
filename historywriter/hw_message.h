/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# ifndef _HW_MESSAGE_H_
# define _HW_MESSAGE_H_

# include "hw_config.h"

int init_message(void);
void fini_message(void);

void suspend_message(void);
void resume_message(void);
sds message_status(sds reply);

# endif
