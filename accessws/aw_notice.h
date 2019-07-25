/*
 * Description: 
 *     History: ouxiangyang, 2019/07/19, create
 */

# ifndef _AW_NOTICE_H_
# define _AW_NOTICE_H_

int init_notice(void);
int notice_subscribe(uint32_t user_id, nw_ses *ses);
int notice_unsubscribe(uint32_t user_id, nw_ses *ses);
int notice_message(json_t *message);
size_t notice_subscribe_number(void);

# endif

