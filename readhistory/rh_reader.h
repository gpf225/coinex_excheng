/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/24, create
 */

# ifndef _RH_READER_H_
# define _RH_READER_H_

# include "rh_config.h"

json_t *get_user_balance_history(MYSQL *conn, uint32_t user_id, uint32_t account,
        const char *asset, const char *business, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit);
json_t *get_user_order_history(MYSQL *conn, uint32_t user_id, int32_t account,
        const char *market, int side, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit);
json_t *get_user_stop_history(MYSQL *conn, uint32_t user_id, int32_t account,
        const char *market, int side, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit);
json_t *get_user_deal_history(MYSQL *conn, uint32_t user_id, int32_t account,
        const char *market, int side, uint64_t start_time, uint64_t end_time, size_t offset, size_t limit);
json_t *get_order_detail(MYSQL *conn, uint32_t user_id, uint64_t order_id);
json_t *get_order_deals(MYSQL *conn, uint32_t user_id, int32_t account, uint64_t order_id, size_t offset, size_t limit);

# endif

