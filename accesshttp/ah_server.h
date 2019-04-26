/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/21, create
 */

# ifndef _AH_SERVER_H_
# define _AH_SERVER_H_

int init_server(void);
void reply_message(nw_ses *ses, int64_t id, json_t *result);
void reply_error_invalid_argument(nw_ses *ses, int64_t id);
void reply_result_null(nw_ses *ses, int64_t id);
json_t *generate_depth_data(json_t *array, int limit);
json_t *pack_depth_result(json_t *result, uint32_t limit);

# endif

