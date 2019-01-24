/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/02, create
 */

# ifndef _UT_RPC_REPLY_H_
# define _UT_RPC_REPLY_H_

# include <jansson.h>
# include <stdbool.h>
# include <stdint.h>

typedef struct ut_rpc_reply
{
    int id;
    int code;
    char *message;
    json_t *error;
    json_t *result;

} ut_rpc_reply_t;

bool reply_valid(ut_rpc_reply_t *reply);
bool reply_ok(ut_rpc_reply_t *reply);

bool reply_valid(ut_rpc_reply_t *reply);

ut_rpc_reply_t* reply_create(json_t *reply);
ut_rpc_reply_t* reply_load(const void *json_data, uint32_t data_size);

void reply_release(ut_rpc_reply_t *reply);
json_t* reply_get_result_json(int id, json_t *result);
json_t* reply_get_error_json(int id, int code, const char *message);


# define REPLY_TRACE_LOG(ses, pkg) {                                                    \
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);                               \
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u, reply: %s",                    \
        nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, reply_str);   \
    sdsfree(reply_str);                                                                 \
}


# define REPLY_INVALID_LOG(ses, pkg) {                              \
    sds hex = hexdump(pkg->body, pkg->body_size);                   \
    log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s",       \
        nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);    \
    sdsfree(hex);                                                   \
}

# define REPLY_ERROR_LOG(ses, pkg) {                                     \
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);                \
    log_error("error reply from: %s, cmd: %u, reply: %s",                \
        nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);   \
    sdsfree(reply_str);                                                  \
}

# endif