# ifndef _ME_REPLY_H_
# define _ME_REPLY_H_

# include "me_config.h"

int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message);

int reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg);

int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg);

int reply_error_service_unavailable(nw_ses *ses, rpc_pkg *pkg);

int reply_error_timeout(nw_ses *ses, rpc_pkg *pkg);

int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result);

int reply_success(nw_ses *ses, rpc_pkg *pkg);

# endif
