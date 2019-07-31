# ifndef _ME_REQUEST_H_
# define _ME_REQUEST_H_

typedef int (*request_callback)(json_t *reply, nw_ses *ses, rpc_pkg *pkg);

int init_request(void);
int fini_request(void);
int init_asset_config(void);
int init_market_config(void);
int update_assert_config(nw_ses *ses, rpc_pkg *pkg, request_callback callback);
int update_market_config(nw_ses *ses, rpc_pkg *pkg, request_callback callback);

# endif
