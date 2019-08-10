# ifndef _UT_WS_CLT_H
# define _UT_WS_CLT_H
# include "nw_clt.h"
# include "ut_ws.h"
# include "ut_http.h"

# define WS_CONNECTTING    0x01
# define WS_HANDSHAKE      0x02

typedef struct ws_clt_cfg {
    char *name;
    char *url;
    dict_t *header;
    sds body;
    uint32_t max_pkg_size;
    uint32_t buf_limit;
    uint32_t read_mem;
    uint32_t write_mem;
    double reconnect_timeout;
    double heartbeat_timeout;
} ws_clt_cfg;

typedef struct ws_clt_type {
    void (*on_error)(nw_ses *ses, const char* error);
    void (*on_open)(nw_ses *ses);
    int  (*on_message)(nw_ses *ses, void *message, size_t size);
    void (*on_close)(nw_ses *ses);
} ws_clt_type;

typedef struct ws_svr_info {
    char        *url;
    char        *path;
    int         port;
    sds         field;
    bool        field_set;
    sds         value;
    bool        value_set;
    bool        upgrade;
    sds         message;
    sds         key;
    struct      ws_frame frame;
    struct      http_parser parser;
    struct      http_request_t *request;
    struct      http_response_t *response;
} ws_svr_info;

typedef struct ws_clt {
    char        *name;
    nw_clt      *raw_clt;
    uint32_t    addr_count;
    nw_addr_t   *addr_arr;
    double      last_heartbeat;
    double      heartbeat_timeout;
    int         upgrade;
    nw_timer    timer;
    http_parser_settings    settings;
    ws_clt_type             type;
    struct ws_svr_info      *svr_info;
} ws_clt;

ws_clt *ws_clt_create(ws_clt_cfg *cfg, ws_clt_type *type);
int ws_clt_start(ws_clt *clt);
int ws_clt_close(ws_clt *clt);
void ws_clt_release(ws_clt *clt);
bool ws_clt_connected(ws_clt *clt);

# endif
