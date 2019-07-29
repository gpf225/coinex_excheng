# include <assert.h>
# include <sys/types.h>
# include <sys/socket.h>
# include <arpa/inet.h>
# include <netdb.h>
# include "ut_ws.h"
# include "ut_ws_clt.h"
# include "ut_http.h"
# include "ut_misc.h"
# include "ut_base64.h"
# include "ut_sds.h"

static int get_network_addr(char *host, int port, nw_addr_t *nw_addr)
{
    struct sockaddr *addr = NULL;
    struct addrinfo *result;
    struct addrinfo hints = {
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_flags = AI_ADDRCONFIG
    };

    char s_port[8] = {0};
    snprintf(s_port, sizeof(s_port), "%u", port);
    int ret = getaddrinfo(host, s_port, &hints, &result);
    if (ret) {
        return -1;
    }
    struct addrinfo *p;
    for (p = result; p != NULL; p = p->ai_next) {
        if (p->ai_family == AF_INET) {
            addr = p->ai_addr;
            break;
        }
    }

    if (addr) {
        memcpy(&nw_addr->in, addr, sizeof(struct sockaddr_in));
        nw_addr->family = nw_addr->in.sin_family;
        nw_addr->addrlen = sizeof(nw_addr->in);
        ret = 0;
    } else {
        ret = -1;
    }
    freeaddrinfo(result);

    return ret;
}

static void send_hand_shake(ws_clt *clt)
{
    struct ws_svr_info *svr_info = clt->svr_info;
    uint8_t nonce_key[16] = {0};
    if (ws_get_nonce_key(nonce_key, 16) < 0 ) {
        if (clt->type.on_error) {
            clt->type.on_error(&clt->raw_clt->ses, "ws get nonce key error");
        }
        return;
    }
    if (clt->svr_info->key) {
        sdsfree(clt->svr_info->key);
    }
    http_request_t *request = svr_info->request;
    base64_encode(nonce_key, 16, &clt->svr_info->key);
    sds message = ws_handshake_request(request, clt->svr_info->key);
    nw_ses_send(&clt->raw_clt->ses, message, sdslen(message));
    sdsfree(message);
}

static void send_ping_message(nw_ses *ses)
{
    ws_send_message(ses, WS_PING_OPCODE, NULL, 0, WS_FRAME_MASKED);
}

static void on_timer(nw_timer *timer, void *privdata)
{
    struct ws_clt *clt = privdata;
    if (!nw_clt_connected(clt->raw_clt))
        return;

    double now = current_timestamp();
    if (clt->last_heartbeat + clt->heartbeat_timeout < now) {
        log_error("peer: %s: heartbeat timeout", nw_sock_human_addr(&clt->raw_clt->ses.peer_addr));
        nw_clt_close(clt->raw_clt);
        nw_clt_start(clt->raw_clt);
    } else {
        if (clt->upgrade) {
            send_ping_message(&clt->raw_clt->ses);
        } else {
            send_hand_shake(clt);
        }
    }
}

int parse_ws_frame(nw_ses *ses, void *data, size_t size)
{
    struct ws_clt *clt = ses->privdata;
    if (!clt->upgrade) {
        return size;
    }

    uint8_t *p = data;
    size_t pkg_size = 0;
    struct ws_svr_info *svr_info = clt->svr_info;
    memset(&svr_info->frame, 0, sizeof(svr_info->frame));
    svr_info->frame.fin = p[0] & 0x80;
    svr_info->frame.opcode = p[0] & 0x0f;
    if (!is_good_opcode(svr_info->frame.opcode))
        return -1;

    uint8_t mask = p[1] & WS_FRAME_MASKED;
    uint8_t len = p[1] & 0x7f;
    if (len < 126) {
        pkg_size = 2;
        svr_info->frame.payload_len = len;
    } else if (len == 126) {
        pkg_size = 2 + 2;
        if (size < pkg_size)
            return 0;
        svr_info->frame.payload_len = be16toh(*(uint16_t *)(p + 2));
    } else if (len == 127) {
        pkg_size = 2 + 8;
        if (size < pkg_size)
            return 0;
        svr_info->frame.payload_len = be64toh(*(uint64_t *)(p + 2));
    }

    uint8_t masks[4];
    if (mask == WS_FRAME_MASKED) {
        memcpy(masks, p + pkg_size, sizeof(masks));
        pkg_size += sizeof(masks);
    }
    svr_info->frame.payload = p + pkg_size;
    pkg_size += svr_info->frame.payload_len;
    if (size < pkg_size)
        return 0;

    p = svr_info->frame.payload;
    if (mask == WS_FRAME_MASKED) {
        for (size_t i = 0; i < svr_info->frame.payload_len; ++i) {
            p[i] = p[i] ^ masks[i & 3];
        }
    }

    return pkg_size;
}

static void on_pong_message(nw_ses *ses)
{
    ws_clt *clt = ses->privdata;
    clt->last_heartbeat = current_timestamp();
}

int decode_pkg(nw_ses *ses, void *data, size_t size)
{
    return parse_ws_frame(ses, data, size);
}

static int parse_http_data(nw_ses *ses, void *data, size_t size)
{
    struct ws_clt *clt = ses->privdata;
    struct ws_svr_info *svr_info = clt->svr_info;
    size_t nparsed = http_parser_execute(&svr_info->parser, &clt->settings, data, size);
    if (!svr_info->parser.upgrade && nparsed != size) {
        log_error("ws server: %s http parse error: %s (%s)", nw_sock_human_addr(&ses->peer_addr),
                http_errno_description(HTTP_PARSER_ERRNO(&svr_info->parser)),
                http_errno_name(HTTP_PARSER_ERRNO(&svr_info->parser)));
        nw_clt_close(clt->raw_clt);
    }
    return nparsed;
}

static void on_recv_pkg(nw_ses *ses, void *data, size_t size)
{
    struct ws_clt *clt = ses->privdata;
    clt->last_heartbeat = current_timestamp();
    struct ws_svr_info *svr_info = clt->svr_info;
    if (!clt->upgrade) {
        parse_http_data(ses, data, size);
        return;
    }

    switch (svr_info->frame.opcode) {
    case WS_CLOSE_OPCODE:
        if (clt->type.on_close) {
            clt->type.on_close(ses);
        }
        nw_clt_close(clt->raw_clt);
        return;

    case WS_PONG_OPCODE:
        on_pong_message(ses);
        return;

    case WS_PING_OPCODE:
        return;
    }

    if (svr_info->message == NULL)
        svr_info->message = sdsempty();

    svr_info->message = sdscatlen(svr_info->message, svr_info->frame.payload, svr_info->frame.payload_len);
    if (svr_info->frame.fin) {
        int ret = clt->type.on_message(ses, svr_info->message, sdslen(svr_info->message));
        if (ret < 0) {
            nw_clt_close(clt->raw_clt);
        } else {
            sdsfree(svr_info->message);
            svr_info->message = NULL;
            memset(&svr_info->frame, 0, sizeof(svr_info->frame));
        }
    }
}

static void on_error_msg(nw_ses *ses, const char *msg)
{
    log_error("peer: %s: %s", nw_sock_human_addr(&ses->peer_addr), msg);
    ws_clt *clt = ses->privdata;
    if(clt->type.on_error) {
        clt->type.on_error(ses, msg);
    }
}

static void on_connect(nw_ses *ses, bool result)
{
    struct ws_clt *clt = ses->privdata;
    if (result) {
        clt->last_heartbeat = current_timestamp();
        if (!clt->upgrade) {
            http_parser_init(&clt->svr_info->parser, HTTP_RESPONSE);
            clt->svr_info->parser.data = clt;
            send_hand_shake(clt);
        }
    } else {
        on_error_msg(ses, "connect ws server fail");
    }
}

static int on_close(nw_ses *ses)
{
    ws_clt *clt = ses->privdata;
    log_info("connection %s -> %s close", clt->name, nw_sock_human_addr(&ses->peer_addr));
    if(clt->type.on_close) {
        clt->type.on_close(ses);
    }
    clt->upgrade = false;
    return 0;
}

int parse_ws_url(char *url, char *host, int host_len, int *port, char path[256], bool *ssl)
{
    if (!url)
        return -1;

    if (!strncmp(url, "ws://", 5)) {
        *ssl = false;
        url += 5;
        *port = 80;
    } else if (!strncmp(url, "wss://", 6)) {
        *ssl = true;
        url += 6;
        *port = 443;
    } else {
        return -1;
    }

    int len = 0;
    char *host_pos = url;

    char *p = strchr(url, ':');
    if (p) {
        len = p - url;
        url = p + 1;
        *port = atoi(url);
    }

    p = strchr(url, '/');
    if (p) {
        strncpy(path, p, 256);
        if(len == 0){
            len = p - host_pos;
        }
    }

    if (len == 0)
        len = strlen(host_pos);

    if (len > host_len - 1)
        len = host_len - 1;

    memcpy(host, host_pos, len);

    return 0;
}

static int on_http_message_begin(http_parser *parser) 
{
    struct ws_clt *clt = parser->data;
    if (clt->svr_info->response) {
        http_response_release(clt->svr_info->response);
    }
    clt->svr_info->response = http_response_new();
    if (clt->svr_info->response == NULL) {
        return -__LINE__;
    }
    return 0;
}

static int on_http_message_complete(http_parser *parser) 
{
    struct ws_clt *clt = parser->data;
    int status_code = parser->status_code;
    int ret = 0;
    if (status_code != 101) {
        goto error;
    }
    http_response_t *response = clt->svr_info->response;
    const char *upgrade = http_response_get_header(response, "Upgrade");
    if (upgrade == NULL || strcasecmp(upgrade, "websocket") != 0) {
        goto error;
    }

    const char *connection = http_response_get_header(response, "Connection");
    if (connection == NULL || strcasecmp(connection, "Upgrade") != 0) {
        goto error;
    }

    const char *sec_key = http_response_get_header(response, "Sec-WebSocket-Accept");
    sds s_key = sdsnew(sec_key);
    if (!ws_check_sec_key(clt->svr_info->key, s_key)) {
        sdsfree(s_key);
        goto error;
    }
    sdsfree(s_key);

    clt->upgrade = true;
    if (clt->type.on_open) {
        clt->type.on_open(&clt->raw_clt->ses);
    }
    return 0;

error:
    if (clt->type.on_error) {
        clt->type.on_error(&clt->raw_clt->ses, "on http message complete error");
    }

    if (clt->type.on_close) {
        clt->type.on_close(&clt->raw_clt->ses);
    }
    return ret;
}

static int on_http_header_field(http_parser *parser, const char *at, size_t length)
{
    struct ws_clt *clt = parser->data;
    clt->svr_info->field_set = true;
    if (clt->svr_info->field == NULL) {
        clt->svr_info->field = sdsnewlen(at, length);
    } else {
        clt->svr_info->field = sdscpylen(clt->svr_info->field, at, length);
    }

    return 0;
}

static int on_http_header_value(http_parser *parser, const char *at, size_t length)
{
    struct ws_clt *clt = parser->data;
    clt->svr_info->value_set = true;
    if (clt->svr_info->value == NULL) {
        clt->svr_info->value = sdsnewlen(at, length);
    } else {
        clt->svr_info->value = sdscpylen(clt->svr_info->value, at, length);
    }

    if (clt->svr_info->field_set && clt->svr_info->value_set) {
        http_response_set_header(clt->svr_info->response, clt->svr_info->field, clt->svr_info->value);
        clt->svr_info->field_set = false;
        clt->svr_info->value_set = false;
    }

    return 0;
}

static int on_http_body(http_parser *parser, const char *at, size_t length)
{
    struct ws_clt *clt = parser->data;
    clt->svr_info->response->content = sdsnewlen(at, length);
    clt->svr_info->response->content_size = length;
    return 0;
}

ws_clt *ws_clt_create(ws_clt_cfg *cfg, ws_clt_type *type)
{
    if (cfg->url == NULL) {
        return NULL;
    }

    if (cfg->name == NULL) {
        return NULL;
    }

    if (type->on_message == NULL) {
        return NULL;
    }

    int host_len = strlen(cfg->url) + 1;
    char *host = malloc(sizeof(char) * host_len);
    char path[256] = "/";
    memset(host, 0, sizeof(char) * host_len);
    int port = 0;
    bool is_ssl = false;
    if (parse_ws_url(cfg->url, host, host_len, &port, path, &is_ssl) < 0) {
        return NULL;
    }
    nw_addr_t *addr_arr = malloc(sizeof(nw_addr_t));
    assert(addr_arr != NULL);
    if (get_network_addr(host, port, addr_arr) != 0) {
        return NULL;
    }

    ws_svr_info *svr_info = malloc(sizeof(struct ws_svr_info));
    assert(svr_info != NULL);
    memset(svr_info, 0, sizeof(struct ws_svr_info));
    svr_info->url = strdup(cfg->url);
    svr_info->path = strdup(path);
    svr_info->port = port;
    svr_info->request = ws_handshake_request_new(HTTP_GET, host, path, cfg->header, cfg->body);
    assert(svr_info->request != NULL);

    nw_clt_cfg raw_cfg;
    memset(&raw_cfg, 0, sizeof(raw_cfg));
    memcpy(&raw_cfg.addr, addr_arr, sizeof(nw_addr_t));
    raw_cfg.sock_type = SOCK_STREAM;
    raw_cfg.buf_limit = cfg->buf_limit;
    raw_cfg.read_mem = cfg->read_mem;
    raw_cfg.write_mem = cfg->write_mem;
    raw_cfg.reconnect_timeout = cfg->reconnect_timeout;
    raw_cfg.max_pkg_size = cfg->max_pkg_size;
    raw_cfg.is_ssl = is_ssl;

    nw_clt_type raw_type;
    memset(&raw_type, 0, sizeof(raw_type));
    raw_type.decode_pkg = decode_pkg;
    raw_type.on_connect = on_connect;
    raw_type.on_close = on_close;
    raw_type.on_recv_pkg = on_recv_pkg;
    raw_type.on_error_msg = on_error_msg;

    ws_clt *clt = malloc(sizeof(struct ws_clt));
    assert(clt != NULL);
    memset(clt, 0, sizeof(struct ws_clt));

    clt->name = strdup(cfg->name);
    clt->heartbeat_timeout = cfg->heartbeat_timeout;
    clt->settings.on_message_begin = on_http_message_begin;
    clt->settings.on_header_field = on_http_header_field;
    clt->settings.on_header_value = on_http_header_value;
    clt->settings.on_body = on_http_body;
    clt->settings.on_message_complete = on_http_message_complete;
    clt->svr_info = svr_info;
    clt->upgrade = false;

    memcpy(&clt->type, type, sizeof(ws_clt_type));
    clt->raw_clt = nw_clt_create(&raw_cfg, &raw_type, clt);
    if (clt->raw_clt == NULL) {
        free(clt);
        return NULL;
    }

    assert(clt->name != NULL);
    clt->addr_count = 1;
    clt->addr_arr = addr_arr;
    nw_timer_set(&clt->timer, clt->heartbeat_timeout, true, on_timer, clt);

    return clt;
}

int ws_clt_start(ws_clt *clt)
{
    int ret = nw_clt_start(clt->raw_clt);
    if (ret < 0) {
        return ret;
    }
    nw_timer_start(&clt->timer);
    return 0;
}

int ws_clt_close(ws_clt *clt)
{
    int ret = nw_clt_close(clt->raw_clt);
    if (ret < 0) {
        return ret;
    }
    nw_timer_stop(&clt->timer);
    return 0;
}

void ws_clt_release(ws_clt *clt)
{
    nw_clt_release(clt->raw_clt);
    nw_timer_stop(&clt->timer);

    if (clt->svr_info) {
        if (clt->svr_info->request) {
            http_request_release(clt->svr_info->request);
        }

        if(clt->svr_info->response) {
            http_response_release(clt->svr_info->response);
        }
        sdsfree(clt->svr_info->field);
        sdsfree(clt->svr_info->value);
        sdsfree(clt->svr_info->key);
        sdsfree(clt->svr_info->message);
        free(clt->svr_info->url);
        free(clt->svr_info->path);
        free(clt->svr_info);
    }

    free(clt->name);
    free(clt);
}

bool ws_clt_connected(ws_clt *clt)
{
    return nw_clt_connected(clt->raw_clt);
}
