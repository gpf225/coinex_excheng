# include <stdio.h>
# include "ut_log.h"
# include <openssl/sha.h> 
# include "ut_ws.h"
# include "ut_http.h"
# include "ut_misc.h"
# include "ut_base64.h"
# include "ut_ws_svr.h"
# include "zlib.h"

/*
int ws_send_message(nw_ses *ses, uint8_t opcode, void *payload, size_t payload_len, int masked)
{
    if (payload == NULL)
        payload_len = 0;

    static void *buf;
    static size_t buf_size = 1024;
    if (buf == NULL) {
        buf = malloc(1024);
        if (buf == NULL)
            return -1;
    }

    size_t require_len = 10 + payload_len;
    if (masked == WS_FRAME_MASKED) {
        require_len += 4;
    }

    if (buf_size < require_len) {
        void *new = realloc(buf, require_len);
        if (new == NULL)
            return -1;
        buf = new;
        buf_size = require_len;
    }

    size_t pkg_len = 0;
    uint8_t *p = buf;
    p[0] = 0;
    p[0] |= 0x1 << 7;
    p[0] |= opcode;
    p[1] = 0;
    if (payload_len < 126) {
        uint8_t len = payload_len|masked;
        p[1] |= len;
        pkg_len = 2;
    } else if (payload_len <= 0xffff) {
        p[1] |= 126|masked;
        uint16_t len = htobe16((uint16_t)payload_len);
        memcpy(p + 2, &len, sizeof(len));
        pkg_len = 2 + sizeof(len);
    } else {
        p[1] |= 127|masked;
        uint64_t len = htobe64(payload_len);
        memcpy(p + 2, &len, sizeof(len));
        pkg_len = 2 + sizeof(len);
    }

    uint8_t masked_key[4] = {0};
    if (masked == WS_FRAME_MASKED) {
        if (ws_get_nonce_key(masked_key, 4) < 0) {
            return -1;
        }
        memcpy(p + pkg_len, masked_key, 4);
        pkg_len += 4;
    }

    if (payload) {
        if (masked == WS_FRAME_MASKED) {
            uint8_t *data = payload;
            for (int i = 0; i < payload_len; i++) {
                p[pkg_len + i] = data[i] ^ masked_key[i % 4];
            }
        } else {
            memcpy(p + pkg_len, payload, payload_len);
        }
        pkg_len += payload_len;
    }

    if (ses->svr) {
        ws_ses_update_activity(ses);
    }
    return nw_ses_send(ses, buf, pkg_len);
}
*/

static sds compress_test(void *payload, size_t payload_len)
{
    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;
    // setup "a" as the input and "b" as the compressed output
    defstream.avail_in = (uInt)payload_len; // size of input, string + terminator
    defstream.next_in = (Bytef *)payload; // input char array    

    sds out = sdsempty();
    deflateInit2(&defstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -MAX_WBITS, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);

    int ret = 0;
    do {
        char b[100] = {0};
        defstream.avail_out = (uInt)sizeof(b); // size of output
        defstream.next_out = (Bytef *)b; // output char array
        ret = deflate(&defstream, Z_FINISH);    /* no bad return value */
        int have = sizeof(b) - defstream.avail_out;
        out = sdscatlen(out, b, have);
        log_error("deflate ret: %d, have:%d\n", ret, have);
    } while (defstream.avail_out == 0);
    deflateEnd(&defstream);

    if (ret != Z_OK && ret != Z_STREAM_END) {
        log_trace("compress fail: %d", ret);
    }
    return out;
}

int ws_send_message(nw_ses *ses, uint8_t opcode, void *payload, size_t payload_len, int masked)
{
    if (payload == NULL)
        payload_len = 0;

    sds payload_compressed = compress_test(payload, payload_len);
    if (sdslen(payload_compressed) == 0) {
        log_error("payload: %s compress fail", (char *)payload);
        return -__LINE__;
    }
    int payload_compressed_len = sdslen(payload_compressed);
    sds payload_compressed_hex = bin2hex(payload_compressed, payload_compressed_len);
    log_trace("payload_compressed_hex: %s", payload_compressed_hex);
    sdsfree(payload_compressed_hex);

    static void *buf;
    static size_t buf_size = 1024;
    if (buf == NULL) {
        buf = malloc(1024);
        if (buf == NULL)
            return -1;
    }

    //masked = WS_FRAME_MASKED;
    size_t require_len = 10 + payload_compressed_len;
    if (masked == WS_FRAME_MASKED) {
        require_len += 4;
    }

    if (buf_size < require_len) {
        void *new = realloc(buf, require_len);
        if (new == NULL)
            return -1;
        buf = new;
        buf_size = require_len;
    }

    size_t pkg_len = 0;
    uint8_t *p = buf;
    p[0] = 0;
    p[0] |= 0x1 << 7;
    p[0] |= 0x1 << 6;
    p[0] |= opcode;
    //p[0] |= WS_BINARY_OPCODE;
    p[1] = 0;
    if (payload_compressed_len < 126) {
        uint8_t len = payload_compressed_len|masked;
        p[1] |= len;
        pkg_len = 2;
    } else if (payload_compressed_len <= 0xffff) {
        p[1] |= 126|masked;
        uint16_t len = htobe16((uint16_t)payload_compressed_len);
        memcpy(p + 2, &len, sizeof(len));
        pkg_len = 2 + sizeof(len);
    } else {
        p[1] |= 127|masked;
        uint64_t len = htobe64(payload_compressed_len);
        memcpy(p + 2, &len, sizeof(len));
        pkg_len = 2 + sizeof(len);
    }

    uint8_t masked_key[4] = {0};
    if (masked == WS_FRAME_MASKED) {
        if (ws_get_nonce_key(masked_key, 4) < 0) {
            return -1;
        }
        memcpy(p + pkg_len, masked_key, 4);
        pkg_len += 4;
    }

    if (payload_compressed_len > 0) {
        if (masked == WS_FRAME_MASKED) {
            uint8_t *data = payload_compressed;
            for (int i = 0; i < payload_compressed_len; i++) {
                p[pkg_len + i] = data[i] ^ masked_key[i % 4];
            }
        } else {
            memcpy(p + pkg_len, payload_compressed, payload_compressed_len);
        }
        pkg_len += payload_compressed_len;
    }

    if (ses->svr) {
        ws_ses_update_activity(ses);
    }

    sds buf_hex = bin2hex(buf, pkg_len);
    log_trace("send buf_hex: %s, hex: %zu", buf_hex, pkg_len);
    sdsfree(buf_hex);

    return nw_ses_send(ses, buf, pkg_len);
}

int ws_get_nonce_key(uint8_t *nonce_key, int len)
{
    return urandom(nonce_key, len);
}

int ws_generate_sec_key(sds base64_nonce_key, sds *base64_sec_key)
{
    sds encode_data = sdsnew(base64_nonce_key);
    encode_data = sdscat(encode_data, WS_GUID);
    uint8_t sha_hash[20];
    SHA1((const unsigned char *)encode_data, sdslen(encode_data), sha_hash);
    sds base64_hash;
    base64_encode(sha_hash, sizeof(sha_hash), &base64_hash);
    *base64_sec_key = sdsnew(base64_hash);
    sdsfree(base64_hash);
    sdsfree(encode_data);

    return 0;
}

bool ws_check_sec_key(sds base64_nonce_key, sds accept_key)
{
    sds sec_key;
    ws_generate_sec_key(base64_nonce_key, &sec_key);
    bool ret = false;
    if (sdscmp(sec_key, accept_key) == 0 ) {
        ret = true;
    }
    sdsfree(sec_key);

    return ret;
}

bool is_good_protocol(const char *protocol_list, const char *protocol)
{
    char *tmp = strdup(protocol_list);
    char *pch = strtok(tmp, ", ");
    while (pch != NULL) {
        if (strcmp(pch, protocol) == 0) {
            free(tmp);
            return true;
        }
        pch = strtok(NULL, ", ");
    }
    free(tmp);
    return false;
}

bool is_good_origin(const char *origin, const char *require)
{
    size_t origin_len  = strlen(origin);
    size_t require_len = strlen(require);
    if (origin_len < require_len)
        return false;

    if (memcmp(origin + (origin_len - require_len), require, require_len) != 0)
        return false;

    return true;
}

bool is_good_opcode(uint8_t opcode)
{
    static uint8_t good_list[] = {WS_CONTINUATION_OPCODE, WS_TEXT_OPCODE, WS_BINARY_OPCODE, WS_CLOSE_OPCODE, WS_PING_OPCODE, WS_PONG_OPCODE};
    for (size_t i = 0; i < sizeof(good_list); ++i) {
        if (opcode == good_list[i])
            return true;
    }
    return false;
}

sds ws_handshake_request(http_request_t *request, char *key)
{
    http_request_t *send_request = http_request_new();
    dict_iterator *iter = dict_get_iterator(request->headers);
    dict_entry *entry;
    while((entry = dict_next(iter)) != NULL) {
        http_request_set_header(send_request, (char*)entry->key, (char*)entry->val);
    }
    dict_release_iterator(iter);

    send_request->method = request->method;
    send_request->version_major = request->version_major;
    send_request->version_minor = request->version_minor;
    send_request->url = sdsdup(request->url);
    if (send_request->body) {
        send_request->body = sdsdup(request->body);
    }
    http_request_set_header(send_request, "Sec-WebSocket-Key", key);
    sds message = http_request_encode(send_request);

    http_request_release(send_request);

    return message;
}

http_request_t *ws_handshake_request_new(uint32_t method, char *host, char *path, dict_t *header, char* body)
{
    http_request_t *request = http_request_new();
    if (!request) {
        return NULL;
    }
    request->version_major = 1;
    request->version_minor = 1;
    request->method = method;
    http_request_set_header(request, "Upgrade", "websocket");
    http_request_set_header(request, "Connection", "Upgrade");
    http_request_set_header(request, "Sec-WebSocket-Version", "13");

    if (header) {
        dict_iterator *iter = dict_get_iterator(header);
        dict_entry *entry;
        while((entry = dict_next(iter)) != NULL) {
            http_request_set_header(request, (char*)entry->key, (char*)entry->val);
        }
        dict_release_iterator(iter);
    }
    
    if (host) {
        const char *host_value = http_request_get_header(request, "Host");
        if (host_value == NULL) {
            http_request_set_header(request, "Host", host);
        }
    }
    if (body) {
        request->body = sdsnew(body);
    }

    if (path) {
        request->url = sdsnew(path);
    }

    return request;
}

http_response_t* ws_handshake_response_new(char *protocol, uint32_t status)
{
    http_response_t *response = http_response_new();
    if(response == NULL) {
        return NULL;
    }

    http_response_set_header(response, "Upgrade", "websocket");
    http_response_set_header(response, "Connection", "Upgrade");
    if (protocol) {
        http_response_set_header(response, "Sec-WebSocket-Protocol", protocol);
    }
    //http_response_set_header(response, "Sec-WebSocket-Extensions", "permessage-deflate;client_max_window_bits=15");
    //http_response_set_header(response, "Sec-WebSocket-Extensions", "permessage-deflate");
    response->status = status;

    return response;
}

sds ws_handshake_response(http_response_t *response, const char *key)
{
    sds b4message;
    sds s_key = sdsnew(key);
    ws_generate_sec_key(s_key, &b4message);
    http_response_set_header(response, "Sec-WebSocket-Accept", b4message);
    //http_response_set_header(response, "Sec-WebSocket-Extensions", "permessage-deflate;client_max_window_bits=15");
    http_response_set_header(response, "Sec-WebSocket-Extensions", "permessage-deflate; server_no_context_takeover; client_no_context_takeover");
    sds message = http_response_encode(response);

    sdsfree(b4message);
    sdsfree(s_key);

    return message;
}

int ws_send_text(nw_ses *ses, char *message)
{
    return ws_send_message(ses, WS_TEXT_OPCODE, message, strlen(message), 0);
}

int ws_send_binary(nw_ses *ses, void *payload, size_t payload_len)
{
    return ws_send_message(ses, WS_BINARY_OPCODE, payload, payload_len, 0);
}

int ws_send_clt_text(nw_ses *ses, char *message)
{
    return ws_send_message(ses, WS_TEXT_OPCODE, message, strlen(message), WS_FRAME_MASKED);
}

int ws_send_clt_binary(nw_ses *ses, void *payload, size_t payload_len)
{
    return ws_send_message(ses, WS_BINARY_OPCODE, payload, payload_len, WS_FRAME_MASKED);
}