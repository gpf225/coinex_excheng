# ifndef _UT_WS_H
# define _UT_WS_H
# include "nw_clt.h"
# include "ut_dict.h"
# include "ut_sds.h"
# include "ut_http.h"

# define WEBSOCKET_GUID "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
# define CONTINUATION_OPCODE  0x00
# define TEXT_OPCODE          0x01
# define BINARY_OPCODE        0x02
# define CLOSE_OPCODE         0x08
# define PING_OPCODE          0x09
# define PONG_OPCODE          0x0A

# define FRAME_MASKED 0x80

struct ws_frame {
    uint8_t     fin;
    uint8_t     opcode;
    uint64_t    payload_len;
    void        *payload;
};

bool is_good_protocol(const char *protocol_list, const char *protocol);
bool is_good_origin(const char *origin, const char *require);
bool is_good_opcode(uint8_t opcode);

int ws_send_message(nw_ses *ses, uint8_t opcode, void *payload, size_t payload_len, int masked);
int ws_get_nonce_key(uint8_t *nonce_key, int len);
int ws_generate_sec_key(sds base64_nonce_key, sds *base64_sec_key);
bool ws_check_sec_key(sds base64_nonce_key, sds accept_key);

const char* ws_request_get_header(dict_t *headers, const char *field);
const char* ws_response_get_header();

http_request_t* ws_handshake_request_new(uint32_t method, char *host, char *path, dict_t *header, char* body);
http_response_t* ws_handshake_response_new(char *protocol, uint32_t status);

sds ws_handshake_request(http_request_t *request, char* key);
sds ws_handshake_response(http_response_t *response, const char *key);

int ws_send_text(nw_ses *ses, char *message);
int ws_send_binary(nw_ses *ses, void *payload, size_t payload_len);

int ws_send_clt_text(nw_ses *ses, char *message);
int ws_send_clt_binary(nw_ses *ses, void *payload, size_t payload_len);

# endif