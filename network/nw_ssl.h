# ifndef _NW_SSL_H
# define _NW_SSL_H

# include <openssl/ssl.h>

typedef struct nw_ssl_ctx {
    SSL *ssl;
    SSL_CTX *ctx;
    bool is_connected;
} nw_ssl_ctx;

# define NW_SSL_OK 0x0
# define NW_SSL_WANT_READ 0x01
# define NW_SSL_WANT_WRITE 0x02
# define NW_SSL_FAIL 0xff

nw_ssl_ctx* nw_ssl_create(int sockfd);
ssize_t nw_ssl_write(nw_ssl_ctx *ctx, const void *buf, size_t count);
ssize_t nw_ssl_read(nw_ssl_ctx *ctx, void *buf, size_t count);
int nw_ssl_connect(nw_ssl_ctx *ctx);
void nw_ssl_free(nw_ssl_ctx *ssl);

# endif