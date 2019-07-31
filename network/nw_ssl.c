# include <stdio.h>
# include <stdbool.h>
# include <string.h>
# include <openssl/err.h>
# include "nw_ssl.h"

static int nw_ssl_get_status(nw_ssl_ctx *ctx, int ret)
{
    int err = SSL_get_error(ctx->ssl, ret); 
    if (err == SSL_ERROR_NONE) {
        return NW_SSL_OK;
    }
    if (err == SSL_ERROR_WANT_READ) {
        return NW_SSL_WANT_READ;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
        return NW_SSL_WANT_WRITE;
    }

    return NW_SSL_FAIL;
}

int nw_ssl_connect(nw_ssl_ctx *ctx)
{
    int ret = SSL_connect(ctx->ssl);
    if (ret != 1) {
        if (ret == -1) {
            int err = SSL_get_error(ctx->ssl, ret);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                ctx->is_connected = false;
                return 0;
            }
        } 
    } else {
        ctx->is_connected = true;
        return 0;
    }
    return -1;
}

nw_ssl_ctx* nw_ssl_create(int sockfd)
{
    nw_ssl_ctx *ctx = malloc(sizeof(nw_ssl_ctx));
    if (!ctx) {
        return NULL;
    }

    memset(ctx, 0, sizeof(nw_ssl_ctx));
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
    const SSL_METHOD *method = SSLv23_client_method();
    if (!method) {
        free(ctx);
        return NULL;
    }

    ctx->ctx = SSL_CTX_new(method);
    if (!ctx->ctx) {
        free(ctx);
        return NULL;
    }

    SSL_CTX_set_verify(ctx->ctx, SSL_VERIFY_NONE, NULL);
    ctx->ssl = SSL_new(ctx->ctx);
    if(!ctx->ssl) {
        free(ctx->ctx);
        free(ctx);
        return NULL;
    }
    ctx->is_connected = false;
    SSL_set_connect_state(ctx->ssl);
    SSL_set_fd(ctx->ssl, sockfd);

    return ctx;
}

ssize_t nw_ssl_write(nw_ssl_ctx *ctx, const void *buf, size_t count)
{
    if (!ctx->is_connected) {
        return -1;
    }
    int ret = SSL_write(ctx->ssl, buf, count);
    if (ret < 0) {
        int err = nw_ssl_get_status(ctx, ret);
        if (err == SSL_ERROR_NONE || err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            return 0;
        }
    }
    return ret;
}

ssize_t nw_ssl_read(nw_ssl_ctx *ctx, void *buf, size_t count)
{
    if (!ctx->is_connected) {
        return -1;
    }
    int ret = SSL_read(ctx->ssl, buf, count);
    if (ret < 0) {
        int err = nw_ssl_get_status(ctx, ret);
        if (err == SSL_ERROR_NONE || err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            return 0;
        }
    }
    return ret;
}

void nw_ssl_free(nw_ssl_ctx *ctx)
{
    if (!ctx) {
        return;
    }

    SSL_shutdown(ctx->ssl);
    SSL_CTX_free(ctx->ctx);
    SSL_free(ctx->ssl);
    free(ctx);
}

