/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_append_file.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

append_file_t* append_file_create(const char *filepath)
{
    int fd = open(filepath, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        log_error("open [%s] failed.", filepath);
        return NULL;
    }

    append_file_t *file = (append_file_t *)malloc(sizeof(append_file_t));
    if (file == NULL) {
        log_fatal("could not allocate memory");
        return NULL;
    }
    memset(file, 0, sizeof(append_file_t));
    file->fd = fd;
    file->path = strdup(filepath);
    file->pos = 0;

    return file;
}

int append_file_release(append_file_t *file)
{
    if (file->fd >= 0) {
        close(file->fd);
    }
    if (file->path) {
        free(file->path);
    }
    free(file);
    return 0;
}

static int write_raw(append_file_t *file, const char* p, size_t n) 
{
    while (n > 0) {
        ssize_t r = write(file->fd, p, n);
        if (r < 0) {
            if (EINTR == errno) {
                continue;
            }
            log_error("write to file[%s] error:%s", file->path, strerror(errno));
            return -__LINE__;
        }
        p += r;
        n -= r;
    }  // ~while

    return 0;
}

int append_file_flush(append_file_t *file)
{
    return write_raw(file, file->buf, file->pos);
}   

int append_file_append(append_file_t *file, const char *data, size_t len)
{
    if (data == NULL || len == 0) {
        return 0;
    }
    
    size_t n = len;
    const char *p = data;

    // Fit as much as possible into buffer.
    size_t copy = n < APPEND_FILE_BUFF_SIZE - file->pos ? n : APPEND_FILE_BUFF_SIZE - file->pos;
    memcpy(file->buf + file->pos, p, copy);
    p += copy;
    n -= copy;
    file->pos += copy;
    if (0 == n) {
        return 0;
    }

    // Can't fit in buffer, so need to do at least one write.
    int ret = append_file_flush(file);
    if (ret != 0) {
        return ret;
    }

    // Small writes go to buffer, large writes are written directly.
    if (n < APPEND_FILE_BUFF_SIZE) {
        memcpy(file->buf, p, n);
        file->pos = n;
        return 0;
    }
    return write_raw(file, p, n);
}