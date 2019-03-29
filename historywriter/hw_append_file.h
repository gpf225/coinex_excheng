/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# ifndef _HW_APPEND_FILE_H_
# define _HW_APPEND_FILE_H_

# include "hw_config.h"

# define APPEND_FILE_BUFF_SIZE  65536

typedef struct append_file_t {
    int fd;
    char *path;
    char buf[APPEND_FILE_BUFF_SIZE];
    size_t pos;
} append_file_t;

append_file_t* append_file_create(const char *filepath);
int append_file_release(append_file_t *file);
int append_file_append(append_file_t *file, const char *data, size_t len);
int append_file_flush(append_file_t *file);

# endif