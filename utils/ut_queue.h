/*
 * Description: A variable length circular queue, support single process or
 *              thread write and single process or thread read.
 *     History: damonyang@tencent.com, 2013/06/08, create
                yangxiaoqaing@viabtc.com, 2019/04/025, update （remove file storage, add evio and named pipe）
 */

# ifndef _UT_QUEUE_H_
# define _UT_QUEUE_H_

# include <stdint.h>
# include <sys/types.h>
# include "nw_evt.h"

typedef struct
{
    void (*on_message)(void *data, uint32_t size);
} queue_type;

typedef struct
{
    ev_io           ev;
    struct ev_loop  *loop;
    void            *memory;
    void            *read_buf;
    size_t          read_buf_size;
    int             pipe_fd;
    char            *pipe_path;
    queue_type      type;
} queue_t;


int queue_reader_init(queue_t *queue, queue_type *type, char *name, char *pipe_path, key_t shm_key, uint32_t mem_size);

int queue_writer_init(queue_t *queue, queue_type *type, char *name, char *pipe_path, key_t shm_key, uint32_t mem_size);
/*
 * return:
 *      <  -1: error
 *      == -1: full
 *      ==  0: success
 */
int queue_push(queue_t *queue, void *data, uint32_t size);

/*
 * return:
 *      <  -1: error
 *      == -1: empty
 *      ==  0: success
 */
int queue_pop(queue_t *queue, void **data, uint32_t *size);

/* return queue len in byte */
uint32_t queue_len(queue_t *queue);

/* return queue unit num */
uint32_t queue_num(queue_t *queue);

/* return queue mem left */
uint32_t queue_left(queue_t *queue);

/* get queue stat */
int queue_stat(queue_t *queue, uint32_t *mem_num, uint32_t *mem_size);

/* free a queue */
void queue_fini(queue_t *queue);

# endif
