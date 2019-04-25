/*
 * Description: A variable length circular queue, support single process or
 *              thread write and single process or thread read.
 *     History: yangxiaoqaing@viabtc.com, 2019/04/025, update
 */

# include <stdio.h>
# include <string.h>
# include <stdlib.h>
# include <stdint.h>
# include <stdbool.h>
# include <assert.h>
# include <limits.h>
# include <errno.h>
# include <unistd.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <sys/ipc.h>
# include <sys/shm.h>
# include <fcntl.h>

# include "ut_queue.h"
# include "nw_sock.h"

# define MAX_NAME_LEN 128
# define MAGIC_NUM    20190424

# pragma pack(1)

struct queue_head
{
    uint32_t magic;
    char     name[128];

    uint64_t shm_key;
    uint32_t mem_size;
    uint32_t mem_use;
    uint32_t mem_num;
    uint32_t p_head;
    uint32_t p_tail;
};

# pragma pack()

static void *get_shm_inner(key_t key, size_t size, int flag)
{
    int shm_id = shmget(key, size, flag);
    if (shm_id < 0)
        return NULL;

    void *p = shmat(shm_id, NULL, 0);
    if (p == (void *)-1)
        return NULL;

    return p;
}

static int get_shm(key_t key, size_t size, void **addr)
{
    if ((*addr = get_shm_inner(key, size, 0666)) != NULL)
        return 0;
    if ((*addr = get_shm_inner(key, size, 0666 | IPC_CREAT)) != NULL)
        return 1;

    return -__LINE__;
}

static void *alloc_read_buf(queue_t *queue, uint32_t size)
{
    if (queue->read_buf == NULL || queue->read_buf_size < size) {
        void  *buf = queue->read_buf;
        size_t buf_size = queue->read_buf_size;

        if (buf == NULL)
            buf_size = 1;
        while (buf_size < size)
            buf_size *= 2;
        buf = realloc(buf, buf_size);
        if (buf == NULL)
            return NULL;

        queue->read_buf = buf;
        queue->read_buf_size = buf_size;
    }

    return queue->read_buf;
}

static void putmem(queue_t *queue, uint32_t *p_tail, void *data, uint32_t size)
{
    volatile struct queue_head *head = queue->memory;
    void *buf = queue->memory + sizeof(struct queue_head);

    uint32_t tail_left = head->mem_size - *p_tail;
    if (tail_left < size) {
        memcpy(buf + *p_tail, data, tail_left);
        *p_tail = size - tail_left;
        memcpy(buf, data + tail_left, *p_tail);
    } else {
        memcpy(buf + *p_tail, data, size);
        *p_tail += size;
    }
}

static void getmem(queue_t *queue, uint32_t *p_head, void *data, uint32_t size)
{
    volatile struct queue_head *head = queue->memory;
    void *buf = queue->memory + sizeof(struct queue_head);

    uint32_t tail_left = head->mem_size - *p_head;

    if (tail_left < size) {
        memcpy(data, buf + *p_head, tail_left);
        *p_head = size - tail_left;
        memcpy(data + tail_left, buf, *p_head);
    } else {
        memcpy(data, buf + *p_head, size);
        *p_head += size;
    }
}

static int check_mem(queue_t *queue, size_t size)
{
    volatile struct queue_head *head = queue->memory;
    if (head->mem_use < size) {
        head->mem_use = 0;
        head->mem_num = 0;
        head->p_head = head->p_tail = 0;
        return -__LINE__;
    }

    return 0;
}

static int queue_init(queue_t *queue, char *name, key_t shm_key, uint32_t mem_size, bool reset)
{
    size_t real_mem_size = sizeof(struct queue_head) + mem_size;
    void *memory = NULL;
    bool old_shm = false;

    int ret = get_shm(shm_key, real_mem_size, &memory);
    if (ret < 0)
        return -__LINE__;
    else if (ret == 0)
        old_shm = true;

    volatile struct queue_head *head = memory;
    if (old_shm) {
        if (name && strcmp((char *)head->name, name) != 0) {
            return -__LINE__;
        }
    } else {
        strcpy((char *)head->name, name);
        head->magic = MAGIC_NUM;
        head->shm_key  = shm_key;
        head->mem_size = mem_size;
    }

    if (reset) {
        head->mem_use = 0;
        head->mem_num = 0;
        head->p_head  = 0;
        head->p_tail  = 0;
    }

    memset(queue, 0, sizeof(*queue));
    queue->memory = memory;

    return 0;
}

static void queue_can_read(struct ev_loop *loop, ev_io *watcher, int events)
{
    queue_t *queue = (queue_t *)watcher;
    char buffer[PIPE_BUF + 1];
    for (;;) {
        int ret = read(queue->pipe_fd, buffer, PIPE_BUF);
        if (ret < 0)
            break;
    }

    for (;;) {
        void *data;
        uint32_t size;
        int ret = queue_pop(queue, &data, &size);
        if (ret < 0) {
            return;
        }
        
        if (queue->type.on_message)
            queue->type.on_message(data, size);
    }
}

int queue_reader_init(queue_t *queue, queue_type *type, char *name, char *pipe_path, key_t shm_key, uint32_t mem_size)
{
    if (!queue || !pipe_path || !name || strlen(name) > MAX_NAME_LEN || !mem_size || !shm_key)
        return -__LINE__;

    int ret = queue_init(queue, name, shm_key, mem_size, false);
    if (ret < 0) {
        return ret;
    }

    if (type == NULL) {
        return -__LINE__;
    }
    queue->type = *type;

    if (access(pipe_path, F_OK) == -1) {
        if (mkfifo(pipe_path, 0777) != 0) {
            return -__LINE__;
        }
    }

    int pipe_fd = open(pipe_path, O_RDONLY);
    if (pipe_fd == -1) {
        return -__LINE__;
    }

    nw_loop_init();
    queue->pipe_path = strdup(pipe_path);
    queue->pipe_fd = pipe_fd;
    queue->loop = nw_default_loop;
    nw_sock_set_nonblock(queue->pipe_fd);
    ev_io_init(&queue->ev, queue_can_read, queue->pipe_fd, EV_READ);
    ev_io_start(queue->loop, &queue->ev);
    return 0;
}

int queue_writer_init(queue_t *queue, queue_type *type, char *name, char *pipe_path, key_t shm_key, uint32_t mem_size)
{
    if (!queue || !pipe_path || !name || strlen(name) > MAX_NAME_LEN || !mem_size || !shm_key)
        return -__LINE__;    

    int ret = queue_init(queue, name, shm_key, mem_size, true);
    if (ret < 0) {
        return ret;
    }

    if (type != NULL) {
        queue->type = *type;
    }
    
    if(access(pipe_path, F_OK)==-1) {
        if (mkfifo(pipe_path, 0777) != 0) {
            return -__LINE__;
        }
    }
    
    int pipe_fd = open(pipe_path, O_WRONLY);
    if (pipe_fd == -1) {
        return -__LINE__;
    }

    queue->pipe_path = strdup(pipe_path);
    queue->pipe_fd = pipe_fd;
    nw_sock_set_nonblock(queue->pipe_fd);
    return 0;
}

int queue_push(queue_t *queue, void *data, uint32_t size)
{
    if (!queue || !data)
        return -__LINE__;

    volatile struct queue_head *head = queue->memory;
    assert(head->magic == MAGIC_NUM);

    if ((head->mem_size - head->mem_use) < (sizeof(size) + size)) {
        return -__LINE__;
    }

    uint32_t p_tail = head->p_tail;

    putmem(queue, &p_tail, &size, sizeof(size));
    putmem(queue, &p_tail, data, size);

    head->p_tail = p_tail;

    __sync_fetch_and_add(&head->mem_use, sizeof(size) + size);
    __sync_fetch_and_add(&head->mem_num, 1);

    write(queue->pipe_fd, " ", 1);
    return 0;
}

int queue_pop(queue_t *queue, void **data, uint32_t *size)
{
    if (!queue || !data || !size)
        return -__LINE__;

    volatile struct queue_head *head = queue->memory;
    assert(head->magic == MAGIC_NUM);
    if (head->mem_num == 0) {
        return -__LINE__;
    }

    uint32_t chunk_size = 0;
    uint32_t p_head = head->p_head;

    if (check_mem(queue, sizeof(chunk_size)) < 0)
        return -__LINE__;
    getmem(queue, &p_head, &chunk_size, sizeof(chunk_size));

    *data = alloc_read_buf(queue, chunk_size);
    if (*data == NULL)
        return -__LINE__;
    *size = chunk_size;

    if (check_mem(queue, (sizeof(chunk_size) + chunk_size)) < 0)
        return -__LINE__;
    getmem(queue, &p_head, *data, chunk_size);

    head->p_head = p_head;

    __sync_fetch_and_sub(&head->mem_use, sizeof(chunk_size) + chunk_size);
    __sync_fetch_and_sub(&head->mem_num, 1);

    return 0;
}

uint64_t queue_len(queue_t *queue)
{
    if (!queue)
        return -__LINE__;

    volatile struct queue_head *head = queue->memory;
    assert(head->magic == MAGIC_NUM);

    return head->mem_use;
}

uint64_t queue_num(queue_t *queue)
{
    if (!queue)
        return -__LINE__;

    volatile struct queue_head *head = queue->memory;
    assert(head->magic == MAGIC_NUM);

    return head->mem_num;
}

int queue_stat(queue_t *queue, uint32_t *mem_num, uint32_t *mem_size)
{
    volatile struct queue_head *head = queue->memory;
    assert(head->magic == MAGIC_NUM);

    *mem_num   = head->mem_num;
    *mem_size  = head->mem_use;
    return 0;
}

void queue_fini(queue_t *queue)
{
    if (!queue)
        return;

    volatile struct queue_head *head = queue->memory;
    assert(head->magic == MAGIC_NUM);

    if (queue->read_buf)
        free(queue->read_buf);

    if (head->shm_key)
        shmdt(queue->memory);

    if (queue->loop)
        ev_io_stop(queue->loop, &queue->ev);

    if (queue->pipe_path)
        free(queue->pipe_path);

    if (queue->pipe_fd > 0)
        close(queue->pipe_fd);

    return;
}

