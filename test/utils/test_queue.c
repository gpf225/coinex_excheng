/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/21, create
 */

# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <error.h>
# include <errno.h>
# include <unistd.h>
# include "ut_title.h"
# include "ut_signal.h"
# include "ut_misc.h"
# include "ut_queue.h"
# include "nw_timer.h"
# include "ut_cli.h"

static nw_timer cron_timer;

# define READER_WORKER_NUM   4
# define QUEUE_SHMKEY_START  0x273833
# define QUEUE_MEM_SIZE      1000

struct reader_info
{
    nw_timer timer;
    queue_t  queue;
    int      id;
};

int reader_id;
queue_t *queue_writers;

static int init_process(void)
{
    if (set_file_limit(1000) < 0) {
        return -__LINE__;
    }    
    
    if (set_core_limit(1000000000) < 0) {
        return -__LINE__;
    }

    return 0;
}

int init_log(void)
{
    default_dlog = dlog_init("test_queue", DLOG_SHIFT_BY_DAY, 100*1024*1024, 10, 7);
    if (default_dlog == NULL)
        return -__LINE__;
    default_dlog_flag = dlog_read_flag("fatal,error,warn,info,debug,trace");

    return 0;
}

static void on_cron_check(nw_timer *timer, void *data)
{
    if (signal_exit) {
        nw_loop_break();
        signal_exit = 0;
    }
}

static sds on_cmd_push(const char *cmd, int argc, sds *argv)
{
    if (argc != 1) {
        sds reply = sdsempty();
        return sdscatprintf(reply, "usage: %s \"msg\"\n", cmd);
    }
    for (int i = 0; i < READER_WORKER_NUM; ++i) {
        queue_push(&queue_writers[i], argv[0], sdslen(argv[0]));
    }
    return sdsnew("ok\n");
}

static int init_writer_cli(void)
{
    char *cli_cfg_str = "tcp@127.0.0.1:8001";
    cli_svr_cfg cli_cfg;
    nw_sock_cfg_parse(cli_cfg_str, &cli_cfg.addr, &cli_cfg.sock_type);
    cli_svr *svr = cli_svr_create(&cli_cfg);
    if (svr == NULL) {
        return -__LINE__;
    }

    cli_svr_add_cmd(svr, "push", on_cmd_push);

    return 0;
}

static void on_message(void *data, uint32_t size)
{
    char *data_s = (char *)malloc(size + 1);
    memset(data_s, 0, size + 1);
    memcpy(data_s, data, size);
    log_info("reader: %d, read from queue: %s, size: %d", reader_id, data_s, size);
    free(data_s);
}

int init_writer()
{
    queue_writers = (queue_t *)malloc(sizeof(queue_t) * READER_WORKER_NUM);
    memset(queue_writers, 0, sizeof(queue_t) * READER_WORKER_NUM);

    for (int i = 0; i < READER_WORKER_NUM; ++i) {
        char queue_name[100] = {0};
        sprintf(queue_name, "queuetest_%d", i);

        char queue_pipe_path[100] = {0};
        sprintf(queue_pipe_path, "/tmp/queue_pipe_%d", i);

        key_t queue_shm_key = QUEUE_SHMKEY_START + i;

        int ret = queue_writer_init(&queue_writers[i], NULL, queue_name, queue_pipe_path, queue_shm_key, QUEUE_MEM_SIZE);
        if (ret < 0) {
            log_error("queue_writer_init %d error: %d", i, ret);
            return ret;
        }
    }
    init_writer_cli();
}

static void on_reader_timer(nw_timer *timer, void *data)
{
    struct reader_info *info = data;
    log_info("reader: %d, timer time out", info->id);
}

int init_reader(int id)
{
    queue_type type;
    memset(&type, 0, sizeof(type));
    type.on_message  = on_message;

    struct reader_info *info = malloc(sizeof(struct reader_info));
    memset(info, 0, sizeof(struct reader_info));

    char queue_name[100] = {0};
    sprintf(queue_name, "queuetest_%d", id);

    char queue_pipe_path[100] = {0};
    sprintf(queue_pipe_path, "/tmp/queue_pipe_%d", id);

    key_t queue_shm_key = QUEUE_SHMKEY_START + id;

    info->id = id;
    reader_id = id;
    int ret = queue_reader_init(&info->queue, &type, queue_name, queue_pipe_path, queue_shm_key, QUEUE_MEM_SIZE);
    if (ret < 0) {
        log_error("queue_writer_init error: %d", ret);
        return ret;
    }

    nw_timer_set(&info->timer, 0.5, true, on_reader_timer, info);
    nw_timer_start(&info->timer);
    return 0;
}

int main(int argc, char *argv[])
{
    int ret = init_process();
    if (ret < 0) {
        printf("init_process fail: %d\n", ret);
        exit(1);
    }

    ret = init_log();
    if (ret < 0) {
        printf("init_log fail: %d\n", ret);
        exit(1);
    }

    for (int i = 0; i < READER_WORKER_NUM; ++i) {
        int pid = fork();
        if (pid < 0) {
            error(EXIT_FAILURE, errno, "fork error");
        } else if (pid != 0) {
            process_title_set("me_reader_%d", i);
            daemon(1, 1);
            process_keepalive();

            ret = init_reader(i);
            if (ret < 0) {
                error(EXIT_FAILURE, errno, "init reader fail: %d", ret);
            }

            goto run;
        }
    }

    process_title_set("me_writer");
    daemon(1, 1);
    process_keepalive();
    ret = init_writer();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init writer fail: %d", ret);
    }
run:
    nw_timer_set(&cron_timer, 0.5, true, on_cron_check, NULL);
    nw_timer_start(&cron_timer);

    nw_loop_run();
    log_info("server stop");

    return 0;
}

