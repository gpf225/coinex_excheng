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
static nw_timer reader_timer;
static queue_t queue_writer;
static queue_t queue_reader;

# define QUEUE_SHM_KEY  0x273830

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
    queue_push(&queue_writer, argv[0], sdslen(argv[0]));
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
    log_info("read from queue: %s, size: %d", data_s, size);
    free(data_s);
}

int init_writer()
{
    int ret = queue_writer_init(&queue_writer, NULL, "queuetest", "/tmp/queue_pipe", QUEUE_SHM_KEY, 1000);
    if (ret < 0) {
        log_error("queue_writer_init error: %d", ret);
        return ret;
    }
    init_writer_cli();
}

static void on_reader_timer(nw_timer *timer, void *data)
{
    log_info("timer time out");
}

int init_reader()
{
    queue_type type;
    memset(&type, 0, sizeof(type));
    type.on_message  = on_message;
    int ret = queue_reader_init(&queue_reader, &type, "queuetest", "/tmp/queue_pipe", QUEUE_SHM_KEY, 1000);
    if (ret < 0) {
        log_error("queue_writer_init error: %d", ret);
        return ret;
    }
    nw_timer_set(&reader_timer, 0.5, true, on_reader_timer, NULL);
    nw_timer_start(&reader_timer);
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

    int pid;
    pid = fork();
    if (pid < 0) {
        error(EXIT_FAILURE, errno, "fork error");
    } else if (pid == 0) {
        process_title_set("writer");
        daemon(1, 1);
        process_keepalive();
        int ret = init_writer();
        if (ret < 0) {
            error(EXIT_FAILURE, errno, "init writer fail: %d", ret);
        }
        goto run;
    }

    process_title_set("marketprice_access");
    daemon(1, 1);
    process_keepalive();

    init_reader();
run:
    nw_timer_set(&cron_timer, 0.5, true, on_cron_check, NULL);
    nw_timer_start(&cron_timer);

    nw_loop_run();
    log_info("server stop");

    return 0;
}

