/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/22, create
 */

# include "ca_depth_cache.h"
# include "ca_depth_update.h"
# include "ca_depth_wait_queue.h"
# include "ca_depth_sub.h"
# include "ca_depth_poll.h"
# include "ca_server.h"
# include "ca_statistic.h"

# include "ut_title.h"

const char *__process__ = "cache";
const char *__version__ = "0.1.0";

nw_timer cron_timer;

static void on_cron_check(nw_timer *timer, void *data)
{
    dlog_check_all();
    if (signal_exit) {
        nw_loop_break();
        signal_exit = 0;
    }
}

static int init_process(void)
{
    if (settings.process.file_limit) {
        if (set_file_limit(settings.process.file_limit) < 0) {
            return -__LINE__;
        }
    }
    if (settings.process.core_limit) {
        if (set_core_limit(settings.process.core_limit) < 0) {
            return -__LINE__;
        }
    }

    return 0;
}

static int init_log(void)
{
    default_dlog = dlog_init(settings.log.path, settings.log.shift, settings.log.max, settings.log.num, settings.log.keep);
    if (default_dlog == NULL)
        return -__LINE__;
    default_dlog_flag = dlog_read_flag(settings.log.flag);
    if (alert_init(&settings.alert) < 0)
        return -__LINE__;
    if (profile_init(__process__, settings.alert.host) < 0)
        return -__LINE__;

    return 0;
}

int main(int argc, char *argv[])
{
    printf("process: %s version: %s, compile date: %s %s\n", __process__, __version__, __DATE__, __TIME__);

    if (argc < 2) {
        printf("usage: %s config.json\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    if (process_exist(__process__) != 0) {
        printf("process: %s exist\n", __process__);
        exit(EXIT_FAILURE);
    }

    int ret;
    ret = init_mpd();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init mpd fail: %d", ret);
    }
    ret = init_config(argv[1]);
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "load config fail: %d", ret);
    }
    ret = init_process();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init process fail: %d", ret);
    }
    ret = init_log();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init log fail: %d", ret);
    }

    daemon(1, 1);
    process_keepalive1(settings.debug);

    ret = init_depth_cache(settings.cache_timeout);
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init depth cache fail: %d", ret);
    }
    ret = init_depth_update();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init depth update fail: %d", ret);
    }
    ret = init_depth_wait_queue();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init depth wait queue fail: %d", ret);
    }
    ret = init_depth_sub();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init depth sub fail: %d", ret);
    }
    ret = init_depth_poll();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init depth poll fail: %d", ret);
    }
    ret = init_statistic();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init statistic fail: %d", ret);
    }
    ret = init_server();
    if (ret < 0) {
        error(EXIT_FAILURE, errno, "init server fail: %d", ret);
    }

    nw_timer_set(&cron_timer, 0.5, true, on_cron_check, NULL);
    nw_timer_start(&cron_timer);

    log_vip("server start");
    log_stderr("server start");
    nw_loop_run();
    log_vip("server stop");

    return 0;
}