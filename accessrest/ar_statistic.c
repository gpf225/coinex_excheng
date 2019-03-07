/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/03/06, create
 */

# include "ar_statistic.h"
# include "ar_config.h"

# define QPS_TIME_INTERVAL 5


typedef struct depth_stat_t{
    uint64_t count_req_total;
    uint64_t count_cache_hit;
    uint64_t count_update_wait;
    uint64_t count_update;
    uint64_t count_update_timeout;
    uint64_t count_wait_released;
    uint32_t qps_max;
    uint32_t qps_now;
    uint32_t now;
}depth_stat_t;

static depth_stat_t depth_stat = {0, 0, 0, 0, 0, 0, 0, 0};
static nw_timer timer;

void stat_depth_req(void)
{
    if (!settings.debug) {
        return ;
    }
    ++depth_stat.count_req_total;
    time_t now = time(NULL);
    if (now - depth_stat.now < QPS_TIME_INTERVAL) {
        ++depth_stat.qps_now;
        return ;
    }

    if (depth_stat.qps_now > depth_stat.qps_max) {
        depth_stat.qps_max = depth_stat.qps_now;
    }
    depth_stat.now = now;
    depth_stat.qps_now = 1;
}

void stat_depth_cached(void)
{
    if (!settings.debug) {
        return ;
    }
    ++depth_stat.count_cache_hit;
}

void stat_depth_update_wait(void)
{
    if (!settings.debug) {
        return ;
    }
    ++depth_stat.count_update_wait;
}

void stat_depth_update(void)
{
    if (!settings.debug) {
        return ;
    }
    ++depth_stat.count_update;
}

void stat_depth_update_timeout(void)
{
    if (!settings.debug) {
        return ;
    }
    ++depth_stat.count_update_timeout;
}

void stat_depth_update_released(void)
{
    if (!settings.debug) {
        return ;
    }
    ++depth_stat.count_wait_released;
}

static void print_depth_stat(void)
{
    log_info("depth req: total:%"PRIu64", cache_hit:%"PRIu64, depth_stat.count_req_total, depth_stat.count_cache_hit);
    log_info("depth update: wait:%"PRIu64", update:%"PRIu64", timeout:%"PRIu64", released:%"PRIu64, 
        depth_stat.count_update_wait, depth_stat.count_update, depth_stat.count_update_timeout, depth_stat.count_wait_released);
    log_info("qps_max:%u qps_now:%u", depth_stat.qps_max, depth_stat.qps_now);
}

static void on_statistic(nw_timer *timer, void *privdata)
{
    print_depth_stat();
}

int init_statistic(void)
{
    if (settings.debug) {
        nw_timer_set(&timer, 60.0, true, on_statistic, NULL);
        nw_timer_start(&timer);
    }
    return 0;
}