/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/02, create
 */

# include "lp_statistic.h"
# include "lp_market.h"
# include "lp_state.h"
# include "lp_depth_sub.h"

static nw_timer timer;

typedef struct market_stat_t{
    uint32_t last_poll_time;
    uint32_t last_update_time;
    uint32_t last_update_count;
}market_stat_t;

typedef struct state_stat_t{
    uint32_t last_update_time;
    uint32_t last_update_count;
}state_stat_t;

typedef struct depth_stat_t{
    uint32_t last_poll_time;
    uint32_t last_poll_count;
    uint32_t last_update_time;
}depth_stat_t;

static market_stat_t market_stat = {0, 0, 0};
static state_stat_t  state_stat = {0, 0};
static depth_stat_t  depth_stat = {0, 0, 0};

static void print_subscribers()
{
    size_t cur = time(NULL);
    log_info("current seconds:%zu", cur);
    log_info("market subscribers:%zu", market_subscribe_number());
    log_info("state  subscribers:%zu", state_subscribe_number());
    log_info("depth  subscribers:%zu", depth_subscribe_number());
    log_info("depth  poll items :%zu", depth_poll_number());
}

static void print_market_stat(void)
{
    log_info("market: last_poll_time:%u last_update_time:%u last_update_count:%u", 
        market_stat.last_poll_time, market_stat.last_update_time, market_stat.last_update_count);
}

static void print_state_stat(void)
{
    log_info("state: last_update_time:%u last_update_count:%u", state_stat.last_update_time, state_stat.last_update_count);
}

static void print_depth_stat(void)
{
    log_info("depth: last_poll_time:%u last_poll_count:%u last_update_time:%u", 
        depth_stat.last_poll_time, depth_stat.last_poll_count, depth_stat.last_update_time);
}

static void on_statistic(nw_timer *timer, void *privdata)
{
    print_subscribers();
    print_market_stat();
    print_state_stat();
    print_depth_stat();
}

int init_statistic(void)
{
    nw_timer_set(&timer, settings.statistic_interval, true, on_statistic, NULL);
    nw_timer_start(&timer);

    return 0;
}

void stat_market_poll(void)
{
    market_stat.last_poll_time = time(NULL);
}

void stat_market_update(uint32_t count)
{
    market_stat.last_update_count = time(NULL);
    market_stat.last_update_count = count;
}

void stat_state_update(uint32_t count)
{
    state_stat.last_update_count = time(NULL);
    state_stat.last_update_count = count;
}

void stat_depth_poll(uint32_t count)
{
    depth_stat.last_poll_time = time(NULL);
    depth_stat.last_poll_count = count;
}

void stat_depth_update(void)
{
    depth_stat.last_update_time = time(NULL);
}