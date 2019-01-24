/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/25, create
 */

# include "ca_statistic.h"
# include "ca_config.h"


typedef struct stat_qps_t {
    int time;
    int qps;
    int interval;
    int max_qps;
}stat_qps_t;

typedef struct stat_depth_t {
    uint64_t depth_req_total;        // depth请求总次数
    uint64_t depth_cache_hit;        // depth缓存命中次数
    uint64_t depth_cache_miss;       // depth缓存失效次数
    uint64_t depth_update_total;     // depth缓存更新次数
}stat_depth_t;

static stat_depth_t depth_stat = {0, 0, 0, 0};
static stat_qps_t depth_req_qps = {0, 0, 0, 0};
static stat_qps_t depth_update_qps = {0, 0, 0, 0};

static void update_qps(stat_qps_t *stat_qps)
{
    int now = current_timestamp();
    if (now - stat_qps->time <= stat_qps->interval) {
        ++stat_qps->qps;
    } else {
        if (stat_qps->qps > stat_qps->max_qps) {
            stat_qps->max_qps = stat_qps->qps;
        }
        stat_qps->time = now;
        stat_qps->qps = 1;
    }
}

void stat_depth_inc(void)
{
    update_qps(&depth_req_qps);
    ++depth_stat.depth_req_total;
}

void stat_depth_cache_hit(void)
{
    ++depth_stat.depth_cache_hit;
}

void stat_depth_cache_miss(void)
{
    ++depth_stat.depth_cache_miss;
}

void stat_depth_cache_update(void)
{
    update_qps(&depth_update_qps);
    ++depth_stat.depth_update_total;
}

sds stat_status(sds reply)
{
    reply = sdscatprintf(reply, "depth_req_total: %"PRIu64"\n", depth_stat.depth_req_total);
    reply = sdscatprintf(reply, "depth_cache_hit: %"PRIu64"\n", depth_stat.depth_cache_hit);
    reply = sdscatprintf(reply, "depth_cache_miss: %"PRIu64"\n", depth_stat.depth_cache_miss);
    reply = sdscatprintf(reply, "depth_update_total: %"PRIu64"\n", depth_stat.depth_update_total);

    reply = sdscatprintf(reply, "depth_req:    max: %d current:%d \n", depth_req_qps.max_qps, depth_req_qps.qps);
    reply = sdscatprintf(reply, "depth_update: max: %d current:%d \n", depth_update_qps.max_qps, depth_update_qps.qps);
    return reply;
}