/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/27, create
 */

# include "aw_common.h"
# include "aw_config.h"

bool is_good_limit(int limit)
{
    for (int i = 0; i < settings.depth_limit.count; ++i) {
        if (settings.depth_limit.limit[i] == limit) {
            return true;
        }
    }

    return false;
}

bool is_good_interval(const char *interval)
{
    if (interval == NULL || strlen(interval) >= INTERVAL_MAX_LEN) {
        return false; 
    }

    mpd_t *merge = decimal(interval, 0);
    if (merge == NULL)
        return false;

    for (int i = 0; i < settings.depth_merge.count; ++i) {
        if (mpd_cmp(settings.depth_merge.limit[i], merge, &mpd_ctx) == 0) {
            mpd_del(merge);
            return true;
        }
    }

    mpd_del(merge);
    return false;
}

bool is_good_market(const char *market)
{
    if (market == NULL || strlen(market) == 0 || strlen(market) >= MARKET_NAME_MAX_LEN) {
        return false;     
    }

    return true;
}

bool is_empty_string(const char *str)
{
    return str == NULL || strlen(str) == 0;
}