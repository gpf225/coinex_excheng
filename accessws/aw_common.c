/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/27, create
 */

# include "aw_common.h"

uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

int dict_ses_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

dict_t* create_ses_dict(uint32_t init_size)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_compare;

    return dict_create(&dt, init_size);
}

uint32_t dict_str_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

int dict_str_compare(const void *value1, const void *value2)
{
    return strcmp(value1, value2);
}

void *dict_str_dup(const void *value)
{
    return strdup(value);
}

void dict_str_free(void *value)
{
    free(value);
}

void *list_str_dup(void *value)
{
    return strdup(value);
}

list_t* create_str_list(void)
{
    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.dup = list_str_dup;
    lt.free = dict_str_free;
    lt.compare = dict_str_compare;
    return list_create(&lt);
}


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