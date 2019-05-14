/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/29, create
 */

# include "me_config.h"
# include "me_trade.h"

dict_t *dict_market;
static nw_timer timer;

static uint32_t market_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int market_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *market_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void market_dict_key_free(void *key)
{
    free(key);
}

static void status_report(void)
{
    size_t pending_orders_count = 0;
    size_t pending_stops_count  = 0;

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        market_t *market = entry->val;
        pending_orders_count += dict_size(market->orders);
        pending_stops_count  += dict_size(market->stops);
    }
    dict_release_iterator(iter);

    profile_set("pending_orders", pending_orders_count);
    profile_set("pending_stops",  pending_stops_count);
}

static void on_timer(nw_timer *timer, void *privdata)
{
    status_report();
}

int init_trade(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = market_dict_hash_function;
    type.key_compare = market_dict_key_compare;
    type.key_dup = market_dict_key_dup;
    type.key_destructor = market_dict_key_free;

    dict_market = dict_create(&type, 64);
    if (dict_market == NULL)
        return -__LINE__;

    for (size_t i = 0; i < json_array_size(settings.market_cfg); ++i) {
        json_t *item = json_array_get(settings.market_cfg, i);
        const char *market_name = json_string_value(json_object_get(item, "name"));
        log_stderr("create market: %s", market_name);
        market_t *m = market_create(item);
        if (m == NULL) {
            char *item_string = json_dumps(item, 0);
            log_stderr("create market: %s, config: %s fail", market_name, item_string);
            free(item_string);
            return -__LINE__;
        }
        dict_add(dict_market, (char *)market_name, m);
    }

    nw_timer_set(&timer, 60, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int update_trade(void)
{
    for (size_t i = 0; i < json_array_size(settings.market_cfg); ++i) {
        json_t *item = json_array_get(settings.market_cfg, i);
        const char *market_name = json_string_value(json_object_get(item, "name"));
        dict_entry *entry = dict_find(dict_market, market_name);
        if (!entry) {
            log_info("create market: %s", market_name);
            market_t *m = market_create(item);
            if (m == NULL) {
                char *item_string = json_dumps(item, 0);
                log_error("create market: %s, config: %s fail", market_name, item_string);
                free(item_string);
                return -__LINE__;
            }
            dict_add(dict_market, (char *)market_name, m);
        } else {
            market_t *m = entry->val;
            int ret = market_update(m, item);
            if (ret < 0) {
                char *item_string = json_dumps(item, 0);
                log_error("update market: %s, config: %s fail", market_name, item_string);
                free(item_string);
                return -__LINE__;
            }
        }
    }

    return 0;
}

market_t *get_market(const char *name)
{
    dict_entry *entry = dict_find(dict_market, name);
    if (entry)
        return entry->val;
    return NULL;
}

json_t *get_market_last_info(void)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        market_t *m = entry->val;
        json_object_set_new_mpd(result, m->name, m->last);
    }
    dict_release_iterator(iter);

    return result;
}

static bool need_convert(const char *asset)
{
    for (int i = 0; i < settings.usdc_assets_num; ++i) {
        if (strcmp(asset, settings.usdc_assets[i]) == 0) {
            return true;
        }
    }
    return false;
}

mpd_t *get_fee_price(market_t *m, const char *asset)
{
    if (strcmp(asset, m->money) == 0) {
        return mpd_one;
    }
    
    char name[100];
    if (strcmp(asset, SYSTEM_FEE_TOKEN) == 0) {
        if (need_convert(m->money)) {
            snprintf(name, sizeof(name), "%s%s", asset, "USDC");
        } else {
            snprintf(name, sizeof(name), "%s%s", asset, m->money);
        }
    } else {
        snprintf(name, sizeof(name), "%s%s", asset, m->money);
    }
    
    market_t *m_fee = get_market(name);
    if (m_fee == NULL)
        return NULL;

    return m_fee->last;
}

