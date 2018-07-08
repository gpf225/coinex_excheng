/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/29, create
 */

# include "me_config.h"
# include "me_trade.h"

static dict_t *dict_market;
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
    size_t count = 0;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        market_t *market = entry->val;
        count += dict_size(market->orders);
    }
    dict_release_iterator(iter);
    profile_set("pending_orders", count);
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

    for (size_t i = 0; i < settings.market_num; ++i) {
        log_stderr("create market: %s", settings.markets[i].name);
        market_t *m = market_create(&settings.markets[i]);
        if (m == NULL) {
            return -__LINE__;
        }

        dict_add(dict_market, settings.markets[i].name, m);
    }

    nw_timer_set(&timer, 60, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int update_trade(void)
{
    for (size_t i = 0; i < settings.market_num; ++i) {
        dict_entry *entry = dict_find(dict_market, settings.markets[i].name);
        if (!entry) {
            market_t *m = market_create(&settings.markets[i]);
            if (m == NULL)
                return -__LINE__;
            dict_add(dict_market, settings.markets[i].name, m);
        } else {
            market_update(entry->val, &settings.markets[i]);
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

mpd_t *get_fee_price(market_t *m, const char *asset)
{
    char name[100];
    snprintf(name, sizeof(name), "%s%s", asset, m->money);
    market_t *m_fee = get_market(name);
    if (m_fee == NULL)
        return NULL;
    return m_fee->last;
}

