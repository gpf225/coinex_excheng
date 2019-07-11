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
        size_t market_len = strlen(market_name);
        if (market_len == 0 || market_len >= MARKET_NAME_MAX_LEN) {
            log_stderr("create market: %s fail", market_name);
            return -__LINE__;
        }

        log_info("create market: %s", market_name);
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
        size_t market_len = strlen(market_name);
        if (market_len == 0 || market_len >= MARKET_NAME_MAX_LEN) {
            log_fatal("update market: %s fail", market_name);
            continue;
        }

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
            char *item_string = json_dumps(item, 0);
            market_t *m = entry->val;
            market_update(m, item);
            log_info("update market: %s, config: %s", market_name, item_string);
            free(item_string);
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

bool check_market_account(uint32_t account, market_t *m)
{
    if (m->account == -1) {
        return true;
    } else if (account == m->account) {
        return true;
    }

    return false;
}

json_t *get_market_last_info(void)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        market_t *m = entry->val;
        json_t *market_info = json_object();
        json_object_set(market_info, "call_auction", json_boolean(m->call_auction));
        json_object_set_new_mpd(market_info, "last", m->last);
        json_object_set(result, m->name, market_info);
    }
    dict_release_iterator(iter);

    return result;
}

static struct convert_fee *get_convert_fee(const char *asset)
{
    dict_entry *entry = dict_find(settings.convert_fee_dict, asset);
    if (entry) {
        return entry->val;
    }

    return NULL;
}

void get_fee_price(market_t *m, const char *asset, mpd_t *fee_price)
{
    if (asset == NULL) {
        mpd_copy(fee_price, mpd_zero, &mpd_ctx);
        return;
    }

    if (strcmp(asset, m->money) == 0) {
        mpd_copy(fee_price, mpd_one, &mpd_ctx);
        return;
    }

    char name[100];
    struct convert_fee *convert = NULL;
    if (strcmp(asset, SYSTEM_FEE_TOKEN) == 0) {
        convert = get_convert_fee(m->money);
        if (convert) {
            snprintf(name, sizeof(name), "%s%s", asset, convert->money);
        } else {
            snprintf(name, sizeof(name), "%s%s", asset, m->money);
        }
    } else {
        snprintf(name, sizeof(name), "%s%s", asset, m->money);
    }

    market_t *m_fee = get_market(name);
    if (m_fee == NULL) {
        mpd_copy(fee_price, mpd_zero, &mpd_ctx);
        return;
    }

    if (convert) {
        mpd_div(fee_price, m_fee->last, convert->price, &mpd_ctx);
        mpd_rescale(fee_price, fee_price, -m_fee->money_prec, &mpd_ctx);
    } else {
        mpd_copy(fee_price, m_fee->last, &mpd_ctx);
    }

    return;
}

json_t *get_market_config(void)
{
    json_t *result = json_array();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        market_t *m = entry->val;
        json_t *info = json_object();
        json_object_set_new(info, "name", json_string(m->name));
        json_object_set_new(info, "stock", json_string(m->stock));
        json_object_set_new(info, "money", json_string(m->money));
        json_object_set_new(info, "account", json_integer(m->account));
        json_object_set_new(info, "fee_prec", json_integer(m->fee_prec));
        json_object_set_new(info, "stock_prec", json_integer(m->stock_prec));
        json_object_set_new(info, "money_prec", json_integer(m->money_prec));
        json_object_set_new_mpd(info, "min_amount", m->min_amount);
        json_array_append_new(result, info);
    }
    dict_release_iterator(iter);

    return result;
}

