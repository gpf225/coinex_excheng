/*
 * Description: 
 *     History: ouxiangyang, 2019/04/27, create
 */

# include "aw_config.h"
# include "aw_http.h"
# include "aw_market.h"

static nw_timer market_timer;
static dict_t *dict_market;

struct market_val {
    int     id;
    double  update_time;
};

static uint32_t dict_market_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_market_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *dict_market_key_dup(const void *key)
{
    return strdup(key);
}

static void dict_market_key_free(void *key)
{
    free(key);
}

static void *dict_market_val_dup(const void *key)
{
    struct market_val *obj = malloc(sizeof(struct market_val));
    memcpy(obj, key, sizeof(struct market_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    struct market_val *obj = val;
    free(obj);
}

static void on_market_list_callback(json_t *result)
{
    static uint32_t update_id = 0;
    update_id += 1;

    for (size_t i = 0; i < json_array_size(result); ++i) {
        json_t *item = json_array_get(result, i);
        const char *name = json_string_value(json_object_get(item, "name"));
        dict_entry *entry = dict_find(dict_market, name);
        if (entry == NULL) {
            struct market_val val;
            memset(&val, 0, sizeof(val));
            val.id = update_id;
            dict_add(dict_market, (char *)name, &val);
            log_info("add market: %s", name);
        } else {
            struct market_val *info = entry->val;
            info->id = update_id;
        }
    }

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->id != update_id) {
            dict_delete(dict_market, entry->key);
            log_info("del market: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);
}

static void on_market_timer(nw_timer *timer, void *privdata)
{
    json_t *params = json_array();
    send_http_request("market.list", params, on_market_list_callback);
}

int init_market(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_market_hash_func;
    dt.key_compare = dict_market_key_compare;
    dt.key_dup = dict_market_key_dup;
    dt.key_destructor = dict_market_key_free;
    dt.val_dup = dict_market_val_dup;
    dt.val_destructor = dict_market_val_free;

    dict_market = dict_create(&dt, 64);
    if (dict_market == NULL)
        return -__LINE__;

    nw_timer_set(&market_timer, settings.market_interval, true, on_market_timer, NULL);
    nw_timer_start(&market_timer);
    on_market_timer(NULL, NULL);

    return 0;
}

bool market_exists(const char *market)
{
    return (dict_find(dict_market, market) != NULL);
}
