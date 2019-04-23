/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ar_config.h"
# include "ar_ticker.h"
# include "ar_market.h"

static dict_t *dict_state;

struct state_data {
    uint32_t cmd;
    char market[MARKET_NAME_MAX_LEN];
};

struct state_val {
    json_t  *last;
};

static uint32_t dict_market_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_market_compare(const void *value1, const void *value2)
{
    return strcmp(value1, value2);
}

static void *dict_market_dup(const void *value)
{
    return strdup(value);
}

static void dict_market_free(void *value)
{
    free(value);
}

static void *dict_state_val_dup(const void *key)
{
    struct state_val *obj = malloc(sizeof(struct state_val));
    memcpy(obj, key, sizeof(struct state_val));
    return obj;
}

static void dict_state_val_free(void *val)
{
    struct state_val *obj = val;
    if (obj->last) {
        json_decref(obj->last);
    }
    free(obj);
}

int status_ticker_update(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct state_val val;
        memset(&val, 0, sizeof(val));

        val.last = json_object();
        json_object_set(val.last, "vol", json_object_get(result, "volume"));
        json_object_set(val.last, "low", json_object_get(result, "low"));
        json_object_set(val.last, "open", json_object_get(result, "open"));
        json_object_set(val.last, "high", json_object_get(result, "high"));
        json_object_set(val.last, "last", json_object_get(result, "last"));

        dict_add(dict_state, (char *)market, &val);
        return 0;
    }

    struct state_val *info = entry->val;
    if (info->last != NULL)
        json_decref(info->last);

    info->last = json_object();
    json_object_set(info->last, "vol", json_object_get(result, "volume"));
    json_object_set(info->last, "low", json_object_get(result, "low"));
    json_object_set(info->last, "open", json_object_get(result, "open"));
    json_object_set(info->last, "high", json_object_get(result, "high"));
    json_object_set(info->last, "last", json_object_get(result, "last"));

    return 0;
}

int depth_ticker_update(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL)
        return -__LINE__;
    struct state_val *info = entry->val;

    if (info->last == NULL) {
        info->last = json_object();
    } else {
        json_decref(info->last);
        info->last = json_object();
    }

    json_t *bids = json_object_get(result, "bids");
    if (json_array_size(bids) == 1) {
        json_t *buy = json_array_get(bids, 0);
        json_object_set(info->last, "buy", json_array_get(buy, 0));
        json_object_set(info->last, "buy_amount", json_array_get(buy, 1));
    } else {
        json_object_set_new(info->last, "buy", json_string("0"));
        json_object_set_new(info->last, "buy_amount", json_string("0"));
    }

    json_t *asks = json_object_get(result, "asks");
    if (json_array_size(asks) == 1) {
        json_t *sell = json_array_get(asks, 0);
        json_object_set(info->last, "sell", json_array_get(sell, 0));
        json_object_set(info->last, "sell_amount", json_array_get(sell, 1));
    } else {
        json_object_set_new(info->last, "sell", json_string("0"));
        json_object_set_new(info->last, "sell_amount", json_string("0"));
    }

    return 0;
}

int init_ticker(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_market_hash_func;
    dt.key_dup        = dict_market_dup;
    dt.key_destructor = dict_market_free;
    dt.key_compare    = dict_market_compare;
    dt.val_dup        = dict_state_val_dup;
    dt.val_destructor = dict_state_val_free;

    dict_state = dict_create(&dt, 64);
    if (dict_state == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    return 0;
}

json_t *get_market_ticker(const void *market)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL)
        return NULL;

    struct state_val *info = entry->val;
    if (info->last == NULL) {
        return NULL;
    }

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer((uint64_t)(current_timestamp() * 1000)));
    json_object_set(data, "ticker", info->last);

    return data;
}

json_t *get_market_ticker_all(void)
{
    json_t *ticker = json_object();

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_state);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        struct state_val *info = entry->val;
        json_object_set(ticker, market, info->last);
    }
    dict_release_iterator(iter);

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer((uint64_t)(current_timestamp() * 1000)));
    json_object_set_new(data, "ticker", ticker);

    return data;
}

