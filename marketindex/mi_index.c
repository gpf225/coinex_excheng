/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "mi_exchange.h"
# include "mi_request.h"
# include "mi_history.h"
# include "mi_message.h"
# include "mi_index.h"
# include "ut_comm_dict.h"

static nw_timer timer;
static nw_state *state_context;
static dict_t *dict_market;

struct market_info {
    int     prec;
    dict_t  *sources;

    mpd_t   *last_index;
    time_t  last_index_time;
};

struct source_info {
    char    *exchange;
    char    *url;
    mpd_t   *weight;

    mpd_t   *last_price;
    double  last_time;
    time_t  last_update;
};

struct state_data {
    char *market;
    time_t request_time;
    size_t request_count;
    size_t finished_count;
};

static void market_info_free(void *val)
{
    struct market_info *info = val;
    if (info->last_index)
        mpd_del(info->last_index);
    if (info->sources)
        dict_release(info->sources);
    free(info);
}

static void source_info_free(void *val)
{
    struct source_info *info = val;
    if (info->weight)
        mpd_del(info->weight);
    if (info->last_price)
        mpd_del(info->last_price);
    free(info->exchange);
    free(info->url);
    free(info);
}

static void on_request_finished(const char *market, time_t timestamp)
{
    dict_entry *entry = dict_find(dict_market, market);
    if (entry == NULL)
        return;
    struct market_info *minfo = entry->val;

    mpd_t *index = mpd_new(&mpd_ctx);
    mpd_t *total_index = mpd_qncopy(mpd_zero);
    mpd_t *total_weight = mpd_qncopy(mpd_zero);
    json_t *detail = json_object();

    dict_iterator *iter = dict_get_iterator(minfo->sources);
    while ((entry = dict_next(iter)) != NULL) {
        const char *exchange = entry->key;
        struct source_info *sinfo = entry->val;
        if (sinfo->last_update != timestamp)
            continue;
        if (sinfo->last_update - sinfo->last_time > settings.expire_interval)
            continue;

        mpd_mul(index, sinfo->last_price, sinfo->weight, &mpd_ctx);
        mpd_add(total_index, total_index, index, &mpd_ctx);
        mpd_add(total_weight, total_weight, sinfo->weight, &mpd_ctx);

        json_t *item = json_object();
        json_object_set_new_mpd(item, "price", sinfo->last_price);
        json_object_set_new_mpd(item, "weight", sinfo->weight);
        json_object_set_new(detail, exchange, item);
    }
    dict_release_iterator(iter);

    if (mpd_cmp(total_index, mpd_zero, &mpd_ctx) == 0) {
        log_fatal("update market: %s, timestamp: %ld fail", market, timestamp);
        goto cleanup;
    }

    mpd_div(index, total_index, total_weight, &mpd_ctx);
    mpd_rescale(index, index, -minfo->prec, &mpd_ctx);

    if (minfo->last_index == NULL) {
        minfo->last_index = mpd_qncopy(index);
    } else {
        mpd_copy(minfo->last_index, index, &mpd_ctx);
    }
    minfo->last_index_time = timestamp;

    char *detail_str = json_dumps(detail, 0);
    push_index_message(market, index, detail);
    append_index_history(market, index, detail_str);
    free(detail_str);
    profile_inc("update_success", 1);

cleanup:
    mpd_del(index);
    mpd_del(total_index);
    mpd_del(total_weight);
    json_decref(detail);
}

static void on_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
    struct state_data *state = entry->data;
    on_request_finished(state->market, state->request_time);
}

static int init_state(void)
{
    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;
    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL) {
        return -__LINE__;
    }

    return 0;
}

static void update_market_index(const char *market, const char *exchange, time_t timestamp, mpd_t *price, double price_time)
{
    dict_entry *entry = dict_find(dict_market, market);
    if (entry == NULL)
        return;
    struct market_info *minfo = entry->val;
    entry = dict_find(minfo->sources, exchange);
    if (entry == NULL)
        return;
    struct source_info *sinfo = entry->val;
    if (sinfo->last_price)
        mpd_del(sinfo->last_price);

    sinfo->last_price  = price;
    sinfo->last_time   = price_time;
    sinfo->last_update = timestamp;
}

static void on_request_callback(uint32_t id, const char *exchange, json_t *reply)
{
    nw_state_entry *entry = nw_state_get(state_context, id);
    if (entry == NULL)
        return;
    struct state_data *state = entry->data;

    if (reply != NULL) {
        mpd_t *price = NULL;
        double price_time = 0;
        int ret = exchange_parse_response(exchange, reply, &price, &price_time);
        if (ret < 0) {
            char *reply_str = json_dumps(reply, 0);
            log_fatal("parse exchange: %s response: %s fail: %d", exchange, reply_str, ret);
            free(reply_str);
        } else {
            char buf[100];
            log_debug("exchange: %s, price: %s, time: %lf", exchange, strmpd(buf, sizeof(buf), price), price_time);
            update_market_index(state->market, exchange, state->request_time, price, price_time);
        }
        profile_inc("request_success", 1);
    } else {
        profile_inc("request_fail", 1);
    }

    state->finished_count += 1;
    if (state->finished_count == state->request_count) {
        on_request_finished(state->market, state->request_time);
        nw_state_del(state_context, id);
    }
}

static void request_market_index(char *market, struct market_info *info, time_t now)
{
    nw_state_entry *entry = nw_state_add(state_context, settings.request_timeout + 0.5, 0);
    struct state_data *state = entry->data;
    state->market = market;
    state->request_time = now;
    state->request_count = dict_size(info->sources);

    dict_entry *result;
    dict_iterator *iter = dict_get_iterator(info->sources);
    while ((result = dict_next(iter)) != NULL) {
        struct source_info *sinfo = result->val;
        send_request(entry->id, sinfo->exchange, sinfo->url, settings.request_timeout, on_request_callback);
    }
    dict_release_iterator(iter);
}

static void request_index(time_t now)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        request_market_index(entry->key, entry->val, now);
    }
    dict_release_iterator(iter);
}

static struct market_info *market_create(json_t *node)
{
    struct market_info *minfo = malloc(sizeof(struct market_info));
    memset(minfo, 0, sizeof(struct market_info));
    if (read_cfg_int(node, "prec", &minfo->prec, true, 0) < 0)
        goto error;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_destructor = source_info_free;
    minfo->sources = dict_create(&dt, 16);
    if (minfo->sources == NULL)
        goto error;

    json_t *sources = json_object_get(node, "sources");
    if (sources == NULL || !json_is_array(sources))
        goto error;
    for (size_t i = 0; i < json_array_size(sources); ++i) {
        json_t *item = json_array_get(sources, i);
        struct source_info *sinfo = malloc(sizeof(struct source_info));
        memset(sinfo, 0, sizeof(struct source_info));
        if (read_cfg_str(item, "exchange", &sinfo->exchange, "") < 0)
            goto error;
        strtolower(sinfo->exchange);
        if (!exchange_is_supported(sinfo->exchange))
            goto error;
        if (read_cfg_str(item, "trade_url", &sinfo->url, "") < 0)
            goto error;
        if (read_cfg_mpd(item, "weight", &sinfo->weight, "") < 0)
            goto error;
        dict_add(minfo->sources, sinfo->exchange, sinfo);
    }

    return minfo;
error:
    market_info_free(minfo);
    return NULL;
}

int reload_index_config(void)
{
    const char *key;
    json_t *value;
    json_object_foreach(settings.index_cfg, key, value) {
        struct market_info *minfo = market_create(value);
        if (minfo == NULL) {
            return -__LINE__;
        }
        dict_replace(dict_market, (void *)key, minfo);
    }

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        if (json_object_get(settings.index_cfg, market) == NULL) {
            dict_delete(dict_market, market);
        }
    }
    dict_release_iterator(iter);

    time_t now = time(NULL);
    request_index(now);

    return 0;
}

static int init_dict(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_destructor = market_info_free;
    dict_market = dict_create(&dt, 16);
    if (dict_market == NULL)
        return -__LINE__;

    int ret = reload_index_config();
    if (ret < 0) {
        char *index_cfg_str = json_dumps(settings.index_cfg, 0);
        log_error("update index config fail: %d\n%s", ret, index_cfg_str);
        free(index_cfg_str);
        return -__LINE__;
    }

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata)
{
    static time_t last_sec;
    time_t now = time(NULL);
    if (now != last_sec) {
        last_sec = now;
        if (now % settings.update_interval == 0) {
            request_index(now);
        }
    }
}

int init_index(void)
{
    ERR_RET(init_state());
    ERR_RET(init_dict());

    nw_timer_set(&timer, 0.01, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

bool market_exists(const char *market_name)
{
    if (dict_find(dict_market, market_name) != NULL)
        return true;
    return false;
}

json_t *get_market_list(void)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market_name = entry->key;
        struct market_info *info = entry->val;
        if (info->last_index == NULL)
            continue;

        json_t *item = json_object();
        json_object_set_new(item, "time", json_integer(info->last_index_time * 1000));
        json_object_set_new_mpd(item, "index", info->last_index);
        json_object_set_new(result, market_name, item);
    }
    dict_release_iterator(iter);

    return result;
}

json_t *get_market_index(const char *market_name)
{
    dict_entry *entry = dict_find(dict_market, market_name);
    if (entry == NULL)
        return NULL;
    struct market_info *info = entry->val;
    if (info->last_index == NULL)
        return NULL;

    json_t *result = json_object();
    json_object_set_new(result, "name", json_string(market_name));
    json_object_set_new(result, "time", json_integer(info->last_index_time * 1000));
    json_object_set_new_mpd(result, "index", info->last_index);

    return result;
}

