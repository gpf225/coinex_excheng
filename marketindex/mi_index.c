/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "mi_exchange.h"
# include "mi_request.h"
# include "mi_history.h"
# include "mi_message.h"
# include "mi_index.h"

static nw_timer timer;
static nw_state *state_context;
static dict_t *dict_market;
static dict_t *dict_compose;

#define COMPOSE_METHD_MUL   1
#define COMPOSE_METHD_DIV   2

#define COMPOSE_DIV_FIRST   1
#define COMPOSE_DIV_SECOND  2

struct market_info {
    int       prec;
    dict_t    *sources;

    mpd_t     *last_index;
    mpd_t     *protect_price;
    time_t    last_protect_time;
    time_t    last_index_time;

    bool      use_compose;
    int       compose_methd;
    char      *compose_first_market;
    char      *compose_second_market;
    uint32_t  compose_state_id;
};

struct compose_info {
    char      *target_market;
    char      *other_market;
    uint32_t  div_seq;
};

struct compose_data {
    list_t   *composes;
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
    if (info->protect_price)
        mpd_del(info->protect_price);
    if (info->sources)
        dict_release(info->sources);
    if (info->compose_first_market)
        free(info->compose_first_market);
    if (info->compose_second_market)
        free(info->compose_second_market);
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

static void *compose_data_dup(const void *key)
{
    struct compose_data *val = malloc(sizeof(struct compose_data));
    memcpy(val, key, sizeof(struct compose_data));
    return val;
}

static void compose_data_free(void *val)
{
    struct compose_data *obj = val;
    if (obj->composes)
        list_release(obj->composes);
    free(obj);
}

static void list_compose_info_free(void *value)
{
    struct compose_info *obj = value;
    if (obj->target_market)
        free(obj->target_market);
    if (obj->other_market)
        free(obj->other_market);
    free(obj);
}

static dict_t *create_compose_dict(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_dup        = compose_data_dup;
    dt.val_destructor = compose_data_free;

    return dict_create(&dt, 16);
}

static dict_t *create_market_dict(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_destructor = market_info_free;

    return dict_create(&dt, 16);
}

static void update_single_compose_index(struct compose_info *info, const char *market, time_t timestamp, mpd_t *index)
{
    dict_entry *entry = dict_find(dict_market, info->other_market);
    if (entry == NULL) {
        log_fatal("update compose index fail, no other market, market: %s", market);
        return;
    }
    struct market_info *other_minfo = entry->val;
    if (other_minfo->last_index_time != timestamp)
        return;

    entry = dict_find(dict_market, info->target_market);
    if (entry == NULL) {
        log_fatal("update compose index fail, no target market, market: %s", market);
        return;
    }
    struct market_info *target_minfo = entry->val;

    mpd_t *index_result = mpd_new(&mpd_ctx);
    if (target_minfo->compose_methd == COMPOSE_METHD_MUL) {
        mpd_mul(index_result, index, other_minfo->last_index, &mpd_ctx);
    } else {
        if (info->div_seq == COMPOSE_DIV_FIRST) {
            mpd_div(index_result, index, other_minfo->last_index, &mpd_ctx);
        } else {
            mpd_div(index_result, other_minfo->last_index, index, &mpd_ctx);
        }
    }

    mpd_rescale(index_result, index_result, -target_minfo->prec, &mpd_ctx);
    if (target_minfo->last_index) {
        mpd_del(target_minfo->last_index);
    }
    target_minfo->last_index = mpd_qncopy(index_result);
    target_minfo->last_index_time = timestamp;

    json_t *detail = json_object();
    json_object_set_new_mpd(detail, market, index);
    json_object_set_new_mpd(detail, info->other_market, other_minfo->last_index);
    char *detail_str = json_dumps(detail, 0);
    push_index_message(info->target_market, index_result, detail);
    append_index_history(info->target_market, index_result, detail_str);
    free(detail_str);
    mpd_del(index_result);
    nw_state_del(state_context, target_minfo->compose_state_id);
    profile_inc("update_success", 1);
}

static void update_compose_index(const char *market, time_t timestamp, mpd_t *index) 
{
    dict_entry *entry = dict_find(dict_compose, market);
    if (!entry)
        return;

    struct compose_data *obj = entry->val;
    list_node *node;
    list_iter *iter = list_get_iterator(obj->composes, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        struct compose_info *info = node->value;
        update_single_compose_index(info, market, timestamp, index);
    }
    list_release_iterator(iter);

    return;
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
        if (sinfo->last_price == NULL)
            continue;
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
        log_fatal("update market: %s, timestamp: %ld, use_compose: %d fail", market, timestamp, minfo->use_compose);
        goto cleanup;
    }

    mpd_div(index, total_index, total_weight, &mpd_ctx);
    mpd_rescale(index, index, -minfo->prec, &mpd_ctx);

    if (minfo->last_index == NULL) {
        minfo->last_index = mpd_qncopy(index);
        minfo->protect_price = mpd_qncopy(index);
        minfo->last_protect_time = timestamp;
    } else {
        mpd_copy(minfo->last_index, index, &mpd_ctx);
    }
    minfo->last_index_time = timestamp;

    if (time(NULL) - minfo->last_protect_time >= settings.protect_interval) {
        mpd_copy(minfo->protect_price, index, &mpd_ctx);
    }

    update_compose_index(market, timestamp, index);

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

bool check_price(mpd_t *price, mpd_t *protect_price, mpd_t *protect_rate)
{
    if (protect_price == NULL)
        return true;

    mpd_t *change = mpd_new(&mpd_ctx);
    mpd_sub(change, price, protect_price, &mpd_ctx);
    mpd_abs(change, change, &mpd_ctx);
    mpd_div(change, change, protect_price, &mpd_ctx);

    bool ret = mpd_cmp(change, protect_rate, &mpd_ctx) <= 0;
    mpd_del(change);

    return ret;
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
    if (sinfo->last_price == NULL) {
        sinfo->last_price  = price;
        sinfo->last_time   = price_time;
        sinfo->last_update = timestamp;
        return;
    }

    if (!check_price(price, minfo->protect_price, settings.protect_rate)) {
        char buf_price[30], buf_protect_price[30], buf_protect_rate[30];
        log_fatal("url: %s, protect_interval: %d, price: %s, protect_price: %s, protect_rate: %s", sinfo->url, settings.protect_interval, strmpd(buf_price, sizeof(buf_price), price), 
                strmpd(buf_protect_price, sizeof(buf_protect_price), minfo->protect_price), strmpd(buf_protect_rate, sizeof(buf_protect_rate), settings.protect_rate));
        return;
    }

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
            log_error("parse exchange: %s response: %s fail: %d", exchange, reply_str, ret);
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
    if (info->use_compose) {
        info->compose_state_id = entry->id;
        return;
    }
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

static struct market_info *market_create(json_t *node, const char *key, dict_t *dict_compose_tmp)
{
    struct market_info *minfo = malloc(sizeof(struct market_info));
    memset(minfo, 0, sizeof(struct market_info));
    if (read_cfg_int(node, "prec", &minfo->prec, true, 0) < 0)
        goto error;
    if (read_cfg_bool(node, "use_compose", &minfo->use_compose, false, false) < 0)
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

    if (minfo->use_compose) {
        if (read_cfg_str(node, "compose_first_market", &minfo->compose_first_market, NULL) < 0)
            goto error;
        if (read_cfg_str(node, "compose_second_market", &minfo->compose_second_market, NULL) < 0)
            goto error;
        if (read_cfg_int(node, "compose_methd", &minfo->compose_methd, true, 0) < 0)
            goto error;

        if (minfo->compose_methd != COMPOSE_METHD_MUL && minfo->compose_methd != COMPOSE_METHD_DIV)
            goto error;
        if (!json_object_get(settings.index_cfg, minfo->compose_first_market))
            goto error;
        if (!json_object_get(settings.index_cfg, minfo->compose_second_market))
            goto error;

        // calculation compose index, first market
        dict_entry *entry = dict_find(dict_compose_tmp, minfo->compose_first_market);
        if (!entry) {
            struct compose_data val;
            list_type lt;
            memset(&lt, 0, sizeof(lt));
            lt.free = list_compose_info_free;
            val.composes = list_create(&lt);
            entry = dict_add(dict_compose_tmp, minfo->compose_first_market, &val);
        }
        struct compose_data *obj = entry->val;
        struct compose_info *info = malloc(sizeof(struct compose_info));
        info->target_market = strdup(key);
        info->other_market = strdup(minfo->compose_second_market);
        if (minfo->compose_methd == COMPOSE_METHD_DIV) {
            info->div_seq = COMPOSE_DIV_FIRST;
        }
        list_add_node_head(obj->composes, info); 

        // calculation compose index, second market
        entry = dict_find(dict_compose_tmp, minfo->compose_second_market);
        if (!entry) {
            struct compose_data val;
            list_type lt;
            memset(&lt, 0, sizeof(lt));
            lt.free = list_compose_info_free;
            val.composes = list_create(&lt);
            entry = dict_add(dict_compose_tmp, minfo->compose_second_market, &val);
        }
        obj = entry->val;
        info = malloc(sizeof(struct compose_info));
        info->target_market = strdup(key);
        info->other_market = strdup(minfo->compose_first_market);
        if (minfo->compose_methd == COMPOSE_METHD_DIV) {
            info->div_seq = COMPOSE_DIV_SECOND;
        }
        list_add_node_head(obj->composes, info); 
    } else {
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
    }

    return minfo;

error:
    market_info_free(minfo);
    return NULL;
}

int reload_index_config(void)
{
    log_info("update index config");
    dict_t *dict_compose_tmp = create_compose_dict();
    dict_t *dict_market_tmp = create_market_dict();

    const char *key;
    json_t *value;
    json_object_foreach(settings.index_cfg, key, value) {
        struct market_info *minfo = market_create(value, key, dict_compose_tmp);
        if (minfo == NULL) {
            char *str = json_dumps(value, 0);
            log_fatal("update index config fail, market: %s, config: %s", key, str);
            free(str);
            dict_release(dict_compose_tmp);
            dict_release(dict_market_tmp);
            return -__LINE__;
        }
        dict_add(dict_market_tmp, (void *)key, minfo);
    }

    dict_release(dict_compose);
    dict_compose = dict_compose_tmp;

    dict_release(dict_market);
    dict_market = dict_market_tmp;

    time_t now = time(NULL);
    request_index(now);

    return 0;
}

static int init_dict(void)
{
    dict_market = create_market_dict();
    if (dict_market == NULL)
        return -__LINE__;

    dict_compose = create_compose_dict();
    if (dict_compose == NULL)
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

