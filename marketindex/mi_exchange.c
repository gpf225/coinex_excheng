/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "mi_exchange.h"

static dict_t *dict_exchange;

typedef int (*exchange_parser)(json_t *response, mpd_t **price, double *timestamp);

static int convert_ymdhmsm_to_timestamp(const char *str, double *timestamp)
{
    int year, month, day, hour, minute, second, milsec;
    int ret = sscanf(str, "%d-%d-%dT%d:%d:%d.%dZ", &year, &month, &day, &hour, &minute, &second, &milsec);
    if (ret != 7)
        return -__LINE__;

    struct tm dtm;
    memset(&dtm, 0, sizeof(dtm));
    dtm.tm_year = year - 1900;
    dtm.tm_mon  = month - 1;
    dtm.tm_mday = day;
    dtm.tm_hour = hour;
    dtm.tm_min  = minute;
    dtm.tm_sec  = second;

    *timestamp = mktime(&dtm) - timezone + (double)milsec / 1000;

    return 0;
}

static int convert_ymdhms_to_timestamp(const char *str, double *timestamp)
{
    int year, month, day, hour, minute, second;
    int ret = sscanf(str, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second);
    if (ret != 6)
        return -__LINE__;

    struct tm dtm;
    memset(&dtm, 0, sizeof(dtm));
    dtm.tm_year = year - 1900;
    dtm.tm_mon  = month - 1;
    dtm.tm_mday = day;
    dtm.tm_hour = hour;
    dtm.tm_min  = minute;
    dtm.tm_sec  = second;

    *timestamp = mktime(&dtm) - timezone;

    return 0;
}

int parse_coinex_response(json_t *response, mpd_t **price, double *timestamp)
{
    json_t *data = json_object_get(response, "data");
    if (data == NULL || !json_is_array(data))
        return -__LINE__;
    json_t *item = json_array_get(data, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "date_ms") || !json_object_get(item, "price"))
        return -__LINE__;

    double time_ms = json_real_value(json_object_get(item, "date_ms"));
    if (time_ms == 0)
        return -__LINE__;
    *timestamp = time_ms / 1000;

    ERR_RET_LN(read_cfg_mpd(item, "price", price, ""));

    return 0;
}

int parse_okex_response(json_t *response, mpd_t **price, double *timestamp)
{
    if (!json_is_array(response))
        return -__LINE__;
    json_t *item = json_array_get(response, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "time") || !json_object_get(item, "price"))
        return -__LINE__;

    const char *time_str = json_string_value(json_object_get(item, "time"));
    if (time_str == NULL)
        return -__LINE__;
    if (convert_ymdhmsm_to_timestamp(time_str, timestamp) < 0)
        return -__LINE__;

    ERR_RET_LN(read_cfg_mpd(item, "price", price, ""));

    return 0;
}

int parse_binance_response(json_t *response, mpd_t **price, double *timestamp)
{
    if (!json_is_array(response))
        return -__LINE__;
    json_t *item = json_array_get(response, json_array_size(response) - 1);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "time") || !json_object_get(item, "price"))
        return -__LINE__;

    double time_ms = json_real_value(json_object_get(item, "time"));
    if (time_ms == 0)
        return -__LINE__;
    *timestamp = time_ms / 1000;

    ERR_RET_LN(read_cfg_mpd(item, "price", price, ""));

    return 0;
}

int parse_huobiglobal_response(json_t *response, mpd_t **price, double *timestamp)
{
    json_t *tick = json_object_get(response, "tick");
    if (tick == NULL || !json_is_object(tick))
        return -__LINE__;
    json_t *data = json_object_get(tick, "data");
    if (data == NULL || !json_is_array(data))
        return -__LINE__;
    json_t *item = json_array_get(data, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "ts") || !json_object_get(item, "price"))
        return -__LINE__;

    int64_t time_ms = json_real_value(json_object_get(item, "ts"));
    if (time_ms == 0)
        return -__LINE__;
    *timestamp = time_ms / 1000;

    char buf[100];
    snprintf(buf, sizeof(buf), "%f", json_real_value(json_object_get(item, "price")));
    *price = decimal(buf, 0);

    return 0;
}

int parse_gateio_response(json_t *response, mpd_t **price, double *timestamp)
{
    json_t *data = json_object_get(response, "data");
    if (data == NULL || !json_is_array(data))
        return -__LINE__;
    json_t *item = json_array_get(data, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "timestamp") || !json_object_get(item, "rate"))
        return -__LINE__;

    double time = strtod(json_string_value(json_object_get(item, "timestamp")), NULL);
    if (time == 0)
        return -__LINE__;
    *timestamp = time;

    ERR_RET_LN(read_cfg_mpd(item, "rate", price, ""));

    return 0;
}

int parse_kucoin_response(json_t *response, mpd_t **price, double *timestamp)
{
    json_t *data = json_object_get(response, "data");
    if (data == NULL || !json_is_array(data))
        return -__LINE__;
    json_t *item = json_array_get(data, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "time") || !json_object_get(item, "price"))
        return -__LINE__;

    double time = json_real_value(json_object_get(item, "time"));
    if (time == 0)
        return -__LINE__;
    *timestamp = time / 1000000000;

    ERR_RET_LN(read_cfg_mpd(item, "price", price, ""));

    return 0;
}

int parse_bitfinex_response(json_t *response, mpd_t **price, double *timestamp)
{
    if (!json_is_array(response))
        return -__LINE__;
    json_t *item = json_array_get(response, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "timestamp") || !json_object_get(item, "price"))
        return -__LINE__;

    double time = json_real_value(json_object_get(item, "timestamp"));
    if (time == 0)
        return -__LINE__;
    *timestamp = time;

    ERR_RET_LN(read_cfg_mpd(item, "price", price, ""));

    return 0;
}

int parse_mxc_response(json_t *response, mpd_t **price, double *timestamp)
{
    json_t *data = json_object_get(response, "data");
    if (data == NULL || !json_is_array(data))
        return -__LINE__;
    json_t *item = json_array_get(data, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "tradeTime") || !json_object_get(item, "tradePrice"))
        return -__LINE__;

    const char *time_str = json_string_value(json_object_get(item, "tradeTime"));
    if (time_str == NULL)
        return -__LINE__;
    if (convert_ymdhms_to_timestamp(time_str, timestamp) < 0)
        return -__LINE__;
    *timestamp = *timestamp - 8 * 3600;

    ERR_RET_LN(read_cfg_mpd(item, "tradePrice", price, ""));

    return 0;
}

int parse_bittrex_response(json_t *response, mpd_t **price, double *timestamp)
{
    json_t *result = json_object_get(response, "result");
    if (result == NULL || !json_is_array(result))
        return -__LINE__;
    json_t *item = json_array_get(result, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "TimeStamp") || !json_object_get(item, "Price"))
        return -__LINE__;

    const char *time_str = json_string_value(json_object_get(item, "TimeStamp"));
    if (time_str == NULL)
        return -__LINE__;
    if (convert_ymdhmsm_to_timestamp(time_str, timestamp) < 0)
        return -__LINE__;

    char buf[100];
    snprintf(buf, sizeof(buf), "%f", json_real_value(json_object_get(item, "Price")));
    *price = decimal(buf, 0);

    return 0;
}

int parse_poloniex_response(json_t *response, mpd_t **price, double *timestamp)
{
    if (!json_is_array(response))
        return -__LINE__;
    json_t *item = json_array_get(response, 0);
    if (item == NULL || !json_is_object(item) || !json_object_get(item, "date") || !json_object_get(item, "rate"))
        return -__LINE__;

    const char *time_str = json_string_value(json_object_get(item, "date"));
    if (time_str == NULL)
        return -__LINE__;

    if (convert_ymdhms_to_timestamp(time_str, timestamp) < 0)
        return -__LINE__;

    ERR_RET_LN(read_cfg_mpd(item, "rate", price, ""));

    return 0;
}

int init_exchange()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;

    dict_exchange = dict_create(&dt, 16);
    if (dict_exchange == NULL)
        return -__LINE__;

    dict_add(dict_exchange, "coinex",       parse_coinex_response);
    dict_add(dict_exchange, "okex",         parse_okex_response);
    dict_add(dict_exchange, "binance",      parse_binance_response);
    dict_add(dict_exchange, "huobiglobal",  parse_huobiglobal_response);
    dict_add(dict_exchange, "gateio",       parse_gateio_response);
    dict_add(dict_exchange, "kucoin",       parse_kucoin_response);
    dict_add(dict_exchange, "bitfinex",     parse_bitfinex_response);
    dict_add(dict_exchange, "mxc",          parse_mxc_response);
    dict_add(dict_exchange, "bittrex",      parse_bittrex_response);
    dict_add(dict_exchange, "poloniex",     parse_poloniex_response);

    return 0;
}

json_t *exchange_list(void)
{
    json_t *list = json_array();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_exchange);
    while ((entry = dict_next(iter)) != NULL) {
        json_array_append_new(list, json_string(entry->key));
    }
    dict_release_iterator(iter);

    return list;
}

bool exchange_is_supported(const char *name)
{
    if (dict_find(dict_exchange, name) != NULL)
        return true;
    return false;
}

int exchange_parse_response(const char *name, json_t *response, mpd_t **price, double *timestamp)
{
    dict_entry *entry = dict_find(dict_exchange, name);
    if (entry == NULL)
        return -__LINE__;
    exchange_parser parser = entry->val;
    return parser(response, price, timestamp);
}

