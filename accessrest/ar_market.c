/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/18, create
 */

# include "ar_market.h"
# include "nw_job.h"
# include "ut_log.h"

# include <curl/curl.h>

static const long HTTP_TIMEOUT = 2000L;

static dict_t *dict_market = NULL;
static nw_job *job = NULL;
static nw_timer market_update_timer;

struct market_val {
    int     id;
    json_t *info;
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
    if (obj->info) {
        json_decref(obj->info);
    }
    free(obj);
}

static size_t write_callback_func(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    sds *reply = userdata;
    *reply = sdscatlen(*reply, ptr, size * nmemb);
    return size * nmemb;
}

static sds http_get(const char *url)
{
    CURL *curl = curl_easy_init();
    sds reply_str = sdsempty();

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback_func);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &reply_str);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, HTTP_TIMEOUT);

    CURLcode ret = curl_easy_perform(curl);
    if (ret != CURLE_OK) {
        log_fatal("get %s fail: %s", url, curl_easy_strerror(ret));
    }
    
    curl_easy_cleanup(curl);
    return reply_str;
}

static json_t* get_market_item(const char *market_name, json_t *market_info) 
{
    const char *maker_fee_rate = json_string_value(json_object_get(market_info, "maker_fee_rate"));
    const char *taker_fee_rate = json_string_value(json_object_get(market_info, "taker_fee_rate"));
    const char *min_amount = json_string_value(json_object_get(market_info, "min_amount"));
    if ( (maker_fee_rate == NULL) || (taker_fee_rate == NULL) || (min_amount == NULL) ) {
        log_error("market:%s, info incompleted", market_name);
        return NULL;
    }

    json_t *money = json_object_get(market_info, "money");
    json_t *stock = json_object_get(market_info, "stock");
    if ( (money == NULL) || (stock == NULL) ) {
        log_error("market:%s, info does not have money or stock", market_name);
        return NULL;
    }

    const char *money_name = json_string_value(json_object_get(money, "name"));
    const int money_prec = json_integer_value(json_object_get(money, "prec"));
    if ( (money_name == NULL) || (money_prec <= 0) ) {
        log_error("market:%s, money item invalid", market_name);
        return NULL;
    }

    const char *stock_name = json_string_value(json_object_get(stock, "name"));
    const int stock_prec = json_integer_value(json_object_get(stock, "prec"));
    if ( (stock_name == NULL) || (stock_prec <= 0) ) {
        log_error("market:%s, stock item invalid", market_name);
        return NULL;
    }   

    json_t *market_item = json_object();
    json_object_set_new(market_item, "name", json_string(market_name));
    json_object_set_new(market_item, "min_amount", json_string(min_amount));
    json_object_set_new(market_item, "maker_fee_rate", json_string(maker_fee_rate));
    json_object_set_new(market_item, "taker_fee_rate", json_string(taker_fee_rate));
    json_object_set_new(market_item, "pricing_name", json_string(money_name));
    json_object_set_new(market_item, "pricing_decimal", json_integer(money_prec));
    json_object_set_new(market_item, "trading_name", json_string(stock_name));
    json_object_set_new(market_item, "trading_decimal", json_integer(stock_prec));
    return market_item;
}

static int load_markets(json_t *market_infos)
{
    static uint32_t update_id = 0;
    update_id += 1;

    const size_t market_num = json_array_size(market_infos);
    for (size_t i = 0; i < market_num; ++i) {
        json_t *row = json_array_get(market_infos, i);
        if (!json_is_object(row)) {
            return -__LINE__;
        }

        const char *market_name = json_string_value(json_object_get(row, "name"));
        if (market_name == NULL) {
            log_error("invalid market item, can not get market name");
            return -__LINE__;
        }

        json_t *market_item = get_market_item(market_name, row);
        if (market_item == NULL) {
            return -__LINE__;
        }

        dict_entry *entry = dict_find(dict_market, market_name);
        if (entry == NULL) {
            struct market_val val;
            memset(&val, 0, sizeof(val));
            val.id = update_id;
            val.info = market_item;
            dict_add(dict_market, (char *)market_name, &val);
            log_info("add market info: %s", market_name);
        } else {
            struct market_val *info = entry->val;
            info->id = update_id;
            if (info->info != NULL) {
                json_decref(info->info);
            }
            info->info = market_item;
        }
    }

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->id != update_id) {
            dict_delete(dict_market, entry->key);
            log_info("del market info: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);

    return 0;
}

static json_t* fetch_market_info() 
{
    sds response = http_get(settings.market_url);
    if (sdslen(response) == 0) {
        log_error("fetch keys returns empty");
        sdsfree(response);
        return NULL;
    }

    json_t *reply = json_loads(response, 0, NULL);
    if (reply == NULL) {
        log_error("parse %s response fail: %s", settings.market_url, response);
        sdsfree(response);
        return NULL;
    }

    int code = json_integer_value(json_object_get(reply, "code"));
    if (code != 0) {
        json_decref(reply);
        sdsfree(response);
        log_error("reply error: %s: %s", settings.market_url, response);
        return NULL;
    }

    json_t *result = json_object_get(reply, "data");
    if (result == NULL) {
        json_decref(reply);
        sdsfree(response);
        log_error("reply error: %s: %s", settings.market_url, response);
        return NULL;
    }
    
    json_incref(result);
    json_decref(reply);
    sdsfree(response);

    return result; 
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    entry->reply = fetch_market_info();
}

static void on_job_finish(nw_job_entry *entry)
{
    json_t *result = entry->reply;
    if (result == NULL) {
        return ;
    }

    int ret = load_markets(result);
    if (ret != 0) {
        char *result_str = json_dumps(result, 0);
        log_error("load_markets failed, result:%s", result_str);
        free(result_str);
        return ;
    }

    log_info("load markes success");
}

static void on_job_cleanup(nw_job_entry *entry)
{
    json_t *result = entry->reply;
    if (result != NULL) {
        json_decref(result);
    }
}

static void on_update_market(nw_timer *timer, void *privdata)
{
    nw_job_add(job, 0, NULL);
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

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_finish  = on_job_finish;
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;

    job = nw_job_create(&jt, 1);
    if (job == NULL)
        return -__LINE__;

    json_t *result = fetch_market_info();
    if (result == NULL)
        return -__LINE__;

    if (load_markets(result) != 0) {
        char *result_str = json_dumps(result, 0);
        log_error("load_markets failed, result:%s", result_str);
        free(result_str);
        json_decref(result);
        return -__LINE__;
    }
    json_decref(result);

    nw_timer_set(&market_update_timer, settings.market_interval, true, on_update_market, NULL);
    nw_timer_start(&market_update_timer);

    return 0;
}

json_t *get_market_info_list(void)
{
    json_t *info_list = json_object();

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *val = entry->val;
        const char *market_name = entry->key;
        json_object_set(info_list, market_name, val->info);
    }
    dict_release_iterator(iter);
    
    return info_list;
}

dict_t *get_market(void)
{
    return dict_market;
}

json_t *get_market_list(void)
{
    json_t *data = json_array();

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        json_array_append_new(data, json_string(entry->key));
    }
    dict_release_iterator(iter);

    return data;
}

bool market_exist(const char *market)
{
    return dict_find(dict_market, market) != NULL;
}


