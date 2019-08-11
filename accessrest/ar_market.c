/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2018/12/18, create
 */

# include "ar_market.h"
# include "ar_server.h"
# include "nw_job.h"
# include "ut_log.h"
# include "nw_state.h"

# include <curl/curl.h>

static const long HTTP_TIMEOUT = 2000L;

static dict_t *dict_market = NULL;
static nw_job *job = NULL;
static nw_timer market_update_timer;
static nw_timer index_update_timer;
static json_t *market_list = NULL;
static json_t *market_info = NULL;
static nw_state *state;

static rpc_clt *marketindex;

struct market_val {
    int     id;
    bool    is_index;
    json_t  *info;
};

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
        log_error("get %s fail: %s", url, curl_easy_strerror(ret));
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

static int is_index_market(const char *name)
{	
    if (name && strlen(name) > 6 && strcmp(name + strlen(name) - 6, "_INDEX") == 0) {
        return true;
    }
    return false;
}

static void update_market_info_list(void)
{
    json_t *info = json_object();

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *val = entry->val;
        const char *market_name = entry->key;
        if (is_index_market(market_name)) {
            continue;
        }
        json_object_set(info, market_name, val->info);
    }
    dict_release_iterator(iter);

    if (market_info != NULL)
        json_decref(market_info);
    market_info = info;
}

static void update_market_list(void)
{
    json_t *list = json_array();

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market_name = entry->key;
        if (is_index_market(market_name)) {
            continue;
        }
        json_array_append_new(list, json_string(entry->key));
    }
    dict_release_iterator(iter);

    if (market_list != NULL)
        json_decref(market_list);
    market_list = list;
}

static void clear_market(uint32_t update_id, bool is_index)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->id != update_id && info->is_index == is_index) {
            dict_delete(dict_market, entry->key);
            log_info("del market info: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);
}

static int load_markets(json_t *result)
{
    static uint32_t update_id = 0;
    update_id += 1;

    const size_t market_num = json_array_size(result);
    for (size_t i = 0; i < market_num; ++i) {
        json_t *row = json_array_get(result, i);
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
            val.is_index = false;
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

    clear_market(update_id, false);

    update_market_list();
    update_market_info_list();

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

static int query_index_list(void)
{
    json_t *params = json_array();
    nw_state_entry *state_entry = nw_state_add(state, settings.backend_timeout, 0);

    rpc_request_json(marketindex, CMD_INDEX_LIST, state_entry->id, 0, params);
    json_decref(params);
    return 0;
}

static void on_index_update_timer(nw_timer *timer, void *privdata)
{
    query_index_list();
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        query_index_list();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static char *convert_index_name(const char *name)
{
    static char buf[100];
    snprintf(buf, sizeof(buf), "%s_INDEX", name);
    return buf;
}

static int update_index_list(json_t *result)
{
    static uint32_t update_id = 0;
    update_id += 1;
    const char *market;
    json_t *info;
    json_object_foreach(result, market, info) {
        char *index_name = convert_index_name(market);
        dict_entry *entry = dict_find(dict_market, index_name);
        if (entry == NULL) {
            struct market_val val;
            memset(&val, 0, sizeof(val));
            val.id = update_id;
            val.is_index = true;
            dict_add(dict_market, (char *)index_name, &val);
            log_info("add market: %s", index_name);
        } else {
            struct market_val *info = entry->val;
            info->id = update_id;
            info->is_index = true;
        }
    }
    clear_market(update_id, true);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("market_list reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);

    nw_state_entry *entry = nw_state_get(state, pkg->sequence);
    if (entry == NULL) {
        sdsfree(reply_str);
        return;
    }

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sdsfree(reply_str);
        nw_state_del(state, pkg->sequence);
        return;
    }

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        nw_state_del(state, pkg->sequence);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_INDEX_LIST:
        ret = update_index_list(result);
        if (ret < 0) {
            log_error("on_index_list_reply: %d, reply: %s", ret, reply_str);
        }
        break;
    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    sdsfree(reply_str);
    json_decref(reply);
    nw_state_del(state, pkg->sequence);
}

static void on_state_timeout(nw_state_entry *entry)
{
    profile_inc("query_index_list timeout", 1);
    log_error("query timeout, state id: %u", entry->id);
}

int init_market(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_dup        = dict_market_val_dup;
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

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_state_timeout;
    state = nw_state_create(&st, 0);
    if (state == NULL)
        return -__LINE__;

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    marketindex = rpc_clt_create(&settings.marketindex, &ct);
    if (marketindex == NULL)
        return -__LINE__;
    if (rpc_clt_start(marketindex) < 0)
        return -__LINE__;

    nw_timer_set(&market_update_timer, settings.market_interval, true, on_update_market, NULL);
    nw_timer_start(&market_update_timer);

    nw_timer_set(&index_update_timer, settings.index_interval, true, on_index_update_timer, NULL);
    nw_timer_start(&index_update_timer);

    return 0;
}

json_t *get_market_list(void)
{
    return json_incref(market_list);
}

json_t *get_market_info_list(void)
{
    return json_incref(market_info);
}

dict_t *get_market(void)
{
    return dict_market;
}

bool market_exist(const char *market)
{
    return dict_find(dict_market, market) != NULL;
}

json_t* get_market_detail(const char *market)
{
    dict_entry *entry =  dict_find(dict_market, market);
    if (entry == NULL)
        return NULL;

    struct market_val *info = entry->val;
    if (info->is_index)
        return NULL;

    return json_incref(info->info);
}