/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/18, create
 */


# include "ar_config.h"
# include "ar_depth.h"

static dict_t *dict_state;
static rpc_clt *longpoll;

struct state_val {
    json_t *last;
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

static int on_market_status_reply(json_t *result)
{
    const char *key;
    json_t *value;
    int count = 0;
    json_object_foreach(result, key, value) {
        ++count;
        dict_entry *entry = dict_find(dict_state, key);
        if (entry == NULL) {
            struct state_val val;
            memset(&val, 0, sizeof(val));
            val.last = value;
            json_incref(val.last);

            dict_add(dict_state, (char*)key, &val);
            continue;
        }

        struct state_val *val = entry->val;
        json_decref(val->last);
        val->last = value;
        json_incref(val->last);
    }
    
    log_trace("updated %d market state", count);
    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    ut_rpc_reply_t *rpc_reply = reply_load(pkg->body, pkg->body_size);
    if (rpc_reply == NULL) {
        log_fatal("reply_load returns NULL, system maybe lack memory");
        return ;
    }

    do {
        if (!reply_valid(rpc_reply)) {
            REPLY_INVALID_LOG(ses, pkg);
            break;
        }
        if (!reply_ok(rpc_reply)) {
            REPLY_ERROR_LOG(ses, pkg);
            break;
        }
        
        if (pkg->command == CMD_LP_STATE_UPDATE) {
            on_market_status_reply(rpc_reply->result);
        } else {
            if (pkg->command != CMD_LP_STATE_SUBSCRIBE) {
                log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
            }
        }
    } while(0);

    reply_release(rpc_reply);
}

static void subscribe_state(void)
{
    json_t *params = json_array();
    char *params_str = json_dumps(params, 0);
    json_decref(params);

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_LP_STATE_SUBSCRIBE;
    pkg.sequence  = 0;
    pkg.body      = params_str;
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(longpoll, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s", nw_sock_human_addr(rpc_clt_peer_addr(longpoll)), pkg.command, pkg.sequence, (char *)pkg.body);

    free(params_str);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        subscribe_state();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
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

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    longpoll = rpc_clt_create(&settings.longpoll, &ct);
    if (longpoll == NULL) {
        return -__LINE__;
    }
    if (rpc_clt_start(longpoll) < 0) {
        return -__LINE__;
    }

    return 0;
}

static void set_sell_and_buy(const char *market, json_t *ticker)
{
    json_t *depth_result = depth_get_last_one(market);
    if (depth_result == NULL) {
        json_object_set_new(ticker, "buy", json_string("0"));
        json_object_set_new(ticker, "buy_amount", json_string("0"));
        json_object_set_new(ticker, "sell", json_string("0"));
        json_object_set_new(ticker, "sell_amount", json_string("0"));
        return ;
    }

    json_t *bids = json_object_get(depth_result, "bids");
    if (json_array_size(bids) == 1) {
        json_t *buy = json_array_get(bids, 0);
        json_object_set(ticker, "buy", json_array_get(buy, 0));
        json_object_set(ticker, "buy_amount", json_array_get(buy, 1));
    } else {
        json_object_set_new(ticker, "buy", json_string("0"));
        json_object_set_new(ticker, "buy_amount", json_string("0"));
    }

    json_t *asks = json_object_get(depth_result, "asks");
    if (json_array_size(asks) == 1) {
        json_t *sell = json_array_get(asks, 0);
        json_object_set(ticker, "sell", json_array_get(sell, 0));
        json_object_set(ticker, "sell_amount", json_array_get(sell, 1));
    } else {
        json_object_set_new(ticker, "sell", json_string("0"));
        json_object_set_new(ticker, "sell_amount", json_string("0"));
    }
    
    json_decref(depth_result);

    return ;
}

json_t *get_market_ticker(const void *market)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        return NULL;
    }
    struct state_val *info = entry->val;
    if (info->last == NULL) {
        return NULL;
    }
    set_sell_and_buy(market, info->last);

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
        if (info->last == NULL) {
            continue;
        }
        set_sell_and_buy(market, info->last);
        json_object_set(ticker, market, info->last);
    }
    dict_release_iterator(iter);

    json_t *data = json_object();
    json_object_set_new(data, "date", json_integer((uint64_t)(current_timestamp() * 1000)));
    json_object_set_new(data, "ticker", ticker);

    return data;
}

void fini_ticker(void)
{
    dict_release(dict_state);
}