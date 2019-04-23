/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_config.h"
# include "ca_status.h"
# include "ca_server.h"
# include "ca_market.h"

static rpc_clt *marketprice;
static nw_state *state_context;

static dict_t *dict_status_sub;
static nw_timer timer;

struct dict_status_key {
    char        market[MARKET_NAME_MAX_LEN];
};

struct dict_status_sub_val {
    dict_t      *sessions; 
    json_t      *sub_last;
};

struct state_data {
    char        market[MARKET_NAME_MAX_LEN];
};

static uint32_t dict_status_sub_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_status_key));
}

static int dict_status_sub_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct dict_status_key));
}

static void *dict_status_sub_key_dup(const void *key)
{
    struct dict_status_key *obj = malloc(sizeof(struct dict_status_key));
    memcpy(obj, key, sizeof(struct dict_status_key));
    return obj;
}

static void dict_status_sub_key_free(void *key)
{
    free(key);
}

static void *dict_status_sub_val_dup(const void *val)
{
    struct dict_status_sub_val *obj = malloc(sizeof(struct dict_status_sub_val));
    memcpy(obj, val, sizeof(struct dict_status_sub_val));
    return obj;
}

static void dict_status_sub_val_free(void *key)
{
    struct dict_status_sub_val *obj = key;
    if (obj->sessions != NULL)
        dict_release(obj->sessions);
    free(obj);
}

static dict_t* dict_create_status_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare   = dict_ses_hash_compare;

    return dict_create(&dt, 16);
}

static void on_timeout(nw_state_entry *entry)
{
    log_error("state id: %u timeout", entry->id);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static int notify_message(nw_ses *ses, int command, json_t *message)
{
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.command = command;

    return reply_result(ses, &pkg, message);
}

static int status_sub_reply(const char *market, json_t *result)
{
    struct dict_status_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_status_sub, &key);
    if (entry == NULL)
        return -__LINE__;

    struct dict_status_sub_val *obj = entry->val;
    char *last_str = NULL;

    if (obj->sub_last)
        last_str = json_dumps(obj->sub_last, JSON_SORT_KEYS);
    char *curr_str = json_dumps(result, JSON_SORT_KEYS);

    if (obj->sub_last == NULL || strcmp(last_str, curr_str) != 0) {
        if (obj->sub_last)
            json_decref(obj->sub_last);
        obj->sub_last = result;
        json_incref(result);

        json_t *params = json_array();
        json_array_append_new(params, json_string(market));
        json_array_append(params, result);

        dict_iterator *iter = dict_get_iterator(obj->sessions);
        while ((entry = dict_next(iter)) != NULL) {
            nw_ses *ses = entry->key;
            notify_message(ses, CMD_CACHE_STATUS_UPDATE, params);
        }
        dict_release_iterator(iter);

        json_decref(params);
    }

    if (last_str != NULL)
        free(last_str);
    free(curr_str);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        log_error("nw_state_get get null");
        return;
    }

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    bool is_error = false;
    struct state_data *state = entry->data;

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        is_error = true;
        sdsfree(reply_str);
    }

    switch (pkg->command) {
    case CMD_MARKET_STATUS:
        if (!is_error) {
            status_sub_reply(state->market, result);
        }
        break;
    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    json_decref(reply);
    nw_state_del(state_context, pkg->sequence);
}

static int status_request(const char *market)
{
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);

    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(86400));

    rpc_pkg req_pkg;
    memset(&req_pkg, 0, sizeof(req_pkg));
    req_pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    req_pkg.command   = CMD_MARKET_STATUS;
    req_pkg.sequence  = state_entry->id;
    req_pkg.body      = json_dumps(params, 0);
    req_pkg.body_size = strlen(req_pkg.body);

    rpc_clt_send(marketprice, &req_pkg);
    free(req_pkg.body);
    json_decref(params);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata) 
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_status_sub);

    while ((entry = dict_next(iter)) != NULL) {
        struct dict_status_key *key = entry->key;
        struct dict_status_sub_val *val = entry->val;

        if (dict_size(val->sessions) == 0) {
            log_info("state on_timer sessions num is 0, market: %s", key->market);
            continue;
        }

        log_trace("state sub request, market: %s", key->market);
        status_request(key->market);
    }
    dict_release_iterator(iter);
}

static void send_last_state(nw_ses *ses, const char *market)
{
    struct dict_status_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_status_sub, &key);
    if (entry != NULL) {
        struct dict_status_sub_val *obj = entry->val;
        json_t *params = json_array();
        json_array_append_new(params, json_string(market));
        json_array_append    (params, obj->sub_last);
        notify_message(ses, CMD_CACHE_STATUS_UPDATE, params);
        json_decref(params);
    }

    return;
}

int status_subscribe(nw_ses *ses, const char *market)
{
    log_info("depth subscribe, market: %s", market);
    struct dict_status_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_status_sub, &key);
    if (entry == NULL) {
        struct dict_status_sub_val val;
        memset(&val, 0, sizeof(val));

        val.sessions = dict_create_status_session();
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_status_sub, &key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct dict_status_sub_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    send_last_state(ses, market);
    return 0;  
}

void status_unsubscribe(nw_ses *ses, const char *market)
{
    struct dict_status_key key;
    memset(&key, 0, sizeof(key));
    strncpy(key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_status_sub, &key);
    if (entry == NULL)
        return;

    struct dict_status_sub_val *val = entry->val;
    dict_delete(val->sessions, ses);

    return;
}

void status_unsubscribe_all(nw_ses *ses)
{
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_status_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_status_sub_val *val = entry->val;
        dict_delete(val->sessions, ses);
    }
    dict_release_iterator(iter);

    return;
}

int init_status(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL) {
        log_stderr("rpc_clt_create failed");
        return -__LINE__;
    }
    if (rpc_clt_start(marketprice) < 0) {
        log_stderr("rpc_clt_start failed");
        return -__LINE__;
    }

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL) {
        log_stderr("nw_state_create failed");
        return -__LINE__;
    }

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function   = dict_status_sub_hash_function;
    dt.key_compare     = dict_status_sub_key_compare;
    dt.key_dup         = dict_status_sub_key_dup;
    dt.key_destructor  = dict_status_sub_key_free;
    dt.val_dup         = dict_status_sub_val_dup;
    dt.val_destructor  = dict_status_sub_val_free;

    dict_status_sub = dict_create(&dt, 256);
    if (dict_status_sub == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }

    nw_timer_set(&timer, settings.sub_status_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}
