/*
 * Description: 
 *     History: ouxiangyang, 2019/04/1, create
 */

# include "ca_market.h"
# include "ca_depth.h"
# include "ca_market.h"
# include "ca_deals.h"
# include "ca_status.h"
# include "ca_server.h"

static dict_t *dict_market   = NULL;
static rpc_clt *matchengine  = NULL;
static rpc_clt *marketindex  = NULL;
static nw_timer market_timer;
static nw_timer index_timer;
static nw_state *state_context;

struct market_val {
    int     id;
    bool    is_index;
};

static void *dict_market_val_dup(const void *key)
{
    struct market_val *obj = malloc(sizeof(struct market_val));
    memcpy(obj, key, sizeof(struct market_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    free(val);
}

static char *convert_index_name(const char *name)
{
    static char buf[100];
    snprintf(buf, sizeof(buf), "%s_INDEX", name);
    return buf;
}

static void clear_market(uint32_t update_id, bool is_index)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->id != update_id && info->is_index == is_index) {
            dict_delete(dict_market, entry->key);
            log_info("del market: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);
}

static void update_market_list(const char *name, uint32_t update_id, bool is_index)
{
    dict_entry *entry = dict_find(dict_market, name);
    if (entry == NULL) {
        struct market_val val;
        memset(&val, 0, sizeof(val));
        val.id = update_id;
        val.is_index = is_index;
        dict_add(dict_market, (char *)name, &val);
        log_info("add market: %s", name);
    } else {
        struct market_val *info = entry->val;
        info->id = update_id;
        info->is_index = is_index;
    }
}

static int on_index_list_reply(json_t *result)
{
    static uint32_t update_id = 0;
    update_id += 1;
    const char *market;
    json_t *info;
    json_object_foreach(result, market, info) {
        char *index_name = convert_index_name(market);
        update_market_list(index_name, update_id, true);
    }
    clear_market(update_id, true);

    return 0;
}

static int on_market_list_reply(json_t *result)
{
    static uint32_t update_id = 0;
    update_id += 1;

    for (size_t i = 0; i < json_array_size(result); ++i) {
        json_t *item = json_array_get(result, i);
        const char *name = json_string_value(json_object_get(item, "name"));
        update_market_list(name, update_id, false);
    }
    clear_market(update_id, false);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("market_list reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);

    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        sdsfree(reply_str);
        return;
    }

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sdsfree(reply_str);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        nw_state_del(state_context, pkg->sequence);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_MARKET_LIST:
        ret = on_market_list_reply(result);
        if (ret < 0) {
            log_error("on_market_list_reply: %d, reply: %s", ret, reply_str);
        }
        break;
    case CMD_INDEX_LIST:
        ret = on_index_list_reply(result);
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
    nw_state_del(state_context, pkg->sequence);
}

static int query_market_list(void)
{
    json_t *params = json_array();
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);

    rpc_request_json(matchengine, CMD_MARKET_LIST, state_entry->id, 0, params);
    json_decref(params);

    return 0;
}

static void on_market_timer(nw_timer *timer, void *privdata)
{
    query_market_list();
}

static int query_index_list(void)
{
    json_t *params = json_array();
    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);

    rpc_request_json(marketindex, CMD_INDEX_LIST, state_entry->id, 0, params);
    json_decref(params);

    return 0;
}

static void on_index_timer(nw_timer *timer, void *privdata)
{
    query_index_list();
}

static void on_timeout(nw_state_entry *entry)
{
    profile_inc("query_market_timeout", 1);
    log_error("query timeout, state id: %u", entry->id);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        query_market_list();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_error("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

int init_market(bool is_index)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = str_dict_hash_function;
    dt.key_compare      = str_dict_key_compare;
    dt.key_dup          = str_dict_key_dup;
    dt.key_destructor   = str_dict_key_free;
    dt.val_dup          = dict_market_val_dup;
    dt.val_destructor   = dict_market_val_free;

    dict_market = dict_create(&dt, 128);
    if (dict_market == NULL) {
        return -__LINE__;
    }

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL)
        return -__LINE__;
    if (rpc_clt_start(matchengine) < 0)
        return -__LINE__;

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;

    state_context = nw_state_create(&st, 0);
    if (state_context == NULL)
        return -__LINE__;

    nw_timer_set(&market_timer, settings.market_interval, true, on_market_timer, NULL);
    nw_timer_start(&market_timer);
    on_market_timer(NULL, NULL);

    if (is_index) {
        marketindex = rpc_clt_create(&settings.marketindex, &ct);
        if (marketindex == NULL)
            return __LINE__;
        if (rpc_clt_start(marketindex) < 0)
            return __LINE__;

        nw_timer_set(&index_timer, settings.index_interval, true, on_index_timer, NULL);
        nw_timer_start(&index_timer);
        on_index_timer(NULL, NULL);
    }

    return 0;
}

dict_t *get_market(void)
{
    return dict_market;
}

bool market_exist(const char *market)
{
    return dict_find(dict_market, market) != NULL;
}

bool market_is_index(const char *market)
{
    dict_entry *entry = dict_find(dict_market, market);
    if (entry) {
        struct market_val *info = entry->val;
        return info->is_index;
    }
    return false;
}
