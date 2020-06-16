/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# include "aw_config.h"
# include "aw_asset.h"
# include "aw_server.h"

static dict_t *dict_sub;
static rpc_clt *matchengine;
static nw_state *state_context;

struct sub_unit {
    void *ses;
    char asset[ASSET_NAME_MAX_LEN + 1];
};

struct state_data {
    uint32_t user_id;
    uint32_t account;
    char asset[ASSET_NAME_MAX_LEN + 1];
};

static void dict_sub_val_free(void *val)
{
    list_release(val);
}

static int list_node_compare(const void *value1, const void *value2)
{
    return memcmp(value1, value2, sizeof(struct sub_unit));
}

static void *list_node_dup(void *value)
{
    struct sub_unit *obj = malloc(sizeof(struct sub_unit));
    memcpy(obj, value, sizeof(struct sub_unit));
    return obj;
}

static void list_node_free(void *value)
{
    free(value);
}

static void on_timeout(nw_state_entry *entry)
{
    profile_inc("query_balance_timeout", 1);
    log_error("query balance timeout, state id: %u", entry->id);
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

static int on_balance_query_reply(struct state_data *state, json_t *result)
{
    void *key = (void *)(uintptr_t)state->user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL)
        return 0 ;

    json_t *params = json_array();
    json_array_append(params, result);
    json_array_append_new(params, json_integer(state->account));

    size_t count = 0;
    list_t *list = entry->val;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        struct sub_unit *unit = node->value;
        if (strlen(unit->asset) == 0 || strcmp(unit->asset, state->asset) == 0) {
            ws_send_notify(unit->ses, "asset.update", params);
            count += 1;
        }
    }
    list_release_iterator(iter);
    json_decref(params);
    profile_inc("asset.update", count);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u, reply: %s",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, reply_str);
    nw_state_entry *entry = nw_state_get(state_context, pkg->sequence);
    if (entry == NULL) {
        sdsfree(reply_str);
        return;
    }
    struct state_data *state = entry->data;

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
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
    case CMD_ASSET_QUERY:
        ret = on_balance_query_reply(state, result);
        if (ret < 0) {
            log_error("on_balance_query_reply fail: %d, reply: %s", ret, reply_str);
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

int init_asset(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func; 
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_destructor = dict_sub_val_free;

    dict_sub = dict_create(&dt, 1024);
    if (dict_sub == NULL)
        return -__LINE__;

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

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL)
        return -__LINE__;

    return 0;
}

int asset_subscribe(uint32_t user_id, nw_ses *ses, const char *asset)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
        list_type lt;
        memset(&lt, 0, sizeof(lt));
        lt.dup = list_node_dup;
        lt.free = list_node_free;
        lt.compare = list_node_compare;
        list_t *list = list_create(&lt);
        if (list == NULL)
            return -__LINE__;
        entry = dict_add(dict_sub, key, list);
        if (entry == NULL)
            return -__LINE__;
    }

    list_t *list = entry->val;
    struct sub_unit unit;
    memset(&unit, 0, sizeof(unit));
    unit.ses = ses;
    sstrncpy(unit.asset, asset, sizeof(unit.asset));

    if (list_find(list, &unit) != NULL)
        return 0;
    if (list_add_node_tail(list, &unit) == NULL)
        return -__LINE__;

    return 0;
}

int asset_unsubscribe(uint32_t user_id, nw_ses *ses)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL)
        return 0;

    list_t *list = entry->val;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        struct sub_unit *unit = node->value;
        if (unit->ses == ses) {
            list_del(list, node);
        }
    }
    list_release_iterator(iter);

    if (list->len == 0) {
        dict_delete(dict_sub, key);
    }

    return 0;
}

int asset_on_update(uint32_t user_id, uint32_t account, const char *asset, const char *available, const char *frozen)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL)
        return 0 ;

    json_t *params = json_array();
    json_t *result = json_object();
    json_t *unit = json_object();

    json_object_set_new(unit, "available", json_string(available));
    json_object_set_new(unit, "frozen", json_string(frozen));
    json_object_set_new(result, asset, unit);

    json_array_append_new(params, result);
    json_array_append_new(params, json_integer(account));

    size_t count = 0;
    list_t *list = entry->val;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        struct sub_unit *unit = node->value;
            ws_send_notify(unit->ses, "asset.update", params);
            count += 1;
    }
    list_release_iterator(iter);

    json_decref(params);
    profile_inc("asset.update", count);

    return 0;
}

size_t asset_subscribe_number(void)
{
    return dict_size(dict_sub);
}

