/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/01/18, create
 */

# include "aw_asset_sub.h"
# include "aw_server.h"

static dict_t *dict_sub;
static dict_t *dict_user;
static rpc_clt *matchengine;
static nw_state *state_context;

struct state_data {
    uint32_t user_id;
    char asset[ASSET_NAME_MAX_LEN];
};

static uint32_t dict_sub_hash_func(const void *key)
{
    return (uintptr_t)key;
}

static int dict_sub_key_compare(const void *key1, const void *key2)
{
    return (uintptr_t)key1 == (uintptr_t)key2 ? 0 : 1;
}

static dict_t* create_sub_dict()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_sub_hash_func;
    dt.key_compare = dict_sub_key_compare;

    return dict_create(&dt, 1024);
}

static int list_node_compare(const void *value1, const void *value2)
{
    return (uintptr_t)value1 == (uintptr_t)value2 ? 0 : 1;
}

static list_t* create_user_list() 
{
    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare = list_node_compare;
    return list_create(&lt);
}

static uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

static int dict_ses_key_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

static void dict_user_val_free(void *val)
{
    list_release(val);
}

static dict_t* create_user_dict()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_key_compare;
    dt.val_destructor = dict_user_val_free;

    return dict_create(&dt, 1024);
}

static dict_t* create_ses_dict()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_key_compare;

    return dict_create(&dt, 1);
}

static void on_timeout(nw_state_entry *entry)
{
    log_fatal("query balance timeout, state id: %u", entry->id);
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
    if (entry == NULL) {
        return 0 ;
    }

    json_t *params = json_array();
    json_array_append_new(params, json_integer(state->user_id));
    json_array_append(params, result);
    
    int count = 0;
    dict_t *clients = entry->val;
    dict_iterator *iter = dict_get_iterator(clients);
    while ( (entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        send_notify(ses, "asset.update_sub", params);
        ++count;
    }
    dict_release_iterator(iter);
    json_decref(params);
    profile_inc("asset.update_sub", count);

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

int init_asset_sub(void)
{
    dict_sub = create_sub_dict();
    if (dict_sub == NULL) {
        return -__LINE__;
    }

    dict_user = create_user_dict();
    if (dict_user == NULL) {
        return -__LINE__;
    }

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL) {
        return -__LINE__;
    }
    if (rpc_clt_start(matchengine) < 0) {
        return -__LINE__;
    }

    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;

    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL) {
        return -__LINE__;
    }

    return 0;
}

static int add_subscribe(uint32_t user_id, nw_ses *ses)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
        dict_t *clients = create_ses_dict();
        if (clients == NULL) {
            return -__LINE__;
        }
        entry = dict_add(dict_sub, key, clients);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    dict_t *clients = entry->val;
    if (dict_add(clients, ses, NULL) == NULL) {
        return -__LINE__;
    }

    return 0;
}

static int remove_subscribe(uint32_t user_id, nw_ses *ses)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
       return 0;
    }

    dict_t *clients = entry->val;
    dict_delete(clients, ses);
    if (dict_size(clients) == 0) {
        dict_delete(dict_sub, key);
    }

    return 0;
}

int asset_subscribe_sub(nw_ses *ses, json_t *sub_users)
{
    list_t *list = create_user_list();
    if (list == NULL) {
        return -__LINE__;
    }
    for (size_t i = 0; i < json_array_size(sub_users); ++i) {
        uint32_t user_id = json_integer_value(json_array_get(sub_users, i));
        void *value = (void *)(uintptr_t)user_id;
        if (list_add_node_tail(list, value) == NULL) {
            list_release(list);
            return -__LINE__;
        }
    }
    if (dict_add(dict_user, ses, list) == NULL) {
        list_release(list);
        return -__LINE__;
    }

    for (size_t i = 0; i < json_array_size(sub_users); ++i) {
        uint32_t user_id = json_integer_value(json_array_get(sub_users, i));
        int ret = add_subscribe(user_id, ses);
        if (ret < 0) {
            dict_delete(dict_user, ses);
            return ret;
        }
    }

    return 0;
}

int asset_unsubscribe_sub(nw_ses *ses)
{
    dict_entry *entry = dict_find(dict_user, ses);
    if (entry == NULL) {
        return 0;
    }

    list_t *list = entry->val;
    int count = list_len(list);
    list_node *node = NULL;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ( (node = list_next(iter)) != NULL) {
        uint32_t user_id = (uintptr_t)node->value;
        remove_subscribe(user_id, ses);
    }
    list_release_iterator(iter);

    dict_delete(dict_user, ses);
    return count;
}

int asset_on_update_sub(uint32_t user_id, const char *asset)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub, key);
    if (entry == NULL) {
        return 0;
    }

    json_t *trade_params = json_array();
    json_array_append_new(trade_params, json_integer(user_id));
    json_array_append_new(trade_params, json_string(asset));

    nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
    struct state_data *state = state_entry->data;
    state->user_id = user_id;
    strncpy(state->asset, asset, ASSET_NAME_MAX_LEN - 1);

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_ASSET_QUERY;
    pkg.sequence  = state_entry->id;
    pkg.body      = json_dumps(trade_params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(matchengine, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s",
            nw_sock_human_addr(rpc_clt_peer_addr(matchengine)), pkg.command, pkg.sequence, (char *)pkg.body);
    free(pkg.body);
    json_decref(trade_params);

    return 0;
}

size_t asset_subscribe_sub_number(void)
{
    return dict_size(dict_sub);
}
