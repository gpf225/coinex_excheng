/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/28, create
 */

# include "aw_config.h"
# include "aw_market.h"
# include "aw_state.h"
# include "aw_server.h"
# include "aw_common_struct.h"

static dict_t *dict_market_state;
static dict_t *dict_session;
static rpc_clt *longpoll;

struct market_val {
    json_t *last;
    double  update_time;
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
    if (obj->last) 
        json_decref(obj->last);
    free(obj);
}

static int init_dict_market_state(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = common_str_hash_func;
    dt.key_dup        = common_str_const_dup;
    dt.key_destructor = common_str_free;
    dt.key_compare    = common_str_compare;
    dt.val_dup        = dict_market_val_dup;
    dt.val_destructor = dict_market_val_free;

    dict_market_state = dict_create(&dt, 64);
    if (dict_market_state == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }
    return 0;
}

static void dict_list_val_free(void *val)
{
    if (val) {
        list_release(val);
    }
}

static int init_dict_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = common_ses_hash_func;
    dt.key_compare    = common_ses_compare;
    dt.val_destructor = dict_list_val_free;

    dict_session = dict_create(&dt, 64);
    if (dict_session == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }
    return 0;
}

static json_t *get_notify_full(double last_notify)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market_state);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->last && info->update_time > last_notify) {
            json_object_set(result, entry->key, info->last);
        }
    }
    dict_release_iterator(iter);
    return result;
}

static json_t *get_notify_list(list_t *list, double last_notify)
{
    json_t *result = json_object();
    list_node *node;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        dict_entry *entry = dict_find(dict_market_state, node->value);
        if (!entry) {
            list_del(list, node);
            continue;
        }
        struct market_val *info = entry->val;
        if (info->last && info->update_time > last_notify) {
            json_object_set(result, entry->key, info->last);
        }
    }
    list_release_iterator(iter);
    return result;
}

static void notify_state(void)
{
    static double last_notify;
    size_t count = 0;
    json_t *result;
    json_t *full_result = NULL;

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_session);
    while ((entry = dict_next(iter)) != NULL) {
        if (entry->val == NULL) {
            if (full_result == NULL)
                full_result = get_notify_full(last_notify);
            result = full_result;
        } else {
            result = get_notify_list(entry->val, last_notify);
        }
        if (json_object_size(result) != 0) {
            json_t *params = json_array();
            json_array_append(params, result);
            send_notify(entry->key, "state.update", params);
            json_decref(params);
            count += 1;
        }
        if (result != full_result)
            json_decref(result);
    }
    dict_release_iterator(iter);
    if (full_result)
        json_decref(full_result);
    last_notify = current_timestamp();

    if (count) {
        profile_inc("state.update", count);
    }
    log_info("notify_state count:%zu", count);
}

static int on_market_status_reply(json_t *result)
{
    bool need_notify = false;
    const char *key;
    json_t *value;
    json_object_foreach(result, key, value) {
        dict_entry *entry = dict_find(dict_market_state, key);
        if (entry == NULL) {
            struct market_val val;
            memset(&val, 0, sizeof(val));
            val.last = value;
            json_incref(val.last);
            val.update_time = current_timestamp();

            dict_add(dict_market_state, (char*)key, &val);
            need_notify = true;
            return 0;
        }
        struct market_val *info = entry->val;
        char *last_str = NULL;
        if (info->last) {
            last_str = json_dumps(info->last, JSON_SORT_KEYS);
        }
        char *curr_str = json_dumps(value, JSON_SORT_KEYS);
        if (strcmp(last_str, curr_str) != 0) {
            json_decref(info->last);
            info->last = value;
            json_incref(info->last);
            info->update_time = current_timestamp();
            need_notify = true;
        }

        free(last_str);
        free(curr_str);
    }
    
    if (need_notify) {
         notify_state();
    }

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    //log_trace("recv pkg from: %s, cmd: %u, sequence: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, reply_str);

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        sdsfree(reply_str);
        return;
    }

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        return;
    }

    int ret;
    switch (pkg->command) {
    case CMD_LP_STATE_SUBSCRIBE:
        log_trace("market state subscribe success");
        break;
    case CMD_LP_STATE_UNSUBSCRIBE:
        log_trace("market state unsubscribe success");
        break;
    case CMD_LP_STATE_UPDATE:
        ret = on_market_status_reply(result);
        if (ret < 0) {
            log_error("on_market_status_reply: %d, reply: %s", ret, reply_str);
        }
        break;
    default:
        log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        break;
    }

    sdsfree(reply_str);
    json_decref(reply);
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

int init_state(void)
{
    int ret = init_dict_market_state();
    if (ret < 0) {
        return ret;
    }
    ret = init_dict_session();
    if (ret < 0) {
        return ret;
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

int state_subscribe(nw_ses *ses, json_t *market_list)
{
    dict_delete(dict_session, ses);
    if (json_array_size(market_list) == 0) {
        dict_add(dict_session, ses, NULL);
        return 0;
    }

    list_t *list = common_create_str_list();
    if (list == NULL)
        return -__LINE__;
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        const char *name = json_string_value(json_array_get(market_list, i));
        if (name == NULL) {
            continue;
        }
        if (market_exists(name)) {
            list_add_node_tail(list, (char *)name);
        }
    }
    if (list_len(list) == 0) {
        return -__LINE__;
    }
    dict_add(dict_session, ses, list);

    return 0;
}

int state_unsubscribe(nw_ses *ses)
{
    return dict_delete(dict_session, ses);
}

int state_send_last(nw_ses *ses)
{
    dict_entry *entry = dict_find(dict_session, ses);
    if (entry == NULL) {
        return 0;
    }

    json_t *result = NULL;
    if (entry->val == NULL) {
        result = get_notify_full(0);
    } else {
        result = get_notify_list(entry->val, 0);
    }

    json_t *params = json_array();
    json_array_append_new(params, result);
    send_notify(ses, "state.update", params);
    json_decref(params);

    return 0;
}

size_t state_subscribe_number(void)
{
    return dict_size(dict_session);
}