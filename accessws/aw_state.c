/*
 * Description: 
 *     History: ouxiangyang, 2019/04/19, create
 */

# include "aw_config.h"
# include "aw_server.h"
# include "aw_depth.h"
# include "aw_state.h"

static dict_t *dict_state;
static dict_t *dict_session;

static rpc_clt *cache_state;

struct state_val {
    int     id;
    json_t  *last;
    double  update_time;
};

static void *list_market_dup(void *val)
{
    return strdup(val);
}

static void list_market_free(void *val)
{
    free(val);
}

static void dict_ses_val_free(void *val)
{
    if (val) {
        list_release(val);
    }
}

//dict state
static void *dict_state_val_dup(const void *key)
{
    struct state_val *obj = malloc(sizeof(struct state_val));
    memcpy(obj, key, sizeof(struct state_val));
    return obj;
}

static void dict_state_val_free(void *val)
{
    struct state_val *obj = val;
    if (obj->last)
        json_decref(obj->last);
    free(obj);
}

static json_t *get_state_notify_full(double last_notify)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_state);
    while ((entry = dict_next(iter)) != NULL) {
        struct state_val *info = entry->val;
        if (info->last && info->update_time > last_notify) {
            json_object_set(result, entry->key, info->last);
        }
    }
    dict_release_iterator(iter);

    return result;
}

static json_t *get_state_notify_list(list_t *list, double last_notify)
{
    json_t *result = json_object();
    list_node *node;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        dict_entry *entry = dict_find(dict_state, node->value);
        if (!entry) {
            continue;
        }
        struct state_val *info = entry->val;
        if (info->last && info->update_time > last_notify) {
            json_object_set(result, entry->key, info->last);
        }
    }
    list_release_iterator(iter);

    return result;
}

static void notify_state_update(void)
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
                full_result = get_state_notify_full(last_notify);
            result = full_result;
        } else {
            result = get_state_notify_list(entry->val, last_notify);
        }
        if (json_object_size(result) != 0) {
            json_t *params = json_array();
            json_array_append(params, result);
            ws_send_notify(entry->key, "state.update", params);
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
}

// state update
static int on_sub_state_update(json_t *result_array, nw_ses *ses, rpc_pkg *pkg)
{
    static uint32_t update_id = 0;
    update_id += 1;

    const size_t state_num = json_array_size(result_array);
    log_trace("state update, state_num: %zd", state_num);

    for (size_t i = 0; i < state_num; ++i) {
        json_t *row = json_array_get(result_array, i);
        if (!json_is_object(row)) {
            return -__LINE__;
        }

        const char *market = json_string_value(json_object_get(row, "name"));
        if (market == NULL) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
            sdsfree(reply_str);
            continue;
        }

        json_t *state = json_object_get(row, "state");
        if (state == NULL) {
            sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
            log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
            sdsfree(reply_str);
            continue;
        }

        // add to dict_state
        dict_entry *entry = dict_find(dict_state, market);
        if (entry == NULL) {
            struct state_val val;
            memset(&val, 0, sizeof(val));
            entry = dict_add(dict_state, (char *)market, &val);
        }

        struct state_val *info = entry->val;
        info->id = update_id;

        char *last_str = NULL;
        if (info->last)
            last_str = json_dumps(info->last, JSON_SORT_KEYS);
        char *curr_str = json_dumps(state, JSON_SORT_KEYS);

        if (info->last == NULL || strcmp(last_str, curr_str) != 0) {
            if (info->last)
                json_decref(info->last);
            info->last = state;
            json_incref(state);
            info->update_time = current_timestamp();
        }

        if (last_str != NULL)
            free(last_str);
        free(curr_str);
    }

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_state);
    while ((entry = dict_next(iter)) != NULL) {
        struct state_val *info = entry->val;
        if (info->id != update_id) {
            log_info("del market state: %s", (char *)entry->key);
            dict_delete(dict_state, entry->key);
        }
    }
    dict_release_iterator(iter);

    notify_state_update();

    return 0;
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

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence);
    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (!reply) {
        log_error("json_loadb fail");
        goto clean;
    }
    json_t *error = json_object_get(reply, "error");
    if (!error) {
        log_error("error param not find");
        goto clean;
    }
    if (!json_is_null(error)) {
        log_error("error is not null");
        goto clean;
    }

    json_t *result = json_object_get(reply, "result");
    if (!result) {
        log_error("result param not find");
        goto clean;
    }

    switch (pkg->command) {
    case CMD_CACHE_STATUS_UPDATE:
        on_sub_state_update(result, ses, pkg);
        break;
    default:
        break;
    }

clean:
    if (reply)
        json_decref(reply);

    return;
}

void direct_state_reply(nw_ses *ses, json_t *params, int64_t id)
{
    if (json_array_size(params) != 2) {
        ws_send_error_invalid_argument(ses, id);
        return;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market) {
        ws_send_error_invalid_argument(ses, id);
        return;
    }

    bool is_reply = false;
    dict_entry *entry = dict_find(dict_state, market);
    if (entry != NULL) {
        struct state_val *val = entry->val;
        if (val->last != NULL) {
            is_reply = true;
            ws_send_result(ses, id, val->last);
        }
    }

    if (!is_reply) {
        ws_send_error_direct_result_null(ses, id);
        log_error("state not find result, market: %s", market);
    }

    return;
}

bool judege_state_period_is_day(json_t *params)
{
    if (json_array_size(params) != 2)
        return false;

    int period = json_integer_value(json_array_get(params, 1));
    if (period == 86400) {
        return true;
    } else {
        return false;
    }
}

int init_state(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cache_state = rpc_clt_create(&settings.cache_state, &ct);
    if (cache_state == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache_state) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_dup        = dict_state_val_dup;
    dt.val_destructor = dict_state_val_free;
    dict_state = dict_create(&dt, 64);
    if (dict_state == NULL)
        return -__LINE__;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = ptr_dict_hash_func;
    dt.key_compare    = ptr_dict_key_compare;
    dt.val_destructor = dict_ses_val_free;

    dict_session = dict_create(&dt, 64);
    if (dict_session == NULL)
        return -__LINE__;

    return 0;
}

int state_subscribe(nw_ses *ses, json_t *market_list)
{
    dict_delete(dict_session, ses);
    if (json_array_size(market_list) == 0) {
        dict_add(dict_session, ses, NULL);
        return 0;
    }

    list_type type;
    memset(&type, 0, sizeof(type));
    type.dup = list_market_dup;
    type.free = list_market_free;

    list_t *list = list_create(&type);
    if (list == NULL)
        return -__LINE__;
    for (size_t i = 0; i < json_array_size(market_list); ++i) {
        const char *name = json_string_value(json_array_get(market_list, i));
        if (name == NULL)
            continue;

        if (market_exists(name)) {
            list_add_node_tail(list, (char *)name);
        } else {
            log_info("market: %s not exist", name);
        }
    }
    dict_add(dict_session, ses, list);

    return 0;
}

int state_unsubscribe(nw_ses *ses)
{
    return dict_delete(dict_session, ses);
}

static json_t *get_state(const char *market)
{
    dict_entry *entry = dict_find(dict_state, market);
    if (entry != NULL) {
        struct state_val *val = entry->val;
        if (val->last != NULL) {
            return val->last;
        }
    }

    return NULL;
}

int state_send_last(nw_ses *ses)
{
    dict_entry *entry = dict_find(dict_session, ses);
    if (entry == NULL) {
        return 0;
    }

    json_t *result = json_object();
    if (entry->val == NULL) {
        dict_entry *entry;
        dict_iterator *iter = dict_get_iterator(dict_state);
        while ((entry = dict_next(iter)) != NULL) {
            json_t *state = get_state(entry->key);
            if (state != NULL) {
                json_object_set(result, entry->key, state);
            }
        }
        dict_release_iterator(iter);
    } else {
        list_node *node;
        list_iter *iter = list_get_iterator(entry->val, LIST_START_HEAD);
        while ((node = list_next(iter)) != NULL) {
            dict_entry *entry = dict_find(dict_state, node->value);
            if (!entry) {
                list_del(entry->val, node);
                continue;
            }
            json_t *state = get_state(entry->key);
            if (state != NULL) {
                json_object_set(result, entry->key, state);
            }
        }
        list_release_iterator(iter);
    }

    json_t *params = json_array();
    json_array_append_new(params, result);
    ws_send_notify(ses, "state.update", params);
    json_decref(params);

    return 0;
}

size_t state_subscribe_number(void)
{
    return dict_size(dict_session);
}

bool market_exists(const char *market)
{
    return (dict_find(dict_state, market) != NULL);
}

