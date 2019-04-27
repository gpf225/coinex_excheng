/*
 * Description: 
 *     History: ouxiangyang, 2019/04/19, create
 */

# include "aw_config.h"
# include "aw_server.h"
# include "aw_depth.h"
# include "aw_state.h"
# include "aw_cache_state.h"

static dict_t *dict_state;
static rpc_clt *cache_state;

struct state_val {
    json_t  *last;
    double  update_time;
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

// state update
static int on_sub_state_update(json_t *result_array, nw_ses *ses, rpc_pkg *pkg)
{
    const char *market = json_string_value(json_array_get(result_array, 0));
    json_t *result = json_array_get(result_array, 1);
    if (market == NULL || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        return -__LINE__;
    }

    log_trace("state update, market: %s", market);
    dict_entry *entry = dict_find(dict_state, market);
    if (entry == NULL) {
        struct state_val val;
        memset(&val, 0, sizeof(val));

        val.update_time = current_timestamp();
        val.last = result;
        json_incref(result);

        entry = dict_add(dict_state, (char *)market, &val);
        if (entry == NULL)
            return -__LINE__;
        notify_state_update();
        return 0;
    }

    struct state_val *info = entry->val;
    char *last_str = NULL;
    if (info->last)
        last_str = json_dumps(info->last, JSON_SORT_KEYS);
    char *curr_str = json_dumps(result, JSON_SORT_KEYS);

    if (info->last == NULL || strcmp(last_str, curr_str) != 0) {
        if (info->last)
            json_decref(info->last);
        info->last = result;

        json_incref(result);
        info->update_time = current_timestamp();
        notify_state_update();
    }

    if (last_str != NULL)
        free(last_str);
    free(curr_str);

    return 0;
}

json_t *get_state_notify_full(double last_notify)
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

json_t *get_state_notify_list(list_t *list, double last_notify)
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
        send_error_invalid_argument(ses, id);
        return;
    }

    const char *market = json_string_value(json_array_get(params, 0));
    if (!market) {
        send_error_invalid_argument(ses, id);
        return;
    }

    bool is_reply = false;
    dict_entry *entry = dict_find(dict_state, market);
    if (entry != NULL) {
        struct state_val *val = entry->val;
        if (val->last != NULL) {
            is_reply = true;
            send_result(ses, id, val->last);
        }
    }

    if (!is_reply) {
        reply_result_null(ses, id);
        log_error("state not find result, market: %s", market);
    }

    return;
}

json_t *get_state(const char *market)
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

int init_cache_state(void)
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
    dt.hash_function  = dict_market_hash_func;
    dt.key_compare    = dict_market_key_compare;
    dt.key_dup        = dict_market_key_dup;
    dt.key_destructor = dict_market_key_free;
    dt.val_dup        = dict_state_val_dup;
    dt.val_destructor = dict_state_val_free;
    dict_state = dict_create(&dt, 64);
    if (dict_state == NULL)
        return -__LINE__;

    return 0;
}

