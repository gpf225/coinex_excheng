/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/27, create
 */

# include "aw_config.h"
# include "aw_asset.h"
# include "aw_server.h"

static dict_t *dict_sub;
static dict_t *dict_delay;
static nw_timer timer;

struct sub_unit {
    void *ses;
    char asset[ASSET_NAME_MAX_LEN + 1];
    bool delay;
};

struct delay_key {
    void *ses;
    char asset[ASSET_NAME_MAX_LEN + 1];
    uint32_t account;
};

struct delay_val {
    json_t *result;
};

static void *dict_delay_val_dup(const void *val)
{
    struct delay_val *obj = malloc(sizeof(struct delay_val));
    memcpy(obj, val, sizeof(struct delay_val));
    return obj;
}

static void dict_delay_val_free(void *val)
{
    struct delay_val *obj = val;  
    if (obj->result != NULL) 
        json_decref(obj->result);
    free(obj);
}

static uint32_t dict_delay_key_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct delay_key));
}

static int dict_delay_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct delay_key));
}

static void *dict_delay_key_dup(const void *key)
{
    struct delay_key *obj = malloc(sizeof(struct delay_key));
    memcpy(obj, key, sizeof(struct delay_key));
    return obj;
}

static void dict_delay_key_free(void *key)
{
    free(key);
}

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

static void on_timer(nw_timer *timer, void *privdata)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_delay);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct delay_key *key = entry->key;
        struct delay_val *val = entry->val;
        json_t *params = json_array();
        json_array_append(params, val->result);
        json_array_append_new(params, json_integer(key->account));
        ws_send_notify(key->ses, "asset.update", params);
        dict_delete(dict_delay, key);
        json_decref(params);
        count++;
    }
    dict_release_iterator(iter);
    profile_inc("asset.update", count);
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

    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_delay_key_hash_function;
    dt.key_compare    = dict_delay_key_compare;
    dt.key_dup        = dict_delay_key_dup;
    dt.key_destructor = dict_delay_key_free;
    dt.val_dup        = dict_delay_val_dup;
    dt.val_destructor = dict_delay_val_free;
    dict_delay = dict_create(&dt, 64);
    if (dict_delay == NULL)
        return -__LINE__;

    nw_timer_set(&timer, settings.asset_delay, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int asset_subscribe(uint32_t user_id, nw_ses *ses, const char *asset, bool delay)
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
    unit.delay = delay;
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

    dict_iterator *delay_iter = dict_get_iterator(dict_delay);
    while ((entry = dict_next(delay_iter)) != NULL) {
        struct delay_key *obj = entry->key;
        if (obj->ses == ses) {
            dict_delete(dict_delay, entry->key);
        }
    }
    dict_release_iterator(delay_iter);

    if (list->len == 0) {
        dict_delete(dict_sub, key);
    }

    return 0;
}

static int delay_update(nw_ses *ses, uint32_t account, const char *asset, json_t *result)
{
    struct delay_key key;
    memset(&key, 0, sizeof(struct delay_key));
    key.ses = ses;
    key.account = account;
    sstrncpy(key.asset, asset, sizeof(key.asset));
    dict_entry *entry = dict_find(dict_delay, &key);
    if (entry == NULL) {
        struct delay_val val;
        memset(&val, 0, sizeof(struct delay_val));
        entry = dict_add(dict_delay, &key, &val);
        if (entry == NULL)
            return -__LINE__;
    }
    struct delay_val *val = entry->val;
    json_incref(result);
    if (val->result != NULL) {
        json_decref(val->result);
    }
    val->result = result;
    return 0;
}

int asset_on_update(uint32_t user_id, uint32_t account, const char *asset, const char *available, const char *frozen, double timestamp)
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
    json_object_set_new(unit, "timestamp", json_real(timestamp));
    json_object_set_new(result, asset, unit);

    json_array_append_new(params, result);
    json_array_append_new(params, json_integer(account));

    char *notify = ws_get_notify("asset.update", params);
    sds compressed = NULL;

    size_t count = 0;
    list_t *list = entry->val;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        struct sub_unit *unit = node->value;
        if (strlen(unit->asset) == 0 || strcmp(unit->asset, asset) == 0) {
            if (unit->delay) {
                delay_update(unit->ses, account, asset, result);
            } else if (ws_ses_compress(unit->ses)) {
                if (compressed == NULL)
                    compressed = zlib_compress(notify, strlen(notify));
                ws_send_raw(unit->ses, compressed, sdslen(compressed), true);
            } else {
                ws_send_raw(unit->ses, notify, strlen(notify), false);
                count += 1;
            }
        }
    }
    list_release_iterator(iter);

    free(notify);
    if (compressed)
        sdsfree(compressed);
    json_decref(params);
    profile_inc("asset.update", count);

    return 0;
}

size_t asset_subscribe_number(void)
{
    return dict_size(dict_sub);
}

