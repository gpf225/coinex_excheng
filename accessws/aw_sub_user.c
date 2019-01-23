/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/01/22, create
 */

# include "aw_sub_user.h"

static dict_t *dict_users = NULL;

typedef struct user_key {
    uint32_t user_id;
    nw_ses *ses;
}user_key;

static uint32_t dict_user_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct user_key));
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct user_key));
}

static void *dict_user_key_dup(const void *key)
{
    struct user_key *obj = malloc(sizeof(struct user_key));
    memcpy(obj, key, sizeof(struct user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void dict_user_val_free(void *val)
{
    dict_release(val);
}

static uint32_t dict_user_id_hash_func(const void *key)
{
    return (uintptr_t)key;
}

static int dict_user_id_compare(const void *key1, const void *key2)
{
    return (uintptr_t)key1 == (uintptr_t)key2 ? 0 : 1;
}

static dict_t* create_user_id_dict()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_user_id_hash_func;
    dt.key_compare = dict_user_id_compare;

    return dict_create(&dt, 8);
}

int sub_user_add(uint32_t user_id, nw_ses *ses, json_t *params)
{
    dict_t *sub_users = create_user_id_dict();
    if (sub_users == NULL) {
        return -__LINE__;
    }

    for (size_t i = 0; i < json_array_size(params); ++i) {
        uint32_t user_id = json_integer_value(json_array_get(params, i));
        void *key = (void *)(uintptr_t)user_id;
        if (dict_add(sub_users, key, NULL) == NULL) {
            dict_release(sub_users);
            return -__LINE__;
        }
    }

    user_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.ses = ses;
    if (dict_find(dict_users, &key) != NULL) {
        log_warn("user_id:%u has subscribed, maybe it did not remove before", user_id);
    }

    dict_replace(dict_users, &key, sub_users);
    return 0;
}

int sub_user_remove(uint32_t user_id, nw_ses *ses)
{
    user_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.ses = ses;
    dict_entry *entry = dict_find(dict_users, &key);
    if (entry == NULL) {
        return 0;
    }

    dict_delete(dict_users, &key);
    return 0;
}

bool sub_user_has(uint32_t user_id, nw_ses *ses, uint32_t sub_user_id)
{
    user_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.ses = ses;
    dict_entry *entry = dict_find(dict_users, &key);
    if (entry == NULL) {
        return false;
    }

    dict_t *sub_users = entry->val;
    void *sub_key = (void *)(uintptr_t)sub_user_id;
    if (dict_find(sub_users, sub_key) != NULL) {
        return true;
    }

    return false;
}

bool sub_user_auth(uint32_t user_id, nw_ses *ses, json_t *params)
{
    user_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.ses = ses;
    dict_entry *entry = dict_find(dict_users, &key);
    if (entry == NULL) {
        return false;
    }

    dict_t *sub_users = entry->val;
    for (size_t i = 0; i < json_array_size(params); ++i) {
        uint32_t sub_user_id = json_integer_value(json_array_get(params, i));
        void *sub_key = (void *)(uintptr_t)sub_user_id;
        if (dict_find(sub_users, sub_key) == NULL) {
            return false;
        }
    }
    return true;
}

int sub_user_init(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));

    dt.hash_function = dict_user_hash_func;
    dt.key_dup = dict_user_key_dup;
    dt.key_compare = dict_user_key_compare;
    dt.key_destructor = dict_user_key_free;
    dt.val_destructor = dict_user_val_free;
    dict_users = dict_create(&dt, 16);
    if (dict_users == NULL) {
        return -__LINE__;
    }

    return 0;
}
