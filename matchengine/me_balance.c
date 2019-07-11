/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/15, create
 */

# include "me_asset.h"
# include "me_config.h"
# include "me_balance.h"

dict_t *dict_balance;

static uint32_t balance_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct balance_key));
}

static int balance_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct balance_key));
}

static void *balance_dict_key_dup(const void *key)
{
    struct balance_key *obj = malloc(sizeof(struct balance_key));
    memcpy(obj, key, sizeof(struct balance_key));
    return obj;
}

static void balance_dict_key_free(void *key)
{
    free(key);
}

static void *balance_dict_val_dup(const void *val)
{
    return mpd_qncopy(val);
}

static void balance_dict_val_free(void *val)
{
    mpd_del(val);
}

static void dict_free(void *val)
{
    dict_release(val);
}

int init_balance()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func;
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_destructor = dict_free;

    dict_balance = dict_create(&dt, 1024);
    if (dict_balance == NULL)
        return -__LINE__;

    return 0;
}

static dict_t *dict_account_init(uint32_t user_id, uint32_t account)
{
    dict_entry *entry = dict_find(dict_balance, (void *)(uintptr_t)user_id);
    if (entry == NULL) {
        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function  = uint32_dict_hash_func;
        dt.key_compare    = uint32_dict_key_compare;
        dt.val_destructor = dict_free;

        dict_t *dict = dict_create(&dt, 16);
        if (dict == NULL)
            return NULL;
        entry = dict_add(dict_balance, (void *)(uintptr_t)user_id, dict);
        if (entry == NULL)
            return NULL;
    }

    dict_t *dict_user = entry->val;
    entry = dict_find(dict_user, (void *)(uintptr_t)account);
    if (entry == NULL) {
        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function  = balance_dict_hash_function;
        dt.key_compare    = balance_dict_key_compare;
        dt.key_dup        = balance_dict_key_dup;
        dt.key_destructor = balance_dict_key_free;
        dt.val_dup        = balance_dict_val_dup;
        dt.val_destructor = balance_dict_val_free;

        dict_t *dict = dict_create(&dt, 16);
        if (dict == NULL)
            return NULL;
        entry = dict_add(dict_user, (void *)(uintptr_t)account, dict);
        if (entry == NULL) {
            dict_release(dict);
            return NULL;
        }
    }

    return entry->val;
}

static dict_t *dict_account_query(uint32_t user_id, uint32_t account)
{
    dict_entry *entry = dict_find(dict_balance, (void *)(uintptr_t)user_id);
    if (entry == NULL)
        return NULL;

    dict_t *dict_user = entry->val;
    entry = dict_find(dict_user, (void *)(uintptr_t)account);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

static void dict_account_reset(uint32_t user_id, uint32_t account)
{
    dict_entry *entry = dict_find(dict_balance, (void *)(uintptr_t)user_id);
    if (entry == NULL)
        return;
    dict_t *dict_user = entry->val;

    entry = dict_find(dict_user, (void *)(uintptr_t)account);
    if (entry == NULL)
        return;
    dict_t *dict_account = entry->val;

    if (dict_size(dict_account) == 0) {
        dict_delete(dict_user, (void *)(uintptr_t)account);
    }
    if (dict_size(dict_user) == 0) {
        dict_delete(dict_balance, (void *)(uintptr_t)user_id);
    }
}

mpd_t *balance_get(uint32_t user_id, uint32_t account, uint32_t type, const char *asset)
{
    dict_t *dict = dict_account_query(user_id, account);
    if (dict == NULL)
        return NULL;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.type = type;
    sstrncpy(key.asset, asset, sizeof(key.asset));

    dict_entry *entry = dict_find(dict, &key);
    if (entry) {
        return entry->val;
    }

    return NULL;
}

void balance_del(uint32_t user_id, uint32_t account, uint32_t type, const char *asset)
{
    dict_t *dict = dict_account_query(user_id, account);
    if (dict == NULL)
        return;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.type = type;
    sstrncpy(key.asset, asset, sizeof(key.asset));
    dict_delete(dict, &key);

    dict_account_reset(user_id, account);
}

mpd_t *balance_set(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(account, asset);
    if (at == NULL)
        return NULL;

    int ret = mpd_cmp(amount, mpd_zero, &mpd_ctx);
    if (ret < 0) {
        return NULL;
    } else if (ret == 0) {
        balance_del(user_id, account, type, asset);
        return mpd_zero;
    }

    dict_t *dict = dict_account_init(user_id, account);
    if (dict == NULL)
        return NULL;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.type = type;
    sstrncpy(key.asset, asset, sizeof(key.asset));

    mpd_t *result;
    dict_entry *entry;
    entry = dict_find(dict, &key);
    if (entry) {
        result = entry->val;
        mpd_rescale(result, amount, -at->prec_save, &mpd_ctx);
        return result;
    }

    entry = dict_add(dict, &key, amount);
    if (entry == NULL)
        return NULL;
    result = entry->val;
    mpd_rescale(result, result, -at->prec_save, &mpd_ctx);

    return result;
}

mpd_t *balance_add(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(account, asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    dict_t *dict = dict_account_init(user_id, account);
    if (dict == NULL)
        return NULL;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.type = type;
    sstrncpy(key.asset, asset, sizeof(key.asset));

    mpd_t *result;
    dict_entry *entry = dict_find(dict, &key);
    if (entry) {
        result = entry->val;
        mpd_add(result, result, amount, &mpd_ctx);
        mpd_rescale(result, result, -at->prec_save, &mpd_ctx);
        return result;
    }

    return balance_set(user_id, account, type, asset, amount);
}

mpd_t *balance_sub(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(account, asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    mpd_t *result = balance_get(user_id, account, type, asset);
    if (result == NULL)
        return NULL;
    if (mpd_cmp(result, amount, &mpd_ctx) < 0)
        return NULL;

    mpd_sub(result, result, amount, &mpd_ctx);
    if (mpd_cmp(result, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, account, type, asset);
        return mpd_zero;
    }
    mpd_rescale(result, result, -at->prec_save, &mpd_ctx);

    return result;
}

mpd_t *balance_reset(uint32_t user_id, uint32_t account, const char *asset)
{
    struct asset_type *at = get_asset_type(account, asset);
    if (at == NULL)
        return NULL;

    mpd_t *result = balance_get(user_id, account, BALANCE_TYPE_AVAILABLE, asset);
    if (result == NULL)
        return mpd_zero;

    mpd_t *frozen = balance_get(user_id, account, BALANCE_TYPE_FROZEN, asset);
    if (frozen != NULL && mpd_cmp(frozen, mpd_zero, &mpd_ctx) != 0)
        return result;

    if (mpd_cmp(result, at->min, &mpd_ctx) < 0) {
        balance_del(user_id, account, BALANCE_TYPE_AVAILABLE, asset);
        return mpd_zero;
    }

    return result;
}

mpd_t *balance_freeze(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount)
{
    if (type != BALANCE_TYPE_FROZEN && type != BALANCE_TYPE_LOCK)
        return NULL;

    struct asset_type *at = get_asset_type(account, asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *available = balance_get(user_id, account, BALANCE_TYPE_AVAILABLE, asset);
    if (available == NULL)
        return NULL;
    if (mpd_cmp(available, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, account, type, asset, amount) == NULL)
        return NULL;
    mpd_sub(available, available, amount, &mpd_ctx);
    if (mpd_cmp(available, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, account, BALANCE_TYPE_AVAILABLE, asset);
        return mpd_zero;
    }
    mpd_rescale(available, available, -at->prec_save, &mpd_ctx);

    return available;
}

mpd_t *balance_unfreeze(uint32_t user_id, uint32_t account, uint32_t type, const char *asset, mpd_t *amount)
{
    if (type != BALANCE_TYPE_FROZEN && type != BALANCE_TYPE_LOCK)
        return NULL;

    struct asset_type *at = get_asset_type(account, asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *frozen = balance_get(user_id, account, type, asset);
    if (frozen == NULL)
        return NULL;
    if (mpd_cmp(frozen, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, account, BALANCE_TYPE_AVAILABLE, asset, amount) == NULL)
        return NULL;
    mpd_sub(frozen, frozen, amount, &mpd_ctx);
    if (mpd_cmp(frozen, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, account, type, asset);
        return mpd_zero;
    }
    mpd_rescale(frozen, frozen, -at->prec_save, &mpd_ctx);

    return frozen;
}

mpd_t *balance_available(uint32_t user_id, uint32_t account, const char *asset)
{
    mpd_t *balance = mpd_qncopy(mpd_zero);
    dict_t *dict = dict_account_query(user_id, account);
    if (dict == NULL)
        return balance;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    sstrncpy(key.asset, asset, sizeof(key.asset));

    key.type = BALANCE_TYPE_AVAILABLE;
    dict_entry *entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    return balance;
}

static mpd_t *balance_frozen_lock(uint32_t user_id, uint32_t account, const char *asset)
{
    mpd_t *balance = mpd_qncopy(mpd_zero);
    dict_t *dict = dict_account_query(user_id, account);
    if (dict == NULL)
        return balance;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    sstrncpy(key.asset, asset, sizeof(key.asset));

    key.type = BALANCE_TYPE_FROZEN;
    dict_entry *entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    key.type = BALANCE_TYPE_LOCK;
    entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    return balance;
}

static mpd_t *balance_frozen(uint32_t user_id, uint32_t account, const char *asset)
{
    mpd_t *balance = mpd_qncopy(mpd_zero);
    dict_t *dict = dict_account_query(user_id, account);
    if (dict == NULL)
        return balance;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    sstrncpy(key.asset, asset, sizeof(key.asset));

    key.type = BALANCE_TYPE_FROZEN;
    dict_entry *entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    return balance;
}

static mpd_t *balance_lock(uint32_t user_id, uint32_t account, const char *asset)
{
    mpd_t *balance = mpd_qncopy(mpd_zero);
    dict_t *dict = dict_account_query(user_id, account);
    if (dict == NULL)
        return balance;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    sstrncpy(key.asset, asset, sizeof(key.asset));

    key.type = BALANCE_TYPE_LOCK;
    dict_entry *entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    return balance;
}

mpd_t *balance_total(uint32_t user_id, uint32_t account, const char *asset)
{
    mpd_t *balance = mpd_qncopy(mpd_zero);
    dict_t *dict = dict_account_query(user_id, account);
    if (dict == NULL)
        return balance;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    sstrncpy(key.asset, asset, sizeof(key.asset));

    key.type = BALANCE_TYPE_AVAILABLE;
    dict_entry *entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    key.type = BALANCE_TYPE_FROZEN;
    entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    key.type = BALANCE_TYPE_LOCK;
    entry = dict_find(dict, &key);
    if (entry) {
        mpd_add(balance, balance, entry->val, &mpd_ctx);
    }

    return balance;
}

json_t *balance_query_list(uint32_t user_id, uint32_t account, json_t *params)
{
    json_t *result = json_object();
    size_t array_size = json_array_size(params);
    if (array_size == 2) {
        dict_entry *entry = dict_find(dict_asset, (void *)(uintptr_t)account);
        if (entry == NULL)
            return result;

        dict_iterator *iter = dict_get_iterator(entry->val);
        while ((entry = dict_next(iter)) != NULL) {
            const char *asset = entry->key;
            const struct asset_type *type = entry->val;

            json_t *unit = json_object();
            mpd_t *available = balance_available(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(available, available, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "available", available);
            mpd_del(available);

            mpd_t *frozen = balance_frozen_lock(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(frozen, frozen, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "frozen", frozen);
            mpd_del(frozen);
            json_object_set_new(result, asset, unit);
        }
        dict_release_iterator(iter);
    } else {
        for (size_t i = 2; i < array_size; ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (asset == NULL)
                continue;
            struct asset_type *type = get_asset_type(account, asset);
            if (type == NULL)
                continue;

            json_t *unit = json_object();
            mpd_t *available = balance_available(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(available, available, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "available", available);
            mpd_del(available);

            mpd_t *frozen = balance_frozen_lock(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(frozen, frozen, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "frozen", frozen);
            mpd_del(frozen);
            json_object_set_new(result, asset, unit);
        }
    }

    return result;
}

json_t *balance_query_users(uint32_t account, json_t *users)
{
    json_t *result = json_object();
    size_t array_size = json_array_size(users);
    dict_entry *entry = dict_find(dict_asset, (void *)(uintptr_t)account);
    if (entry == NULL)
        return result;

    dict_iterator *iter = dict_get_iterator(entry->val);
    for (size_t i = 0; i < array_size; ++i) {
        if (!json_is_integer(json_array_get(users, i)))
            continue;

        uint32_t user_id = json_integer_value(json_array_get(users, i));
        json_t *item = json_object();
        while ((entry = dict_next(iter)) != NULL) {
            const char *asset = entry->key;
            const struct asset_type *type = entry->val;

            json_t *unit = json_object();
            mpd_t *available = balance_available(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(available, available, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "available", available);
            mpd_del(available);

            mpd_t *frozen = balance_frozen(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(frozen, frozen, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "frozen", frozen);
            mpd_del(frozen);

            json_object_set_new(item, asset, unit);
        }
        dict_reset_iterator(iter);

        char user_id_str[20];
        snprintf(user_id_str, sizeof(user_id_str), "%u", user_id);
        json_object_set_new(result, user_id_str, item);
    }
    dict_release_iterator(iter);

    return result;
}

json_t *balance_query_lock_list(uint32_t user_id, uint32_t account, json_t *params)
{
    json_t *result = json_object();
    size_t array_size = json_array_size(params);
    if (array_size == 2) {
        dict_entry *entry = dict_find(dict_asset, (void *)(uintptr_t)account);
        if (entry == NULL)
            return result;

        dict_iterator *iter = dict_get_iterator(entry->val);
        while ((entry = dict_next(iter)) != NULL) {
            const char *asset = entry->key;
            const struct asset_type *type = entry->val;

            mpd_t *lock = balance_lock(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(lock, lock, -type->prec_show, &mpd_ctx);
            }
            if (mpd_cmp(lock, mpd_zero, &mpd_ctx) > 0) {
                json_object_set_new_mpd(result, asset, lock);
            }
            mpd_del(lock);
        }
        dict_release_iterator(iter);
    } else {
        for (size_t i = 2; i < array_size; ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (asset == NULL)
                continue;
            struct asset_type *type = get_asset_type(account, asset);
            if (type == NULL)
                continue;

            mpd_t *lock = balance_lock(user_id, account, asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(lock, lock, -type->prec_show, &mpd_ctx);
            }
            if (mpd_cmp(lock, mpd_zero, &mpd_ctx) > 0) {
                json_object_set_new_mpd(result, asset, lock);
            }
            mpd_del(lock);
        }
    }

    return result;
}

json_t *balance_query_all(uint32_t user_id)
{
    json_t *result = json_object();
    dict_entry *entry = dict_find(dict_balance, (void *)(uintptr_t)user_id);
    if (entry == NULL)
        return result;

    dict_t *dict_user = entry->val;
    dict_iterator *iter = dict_get_iterator(dict_user);
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t account = (uintptr_t)entry->key;
        dict_t *dict_account = entry->val;

        json_t *account_info = json_object();
        dict_entry *entry_account;
        dict_iterator *iter_account = dict_get_iterator(dict_account);
        while ((entry_account = dict_next(iter_account)) != NULL) {
            const struct balance_key *key = entry_account->key;
            if (json_object_get(account_info, key->asset) != NULL)
                continue;
            struct asset_type *type = get_asset_type(account, key->asset);
            if (type == NULL)
                continue;

            json_t *unit = json_object();
            mpd_t *available = balance_available(user_id, account, key->asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(available, available, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "available", available);
            mpd_del(available);

            mpd_t *frozen = balance_frozen_lock(user_id, account, key->asset);
            if (type->prec_save != type->prec_show) {
                mpd_rescale(frozen, frozen, -type->prec_show, &mpd_ctx);
            }
            json_object_set_new_mpd(unit, "frozen", frozen);
            mpd_del(frozen);
            json_object_set_new(account_info, key->asset, unit);
        }
        dict_release_iterator(iter_account);

        char account_str[20];
        snprintf(account_str, sizeof(account_str), "%u", account);
        json_object_set_new(result, account_str, account_info);
    }
    dict_release_iterator(iter);

    return result;
}

json_t *balance_get_summary(const char *asset)
{
    int total_users = 0, available_users = 0, lock_users = 0, frozen_users = 0;
    mpd_t *total     = mpd_qncopy(mpd_zero);
    mpd_t *available = mpd_qncopy(mpd_zero);
    mpd_t *frozen    = mpd_qncopy(mpd_zero);
    mpd_t *lock      = mpd_qncopy(mpd_zero);

    dict_t *distinct_dict = uint32_set_create();
    dict_iterator *iter = dict_get_iterator(dict_balance);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        uint32_t user_id = (uintptr_t)entry->key;

        dict_t *dict_user = entry->val;
        dict_entry *entry_user;
        dict_iterator *iter_user = dict_get_iterator(dict_user);
        while ((entry_user = dict_next(iter_user)) != NULL) {
            uint32_t account = (uintptr_t)entry_user->key;
            if (!asset_exist(account, asset)) {
                continue;
            }

            int prec_save = asset_prec_save(account, asset);
            int prec_show = asset_prec_show(account, asset);

            dict_t *dict_account = entry_user->val;
            dict_entry *entry_account;
            dict_iterator *iter_account = dict_get_iterator(dict_account);
            while ((entry_account = dict_next(iter_account)) != NULL) {
                const struct balance_key *key = entry_account->key;
                if (strcmp(key->asset, asset) != 0) {
                    continue;
                }

                mpd_t *balance = mpd_qncopy(entry_account->val);
                if (prec_save != prec_show) {
                    mpd_rescale(balance, balance, -prec_show, &mpd_ctx);
                }

                mpd_add(total, total, balance, &mpd_ctx);
                if (key->type == BALANCE_TYPE_AVAILABLE) {
                    available_users++;
                    mpd_add(available, available, balance, &mpd_ctx);
                } else if (key->type == BALANCE_TYPE_FROZEN) {
                    frozen_users++;
                    mpd_add(frozen, frozen, balance, &mpd_ctx);
                } else {
                    lock_users++;
                    mpd_add(lock, lock, balance, &mpd_ctx);
                }
                mpd_del(balance);

                uint32_set_add(distinct_dict, user_id);
            }
            dict_release_iterator(iter_account);
        }
        dict_release_iterator(iter_user);
    }
    dict_release_iterator(iter);
    total_users = uint32_set_num(distinct_dict);
    uint32_set_release(distinct_dict);

    json_t *result = json_object();
    json_object_set_new(result, "total_users", json_integer(total_users));
    json_object_set_new(result, "available_users", json_integer(available_users));
    json_object_set_new(result, "lock_users", json_integer(lock_users));
    json_object_set_new(result, "frozen_users", json_integer(frozen_users));
    json_object_set_new_mpd(result, "total", total);
    json_object_set_new_mpd(result, "available", available);
    json_object_set_new_mpd(result, "frozen", frozen);
    json_object_set_new_mpd(result, "lock", lock);

    mpd_del(total);
    mpd_del(available);
    mpd_del(frozen);
    mpd_del(lock);
    return result;
}

