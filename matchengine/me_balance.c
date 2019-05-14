/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/15, create
 */

# include "me_asset.h"
# include "me_config.h"
# include "me_balance.h"
# include "ut_comm_dict.h"

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
        if (entry == NULL)
            return NULL;
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

    if (dict_account->used == 0) {
        dict_delete(dict_user, (void *)(uintptr_t)account);
    }
    if (dict_user->used == 0) {
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
    mpd_rescale(result, amount, -at->prec_save, &mpd_ctx);

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

    if (balance_add(user_id, account, type, asset, amount) == 0)
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

    if (balance_add(user_id, account, BALANCE_TYPE_AVAILABLE, asset, amount) == 0)
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

mpd_t *balance_frozen(uint32_t user_id, uint32_t account, const char *asset)
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

mpd_t *balance_lock(uint32_t user_id, uint32_t account, const char *asset)
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

