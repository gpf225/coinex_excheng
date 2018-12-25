/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/15, create
 */

# include "me_config.h"
# include "me_balance.h"
# include "ut_comm_dict.h"

dict_t *dict_balance;
static dict_t *dict_asset;

struct asset_type {
    int prec_save;
    int prec_show;
    mpd_t *min;
};

static uint32_t asset_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static void *asset_dict_key_dup(const void *key)
{
    return strdup(key);
}

static int asset_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void asset_dict_key_free(void *key)
{
    free(key);
}

static void asset_dict_val_free(void *val)
{
    struct asset_type *obj = val;
    mpd_del(obj->min);
    free(obj);
}

static uint32_t balance_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct balance_key));
}

static void *balance_dict_key_dup(const void *key)
{
    struct balance_key *obj = malloc(sizeof(struct balance_key));
    if (obj == NULL)
        return NULL;
    memcpy(obj, key, sizeof(struct balance_key));
    return obj;
}

static void *balance_dict_val_dup(const void *val)
{
    return mpd_qncopy(val);
}

static int balance_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct balance_key));
}

static void balance_dict_key_free(void *key)
{
    free(key);
}

static void balance_dict_val_free(void *val)
{
    mpd_del(val);
}

static int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = asset_dict_hash_function;
    type.key_compare    = asset_dict_key_compare;
    type.key_dup        = asset_dict_key_dup;
    type.key_destructor = asset_dict_key_free;
    type.val_destructor = asset_dict_val_free;

    dict_asset = dict_create(&type, 64);
    if (dict_asset == NULL)
        return -__LINE__;

    memset(&type, 0, sizeof(type));
    type.hash_function  = balance_dict_hash_function;
    type.key_compare    = balance_dict_key_compare;
    type.key_dup        = balance_dict_key_dup;
    type.key_destructor = balance_dict_key_free;
    type.val_dup        = balance_dict_val_dup;
    type.val_destructor = balance_dict_val_free;

    dict_balance = dict_create(&type, 64);
    if (dict_balance == NULL)
        return -__LINE__;

    return 0;
}

int init_balance()
{
    ERR_RET(init_dict());

    for (size_t i = 0; i < settings.asset_num; ++i) {
        struct asset_type *type = malloc(sizeof(struct asset_type));
        if (type == NULL)
            return -__LINE__;
        type->prec_save = settings.assets[i].prec_save;
        type->prec_show = settings.assets[i].prec_show;
        type->min = mpd_new(&mpd_ctx);
        mpd_set_i32(type->min, -type->prec_show, &mpd_ctx);
        mpd_pow(type->min, mpd_ten, type->min, &mpd_ctx);
        log_stderr("init asset: %s, prec save: %d, prec show: %d", settings.assets[i].name, type->prec_save, type->prec_show);
        if (dict_add(dict_asset, settings.assets[i].name, type) == NULL)
            return -__LINE__;
    }

    return 0;
}

int update_balance(void)
{
    for (size_t i = 0; i < settings.asset_num; ++i) {
        dict_entry *entry = dict_find(dict_asset, settings.assets[i].name);
        if (!entry) {
            struct asset_type type;
            type.prec_save = settings.assets[i].prec_save;
            type.prec_show = settings.assets[i].prec_show;
            if (dict_add(dict_asset, settings.assets[i].name, &type) == NULL)
                return -__LINE__;
        } else {
            struct asset_type *type = entry->val;
            if (type->prec_save < settings.assets[i].prec_save)
                type->prec_save = settings.assets[i].prec_save;
            type->prec_show = settings.assets[i].prec_show;
        }
    }

    return 0;
}

static struct asset_type *get_asset_type(const char *asset)
{
    dict_entry *entry = dict_find(dict_asset, asset);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

bool asset_exist(const char *asset)
{
    struct asset_type *at = get_asset_type(asset);
    return at ? true : false;
}

int asset_prec(const char *asset)
{
    struct asset_type *at = get_asset_type(asset);
    return at ? at->prec_save : -1;
}

int asset_prec_show(const char *asset)
{
    struct asset_type *at = get_asset_type(asset);
    return at ? at->prec_show: -1;
}

mpd_t *balance_get(uint32_t user_id, uint32_t type, const char *asset)
{
    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));

    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        return entry->val;
    }

    return NULL;
}

void balance_del(uint32_t user_id, uint32_t type, const char *asset)
{
    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));
    dict_delete(dict_balance, &key);
}

mpd_t *balance_set(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (type == BALANCE_TYPE_AVAILABLE && mpd_cmp(amount, at->min, &mpd_ctx) < 0) {
        return mpd_zero;
    }

    int ret = mpd_cmp(amount, mpd_zero, &mpd_ctx);
    if (ret < 0) {
        return NULL;
    } else if (ret == 0) {
        balance_del(user_id, type, asset);
        return mpd_zero;
    }

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));

    mpd_t *result;
    dict_entry *entry;
    entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        mpd_rescale(result, amount, -at->prec_save, &mpd_ctx);
        return result;
    }

    entry = dict_add(dict_balance, &key, amount);
    if (entry == NULL)
        return NULL;
    result = entry->val;
    mpd_rescale(result, amount, -at->prec_save, &mpd_ctx);

    return result;
}

mpd_t *balance_add(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    struct balance_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));

    mpd_t *result;
    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        mpd_add(result, result, amount, &mpd_ctx);
        mpd_rescale(result, result, -at->prec_save, &mpd_ctx);
        return result;
    }

    return balance_set(user_id, type, asset, amount);
}

mpd_t *balance_sub(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    mpd_t *result = balance_get(user_id, type, asset);
    if (result == NULL)
        return NULL;
    if (mpd_cmp(result, amount, &mpd_ctx) < 0)
        return NULL;

    mpd_sub(result, result, amount, &mpd_ctx);
    if (mpd_cmp(result, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, type, asset);
        return mpd_zero;
    }
    mpd_rescale(result, result, -at->prec_save, &mpd_ctx);

    return result;
}

mpd_t *balance_reset(uint32_t user_id, const char *asset)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    mpd_t *result = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
    if (result == NULL)
        return mpd_zero;

    if (mpd_cmp(result, at->min, &mpd_ctx) < 0) {
        balance_del(user_id, BALANCE_TYPE_AVAILABLE, asset);
        return mpd_zero;
    }

    return result;
}

mpd_t *balance_freeze(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    if (type != BALANCE_TYPE_FROZEN && type != BALANCE_TYPE_LOCK)
        return NULL;

    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
    if (available == NULL)
        return NULL;
    if (mpd_cmp(available, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, type, asset, amount) == 0)
        return NULL;
    mpd_sub(available, available, amount, &mpd_ctx);
    if (mpd_cmp(available, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, BALANCE_TYPE_AVAILABLE, asset);
        return mpd_zero;
    }
    mpd_rescale(available, available, -at->prec_save, &mpd_ctx);

    return available;
}

mpd_t *balance_unfreeze(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    if (type != BALANCE_TYPE_FROZEN && type != BALANCE_TYPE_LOCK)
        return NULL;

    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *frozen = balance_get(user_id, type, asset);
    if (frozen == NULL)
        return NULL;
    if (mpd_cmp(frozen, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, BALANCE_TYPE_AVAILABLE, asset, amount) == 0)
        return NULL;
    mpd_sub(frozen, frozen, amount, &mpd_ctx);
    if (mpd_cmp(frozen, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, type, asset);
        return mpd_zero;
    }
    mpd_rescale(frozen, frozen, -at->prec_save, &mpd_ctx);

    return frozen;
}

mpd_t *balance_available(uint32_t user_id, const char *asset)
{
    mpd_t *balance = mpd_new(&mpd_ctx);
    mpd_copy(balance, mpd_zero, &mpd_ctx);
    mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
    if (available) {
        mpd_add(balance, balance, available, &mpd_ctx);
    }

    return balance;
}

mpd_t *balance_frozen(uint32_t user_id, const char *asset)
{
    mpd_t *balance = mpd_new(&mpd_ctx);
    mpd_copy(balance, mpd_zero, &mpd_ctx);
    mpd_t *frozen = balance_get(user_id, BALANCE_TYPE_FROZEN, asset);
    if (frozen) {
        mpd_add(balance, balance, frozen, &mpd_ctx);
    }
    mpd_t *lock = balance_get(user_id, BALANCE_TYPE_LOCK, asset);
    if (lock) {
        mpd_add(balance, balance, lock, &mpd_ctx);
    }

    return balance;
}

mpd_t *balance_lock(uint32_t user_id, const char *asset)
{
    mpd_t *balance = mpd_new(&mpd_ctx);
    mpd_copy(balance, mpd_zero, &mpd_ctx);
    mpd_t *lock = balance_get(user_id, BALANCE_TYPE_LOCK, asset);
    if (lock) {
        mpd_add(balance, balance, lock, &mpd_ctx);
    }

    return balance;
}

mpd_t *balance_total(uint32_t user_id, const char *asset)
{
    mpd_t *balance = mpd_new(&mpd_ctx);
    mpd_copy(balance, mpd_zero, &mpd_ctx);
    mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
    if (available) {
        mpd_add(balance, balance, available, &mpd_ctx);
    }
    mpd_t *frozen = balance_get(user_id, BALANCE_TYPE_FROZEN, asset);
    if (frozen) {
        mpd_add(balance, balance, frozen, &mpd_ctx);
    }
    mpd_t *lock = balance_get(user_id, BALANCE_TYPE_LOCK, asset);
    if (lock) {
        mpd_add(balance, balance, lock, &mpd_ctx);
    }

    return balance;
}

