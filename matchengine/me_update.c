/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/18, create
 */

# include "me_config.h"
# include "me_update.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"
# include "me_asset.h"

dict_t *dict_update;
static nw_timer timer;

static uint32_t update_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct update_key));
}

static int update_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct update_key));
}

static void *update_dict_key_dup(const void *key)
{
    struct update_key *obj = malloc(sizeof(struct update_key));
    memcpy(obj, key, sizeof(struct update_key));
    return obj;
}

static void update_dict_key_free(void *key)
{
    free(key);
}

static void *update_dict_val_dup(const void *val)
{
    struct update_val*obj = malloc(sizeof(struct update_val));
    memcpy(obj, val, sizeof(struct update_val));
    return obj;
}

static void update_dict_val_free(void *val)
{
    free(val);
}

static void on_timer(nw_timer *t, void *privdata)
{
    double now = current_timestamp();
    dict_iterator *iter = dict_get_iterator(dict_update);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct update_val *val = entry->val;
        if (val->create_time < (now - 86400 * 7)) {
            dict_delete(dict_update, entry->key);
        }
    }
    dict_release_iterator(iter);
}

int init_update(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = update_dict_hash_function;
    type.key_compare    = update_dict_key_compare;
    type.key_dup        = update_dict_key_dup;
    type.key_destructor = update_dict_key_free;
    type.val_dup        = update_dict_val_dup;
    type.val_destructor = update_dict_val_free;

    dict_update = dict_create(&type, 64);
    if (dict_update == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 60, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

static void balance_update_message(uint32_t user_id, uint32_t account, const char *asset)
{
    mpd_t *result = balance_get(user_id, account, BALANCE_TYPE_AVAILABLE, asset);
    if (result == NULL)
        result = mpd_zero;

    struct asset_type *type = get_asset_type(account, asset);
    mpd_t *available = mpd_qncopy(result);
    if (type->prec_save != type->prec_show) {
        mpd_rescale(available, available, -type->prec_show, &mpd_ctx);
    }

    mpd_t *frozen = balance_frozen_lock(user_id, account, asset);
    if (type->prec_save != type->prec_show) {
        mpd_rescale(frozen, frozen, -type->prec_show, &mpd_ctx);
    }

    push_balance_message(current_timestamp(), user_id, account, asset, available, frozen);
    mpd_del(available);
    mpd_del(frozen);
}

int update_user_balance(bool real, uint32_t user_id, uint32_t account, const char *asset, const char *business, uint64_t business_id, mpd_t *change, json_t *detail)
{
    struct update_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.account = account;
    sstrncpy(key.asset, asset, sizeof(key.asset));
    sstrncpy(key.business, business, sizeof(key.business));
    key.business_id = business_id;

    dict_entry *entry = dict_find(dict_update, &key);
    if (entry) {
        return -1;
    }

    mpd_t *result;
    mpd_t *abs_change = mpd_new(&mpd_ctx);
    mpd_abs(abs_change, change, &mpd_ctx);
    if (mpd_cmp(change, mpd_zero, &mpd_ctx) >= 0) {
        result = balance_add(user_id, account, BALANCE_TYPE_AVAILABLE, asset, abs_change);
        if (result == NULL) {
            mpd_del(abs_change);
            return -2;
        }
        result = balance_reset(user_id, account, asset);
    } else {
        result = balance_sub(user_id, account, BALANCE_TYPE_AVAILABLE, asset, abs_change);
        if (result == NULL) {
            mpd_del(abs_change);
            return -2;
        }
        result = balance_reset(user_id, account, asset);
    }
    mpd_del(abs_change);
    if (result == NULL)
        return -2;

    struct update_val val = { .create_time = current_timestamp() };
    dict_add(dict_update, &key, &val);

    if (real) {
        double now = current_timestamp();
        json_object_set_new(detail, "id", json_integer(business_id));
        char *detail_str = json_dumps(detail, 0);
        append_user_balance_history(now, user_id, account, asset, business, change, detail_str);
        balance_update_message(user_id, account, asset);
        free(detail_str);
    }

    return 0;
}

int update_user_lock(bool real, uint32_t user_id, uint32_t account, const char *asset, const char *business, uint64_t business_id, mpd_t *amount)
{
    struct update_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.account = account;
    snprintf(key.asset, sizeof(key.asset), "%s_LOCK", asset);
    sstrncpy(key.business, business, sizeof(key.business));
    key.business_id = business_id;

    dict_entry *entry = dict_find(dict_update, &key);
    if (entry) {
        return -1;
    }

    if (balance_freeze(user_id, account, BALANCE_TYPE_LOCK, asset, amount) == NULL) {
        return -2;
    }

    struct update_val val = { .create_time = current_timestamp() };
    dict_add(dict_update, &key, &val);

    if (real) {
        balance_update_message(user_id, account, asset);
    }

    return 0;
}

int update_user_unlock(bool real, uint32_t user_id, uint32_t account, const char *asset, const char *business, uint64_t business_id, mpd_t *amount)
{
    struct update_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.account = account;
    snprintf(key.asset, sizeof(key.asset), "%s_UNLOCK", asset);
    sstrncpy(key.business, business, sizeof(key.business));
    key.business_id = business_id;

    dict_entry *entry = dict_find(dict_update, &key);
    if (entry) {
        return -1;
    }

    if (balance_unfreeze(user_id, account, BALANCE_TYPE_LOCK, asset, amount) == NULL) {
        return -2;
    }

    struct update_val val = { .create_time = current_timestamp() };
    dict_add(dict_update, &key, &val);

    if (real) {
        balance_update_message(user_id, account, asset);
    }

    return 0;
}

int update_add(uint32_t user_id, uint32_t account, const char *asset, const char *business, uint64_t business_id, double create_time)
{
    struct update_key key;
    memset(&key, 0, sizeof(key));
    key.user_id = user_id;
    key.account = account;
    sstrncpy(key.asset, asset, sizeof(key.asset));
    sstrncpy(key.business, business, sizeof(key.business));
    key.business_id = business_id;

    struct update_val val = { .create_time = create_time };
    dict_add(dict_update, &key, &val);

    return 0;
}

