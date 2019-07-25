/*
 * Description: 
 *     History: ouxiangyang, 2019/07/19, create
 */

# include "aw_config.h"
# include "aw_notice.h"

static dict_t *dict_sub_notice;

struct sub_notice_val {
    dict_t *sessions;
};

static void *dict_notice_val_dup(const void *val)
{
    struct sub_notice_val *obj = malloc(sizeof(struct sub_notice_val));
    memcpy(obj, val, sizeof(struct sub_notice_val));
    return obj;
}

static void dict_notice_val_free(void *val)
{
    struct sub_notice_val *obj = val;
    if (obj->sessions)
        dict_release(obj->sessions);
    free(obj);
}

int init_notice(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = uint32_dict_hash_func; 
    dt.key_compare    = uint32_dict_key_compare;
    dt.val_dup        = dict_notice_val_dup;
    dt.val_destructor = dict_notice_val_free;
    dict_sub_notice = dict_create(&dt, 64);
    if (dict_sub_notice == NULL)
        return -__LINE__;

    return 0;
}

int notice_subscribe(uint32_t user_id, nw_ses *ses)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub_notice, key);
    if (entry == NULL) {
        struct sub_notice_val val;
        memset(&val, 0, sizeof(val));

        dict_types dt;
        memset(&dt, 0, sizeof(dt));
        dt.hash_function = ptr_dict_hash_func;
        dt.key_compare = ptr_dict_key_compare;
        val.sessions = dict_create(&dt, 2);
        if (val.sessions == NULL)
            return -__LINE__;

        entry = dict_add(dict_sub_notice, key, &val);
        if (entry == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }

    struct sub_notice_val *obj = entry->val;
    dict_add(obj->sessions, ses, NULL);

    return 0;
}

int notice_unsubscribe(uint32_t user_id, nw_ses *ses)
{
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub_notice, key);
    if (entry) {
        struct sub_notice_val *obj = entry->val;
        dict_delete(obj->sessions, ses);

        if (dict_size(obj->sessions) == 0) {
            dict_delete(dict_sub_notice, key);
        }
    }

    return 0;
}

int notice_message(json_t *msg)
{
    uint32_t user_id = json_integer_value(json_object_get(msg, "user_id"));
    void *key = (void *)(uintptr_t)user_id;
    dict_entry *entry = dict_find(dict_sub_notice, key);
    if (entry == NULL)
        return 0;

    size_t count = 0;
    struct sub_notice_val *obj = entry->val;
    dict_iterator *iter = dict_get_iterator(obj->sessions);
    while ((entry = dict_next(iter)) != NULL) {
        ws_send_notify(entry->key, "notice.update", msg);
        count += 1;
    }
    dict_release_iterator(iter);
    profile_inc("notice.update", count);
    return 0;
}

size_t notice_subscribe_number(void)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_sub_notice);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        const struct sub_notice_val *obj = entry->val;
        count += dict_size(obj->sessions);
    }
    dict_release_iterator(iter);

    return count;
}

