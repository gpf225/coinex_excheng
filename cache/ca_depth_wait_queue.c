/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 */

# include "ca_depth_wait_queue.h"
# include "ca_common.h"

static dict_t *depth_wait_queue = NULL;

static void *dict_depth_wait_val_dup(const void *val)
{
    struct depth_wait_val *obj = malloc(sizeof(struct depth_wait_val));
    memcpy(obj, val, sizeof(struct depth_wait_val));
    return obj;
}

static void depth_depth_wait_val_free(void *val)
{
    struct depth_wait_val *obj = val;
    dict_release(obj->dict_wait_session);
    free(obj);
}

static void depth_depth_wait_list_free(void *val)
{
    list_release(val);
}

static dict_t *dict_create_wait_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_hash_compare;
    dt.val_destructor = depth_depth_wait_list_free;

    return dict_create(&dt, 32);
}

static void *list_depth_wait_item_dup(void *val)
{
    struct depth_wait_item *obj = malloc(sizeof(struct depth_wait_item));
    memcpy(obj, val, sizeof(struct depth_wait_item));
    return obj;
}

static void list_depth_wait_item_free(void *val)
{
    free(val);
}

static int list_depth_wait_item_compare(const void *value1, const void *value2)
{
    const struct depth_wait_item *item1 = value1;
    const struct depth_wait_item *item2 = value2;
    if (item1->sequence > item2->sequence) {
        return -1;
    } else if (item1->sequence < item2->sequence) {
        return 1;
    }
    return 0;
}

static list_t *create_depth_item_list(void)
{
    list_type type;
    memset(&type, 0, sizeof(struct list_type));
    type.dup = list_depth_wait_item_dup;
    type.free = list_depth_wait_item_free;
    type.compare = list_depth_wait_item_compare;
    return list_create(&type);
}

int depth_wait_queue_add(const char *market, const char *interval, uint32_t limit, nw_ses *ses, uint32_t sequence, rpc_pkg *pkg, int wait_type)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);    

    dict_entry *entry = dict_find(depth_wait_queue, &key);
    if (entry == NULL) {
        struct depth_wait_val val;
        memset(&val, 0, sizeof(struct depth_wait_val));
        val.dict_wait_session = dict_create_wait_session();
        if (val.dict_wait_session == NULL) {
            return -__LINE__;
        }
        entry = dict_add(depth_wait_queue, &key, &val);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    struct depth_wait_val *wait_val = entry->val;
    entry = dict_find(wait_val->dict_wait_session, ses);
    if (entry == NULL) {
        list_t *list = create_depth_item_list();
        if (list == NULL) {
            return -__LINE__;
        }
        entry = dict_add(wait_val->dict_wait_session, ses, list);
        if (entry == NULL) {
            return -__LINE__;
        }
    }

    list_t *list = entry->val;
    struct depth_wait_item item;
    memset(&item, 0, sizeof(struct depth_wait_item));
    item.limit = limit;
    item.sequence = sequence;
    item.wait_type = wait_type;
    memcpy(&item.pkg, pkg, RPC_PKG_HEAD_SIZE);
    if (list_find(list, &item) != NULL) {
        return 0;
    }
    list_add_node_tail(list, &item);
    return 1;
}

int depth_wait_queue_remove(const char *market, const char *interval, uint32_t limit, nw_ses *ses, uint32_t sequence)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);    

    dict_entry *entry = dict_find(depth_wait_queue, &key);
    if (entry == NULL) {
       return 0;
    }

    struct depth_wait_val *val = entry->val;
    entry = dict_find(val->dict_wait_session, ses);
    if (entry == NULL) {
        return 0;
    }

    list_t *list = entry->val;
    struct depth_wait_item item;
    memset(&item, 0, sizeof(struct depth_wait_item));
    item.limit = limit;
    item.sequence = sequence;
    list_node *node = list_find(list, &item);
    if (node == NULL) {
        return 0;
    }
    list_del(list, node);
    if (list_len(list) == 0) {
        dict_delete(val->dict_wait_session, ses);
    }
    return 1;
}

int depth_wait_queue_remove_all(nw_ses *ses)
{
    int count = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(depth_wait_queue);
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_wait_val *val = entry->val;
        entry = dict_find(val->dict_wait_session, ses);
        if (entry != NULL) {
            list_t *list = entry->val;
            count += list_len(list);
            dict_delete(val->dict_wait_session, ses);
        } 
    }
    dict_release_iterator(iter);
    return count;
}

struct depth_wait_val *depth_wait_queue_get(const char *market, const char *interval)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);    

    dict_entry *entry = dict_find(depth_wait_queue, &key);
    if (entry == NULL) {
        return NULL;
    }
    
    return entry->val;
}

int init_depth_wait_queue(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_wait_val_dup;
    dt.val_destructor = depth_depth_wait_val_free;

    depth_wait_queue = dict_create(&dt, 256);
    if (depth_wait_queue == NULL) {
        return -__LINE__;
    }

    return 0;
}

void fini_depth_wait_queue(void)
{
    dict_release(depth_wait_queue);
}

