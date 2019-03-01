/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/27, create
 */

# include "ca_depth_wait_queue.h"

static dict_t *dict_wait_queue_req = NULL;  // accessrest深度获取请求等待
static dict_t *dict_wait_queue_sub = NULL;  // 深度订阅等待

struct depth_key {
    char market[MARKET_NAME_MAX_LEN];
    char interval[INTERVAL_MAX_LEN];
    uint limit;
};

struct depth_val {
    dict_t *sessions; 
};

static uint32_t dict_depth_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct depth_key));
}

static int dict_depth_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct depth_key));
}

static void *dict_depth_key_dup(const void *key)
{
    struct depth_key *obj = malloc(sizeof(struct depth_key));
    memcpy(obj, key, sizeof(struct depth_key));
    return obj;
}

static void dict_depth_key_free(void *key)
{
    free(key);
}

static void *dict_depth_val_dup(const void *val)
{
    struct depth_val *obj = malloc(sizeof(struct depth_val));
    memcpy(obj, val, sizeof(struct depth_val));
    return obj;
}

static void dict_depth_val_free(void *val)
{
    struct depth_val *obj = val;
    dict_release(obj->sessions);
    free(obj);
}

static int init_dict_wait_queue_req(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;
    dt.val_dup = dict_depth_val_dup;
    dt.val_destructor = dict_depth_val_free;

    dict_wait_queue_req = dict_create(&dt, 64);
    if (dict_wait_queue_req == NULL) {
        return -__LINE__;
    }

    return 0;
}

static int init_dict_wait_queue_sub(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_depth_key_hash_func;
    dt.key_compare = dict_depth_key_compare;
    dt.key_dup = dict_depth_key_dup;
    dt.key_destructor = dict_depth_key_free;

    dict_wait_queue_sub = dict_create(&dt, 64);
    if (dict_wait_queue_sub == NULL) {
        return -__LINE__;
    }

    return 0;
}

static uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

static int dict_ses_hash_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

static void *dict_depth_wait_pkg_dup(const void *val)
{
    const struct rpc_pkg *pkg = val;
    struct rpc_pkg *pkg1 = malloc(sizeof(struct rpc_pkg));
    memcpy(pkg1, pkg, RPC_PKG_HEAD_SIZE);
    return pkg1;
}

static void dict_depth_wait_pkg_free(void *val)
{
    free(val);
}

static dict_t* dict_create_depth_req_session(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_ses_hash_func;
    dt.key_compare = dict_ses_hash_compare;
    dt.val_dup = dict_depth_wait_pkg_dup;
    dt.val_destructor = dict_depth_wait_pkg_free;
    return dict_create(&dt, 16);
}

static void depth_set_key(struct depth_key *key, const char *market, const char *interval, uint32_t limit)
{
    memset(key, 0, sizeof(struct depth_key));
    strncpy(key->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key->interval, interval, INTERVAL_MAX_LEN - 1);
    key->limit = limit;
}

int depth_wait_queue_req_add(const char *market, const char *interval, uint32_t limit, nw_ses *ses, rpc_pkg *pkg)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);

    dict_entry *entry = dict_find(dict_wait_queue_req, &key);
    if (entry == NULL) {
        struct depth_val val;
        memset(&val, 0, sizeof(struct depth_val));
        val.sessions = dict_create_depth_req_session();
        if (val.sessions == NULL) {
            return -__LINE__;
        }

        if (dict_add(dict_wait_queue_req, &key, &val) == NULL) {
            dict_release(val.sessions);
            return -__LINE__;
        }
    }
    
    struct depth_val *val = entry->val;
    if (dict_add(val->sessions, ses, pkg) == NULL) {
        return -__LINE__;
    }

    return 0;
}

int depth_wait_queue_req_remove(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);
    dict_delete(dict_wait_queue_req, &key);
    return 0;
}

dict_t* depth_wait_queue_req_get(const char *market, const char *interval, uint32_t limit)
{
    struct depth_key key;
    depth_set_key(&key, market, interval, limit);

    dict_entry *entry = dict_find(dict_wait_queue_req, &key);
    if (entry == NULL) {
        return NULL;
    }
    return entry->val;
}

int init_wait_queue(void)
{
    int ret = init_dict_wait_queue_req();
    if (ret != 0) {
        return ret;
    }
    ret = init_dict_wait_queue_sub();
    if (ret != 0) {
        return ret;
    }
    return 0;
}