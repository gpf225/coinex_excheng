/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */
# include "me_config.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_reader.h"
# include "ut_queue.h"
# include "me_reply.h"

static rpc_svr *svr;
static dict_t *dict_cache;
static nw_timer cache_timer;
static int reader_id;
static queue_t queue_reader;

struct cache_val {
    double      time;
    json_t      *result;
};

static bool check_cache(nw_ses *ses, rpc_pkg *pkg, sds *cache_key)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "%u", pkg->command);
    key = sdscatlen(key, pkg->body, pkg->body_size);
    dict_entry *entry = dict_find(dict_cache, key);
    if (entry == NULL) {
        *cache_key = key;
        return false;
    }

    struct cache_val *cache = entry->val;
    double now = current_timestamp();
    if ((now - cache->time) > settings.cache_timeout) {
        dict_delete(dict_cache, key);
        *cache_key = key;
        return false;
    }

    reply_result(ses, pkg, cache->result);
    sdsfree(key);
    return true;
}

static int add_cache(sds cache_key, json_t *result)
{
    struct cache_val cache;
    cache.time = current_timestamp();
    cache.result = result;
    json_incref(result);
    dict_replace(dict_cache, cache_key, &cache);
    return 0;
}

static uint32_t cache_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sdslen((sds)key));
}

static int cache_dict_key_compare(const void *key1, const void *key2)
{
    return sdscmp((sds)key1, (sds)key2);
}

static void *cache_dict_key_dup(const void *key)
{
    return sdsdup((const sds)key);
}

static void cache_dict_key_free(void *key)
{
    sdsfree(key);
}

static void *cache_dict_val_dup(const void *val)
{
    struct cache_val *obj = malloc(sizeof(struct cache_val));
    memcpy(obj, val, sizeof(struct cache_val));
    return obj;
}

static void cache_dict_val_free(void *val)
{
    struct cache_val *obj = val;
    json_decref(obj->result);
    free(val);
}

static void on_cache_timer(nw_timer *timer, void *privdata)
{
    dict_clear(dict_cache);
}

static int init_cache()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = cache_dict_hash_function;
    dt.key_compare    = cache_dict_key_compare;
    dt.key_dup        = cache_dict_key_dup;
    dt.key_destructor = cache_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;

    dict_cache = dict_create(&dt, 64);
    if (dict_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_timer, NULL);
    nw_timer_start(&cache_timer);
    return 0;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{

}

static int init_server()
{
    if (settings.svr.bind_count != 1)
        return -__LINE__;
    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET)
        return -__LINE__;
    bind_arr->addr.in.sin_port = htons(ntohs(bind_arr->addr.in.sin_port) + reader_id + 2);

    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    return 0;
}

static void on_message(void *data, uint32_t size)
{
    char *data_s = (char *)malloc(size + 1);
    memset(data_s, 0, size + 1);
    memcpy(data_s, data, size);
    log_info("reader: %d, read from queue: %s, size: %d", reader_id, data_s, size);
    free(data_s);
}

static int init_queue()
{
    queue_type type;
    memset(&type, 0, sizeof(type));
    type.on_message  = on_message;

    sds queue_name = sdsempty();
    queue_name = sdscatprintf(queue_name, "%s_%d", QUEUE_NAME, reader_id);

    sds queue_pipe_path = sdsempty();
    queue_pipe_path = sdscatprintf(queue_pipe_path, "%s_%d", QUEUE_PIPE_PATH, reader_id);

    key_t queue_shm_key = QUEUE_SHMKEY_START + reader_id;

    int ret = queue_reader_init(&queue_reader, &type, queue_name, queue_pipe_path, queue_shm_key, QUEUE_MEM_SIZE);

    sdsfree(queue_name);
    sdsfree(queue_pipe_path);

    return ret;
}

int init_reader(int id)
{
    reader_id = id;

    int ret;
    ret = init_queue();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_cache();
    if (ret < 0) {
        return -__LINE__;
    }
    
    ret = init_server();
    if (ret < 0) {
        return -__LINE__;
    }
    
    return 0;
}