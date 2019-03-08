/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/18, create
 */

# include "ar_depth.h"

static dict_t *dict_depth = NULL;
static rpc_clt *longpoll = NULL;

struct depth_val{
    double time;
    json_t *data;
};

static uint32_t dict_market_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_market_compare(const void *value1, const void *value2)
{
    return strcmp(value1, value2);
}

static void *dict_market_dup(const void *value)
{
    return strdup(value);
}

static void dict_market_free(void *value)
{
    free(value);
}

static void *dict_depth_val_dup(const void *key)
{
    struct depth_val *obj = malloc(sizeof(struct depth_val));
    memcpy(obj, key, sizeof(struct depth_val));
    return obj;
}

static void dict_depth_val_free(void *val)
{
    struct depth_val *obj = val;
    if (obj->data != NULL) {
        json_decref(obj->data);
    }
    free(obj);
}

static void longpoll_subscribe_depth(void)
{
    if (!rpc_clt_connected(longpoll)) {
        return ;
    }
    
    static uint32_t sequence = 0;
    json_t *params = json_array();
    json_array_append_new(params, json_string("0"));
    json_array_append_new(params, json_integer(1));
    
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_LP_DEPTH_SUBSCRIBE_ALL;
    pkg.sequence  = ++sequence;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(longpoll, &pkg);
    log_trace("send request to %s, cmd: %u, sequence: %u, params: %s", nw_sock_human_addr(rpc_clt_peer_addr(longpoll)), pkg.command, pkg.sequence, (char *)pkg.body);
    
    free(pkg.body);
    json_decref(params);
}

static int on_update_depth(json_t *result)
{
    const char *market = json_string_value(json_object_get(result, "market"));
    const char *interval = json_string_value(json_object_get(result, "interval"));
    const int limit = json_integer_value(json_object_get(result, "limit"));
    json_t *depth_data = json_object_get(result, "data");
    if ( (market == NULL) || (interval == NULL) || (depth_data == NULL) ) {
        log_error("depth result invalid");
        return -__LINE__;
    }
    if ((strcmp("0", interval) != 0) || (limit != 1)) {
        log_error("depth interval not 0 or limit not %d", 1);
        return -__LINE__;
    }

    dict_entry *entry = dict_find(dict_depth, market);
    if (entry == NULL) {
        struct depth_val val;
        memset(&val, 0, sizeof(struct depth_val));
        val.time = current_timestamp();
        val.data = depth_data;
        json_incref(val.data);
        if (dict_add(dict_depth, (char *)market, &val) == NULL) {
            log_error("add market:%s depth failed", market);
            return -__LINE__;
        } 
        return 0;
    }

    struct depth_val *val = entry->val;
    json_decref(val->data);
    val->time = current_timestamp();
    val->data = depth_data;
    json_incref(val->data);

    return 0;
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    ut_rpc_reply_t *rpc_reply = reply_load(pkg->body, pkg->body_size);
    if (rpc_reply == NULL) {
        log_fatal("reply_load returns NULL, system maybe lack memory");
        return ;
    }

    do {
        if (!reply_valid(rpc_reply)) {
            REPLY_INVALID_LOG(ses, pkg);
            break;
        }
        if (!reply_ok(rpc_reply)) {
            REPLY_ERROR_LOG(ses, pkg);
            break;
        }

        if (pkg->command == CMD_LP_DEPTH_UPDATE) {
            int ret = on_update_depth(rpc_reply->result);
            if (ret != 0) {
                REPLY_ERROR_LOG(ses, pkg);
            }
        } else {
            if (pkg->command != CMD_LP_DEPTH_SUBSCRIBE_ALL) {
                log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
            }
        }
    } while(0);

    reply_release(rpc_reply);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    if (result) {
        log_info("connect to longpoll success");
        longpoll_subscribe_depth();
    } else {
        log_error("can not connect to longpoll...");
    }
}

int init_depth(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = dict_market_hash_func;
    dt.key_compare = dict_market_compare;
    dt.key_dup = dict_market_dup;
    dt.key_destructor = dict_market_free;
    dt.val_dup = dict_depth_val_dup;
    dt.val_destructor = dict_depth_val_free;

    dict_depth = dict_create(&dt, 64);
    if (dict_depth == NULL) {
        return -__LINE__;
    }
    
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    longpoll = rpc_clt_create(&settings.longpoll, &ct);
    if (longpoll == NULL) {
        return -__LINE__;
    }
    if (rpc_clt_start(longpoll) < 0){
        return -__LINE__;
    }

    return 0;
}

static json_t* generate_depth_data(json_t *depth_data, int limit) {
    if (depth_data == NULL) {
        return json_array();
    }

    json_t *new_data = json_array();
    int size = json_array_size(depth_data) > limit ? limit : json_array_size(depth_data);
    for (int i = 0; i < size; ++i) {
        json_t *unit = json_array_get(depth_data, i);
        json_array_append(new_data, unit);
    }

    return new_data;
}

json_t* depth_get_last_one(const char *market)
{
    dict_entry *entry = dict_find(dict_depth, market);
    if (entry == NULL) {
        return NULL;
    }

    struct depth_val *val = entry->val;
    json_t *asks_array = json_object_get(val->data, "asks");
    json_t *bids_array = json_object_get(val->data, "bids");

    json_t *new_depth_data = json_object();
    json_object_set_new(new_depth_data, "asks", generate_depth_data(asks_array, 1));
    json_object_set_new(new_depth_data, "bids", generate_depth_data(bids_array, 1));
    json_object_set    (new_depth_data, "last", json_object_get(val->data, "last"));
    json_object_set    (new_depth_data, "time", json_object_get(val->data, "time"));

    return new_depth_data;
}

void fini_depth(void)
{
    dict_release(dict_depth);
}
