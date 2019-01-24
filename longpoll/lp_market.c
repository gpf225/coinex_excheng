/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# include "lp_market.h"
# include "lp_common_struct.h"
# include "lp_server.h"
# include "lp_statistic.h"

static json_t *last_market_info = NULL;
static dict_t *dict_sub;
static dict_t *dict_market;
static nw_timer update_market_timer;
static rpc_clt *matchengine = NULL;

struct market_val {
    int id;
};

static void *dict_market_val_dup(const void *key)
{
    struct market_val *obj = malloc(sizeof(struct market_val));
    memcpy(obj, key, sizeof(struct market_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    struct market_val *obj = val;
    free(obj);
}

static int init_dict_market()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = common_str_hash_func;
    dt.key_dup        = common_str_const_dup;
    dt.key_destructor = common_str_free;
    dt.key_compare    = common_str_compare;
    dt.val_dup        = dict_market_val_dup;
    dt.val_destructor = dict_market_val_free;

    dict_market = dict_create(&dt, 64);
    if (dict_market == NULL) {
        log_stderr("dict_market dict_create failed");
        return -__LINE__;
    }

    return 0;
}

static void notify_subscribers(json_t *params)
{
    if (dict_size(dict_sub) == 0) {
        log_info("no client subscribe market list");
        return ;
    }
    
    int count = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_sub);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        notify_message(ses, CMD_LP_MARKET_UPDATE, params);
        ++count;
    }
    dict_release_iterator(iter);
    stat_market_update(count);
}

void on_market_update(json_t *params)
{
    static uint32_t update_id = 0;
    update_id += 1;
    bool need_notify = false;

    for (size_t i = 0; i < json_array_size(params); ++i) {
        json_t *item = json_array_get(params, i);
        const char *name = json_string_value(json_object_get(item, "name"));
        dict_entry *entry = dict_find(dict_market, name);
        if (entry == NULL) {
            struct market_val val;
            memset(&val, 0, sizeof(val));
            val.id = update_id;
            dict_add(dict_market, (char *)name, &val);
            need_notify = true;
            log_info("add market: %s", name);
        } else {
            struct market_val *info = entry->val;
            info->id = update_id;
        }
    }

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_val *info = entry->val;
        if (info->id != update_id) {
            dict_delete(dict_market, entry->key);
            need_notify = true;
            log_info("del market: %s", (char *)entry->key);
        }
    }
    dict_release_iterator(iter);

    if (need_notify) {
        if (last_market_info != NULL) {
            json_decref(last_market_info);
        }
        last_market_info = params;
        json_incref(last_market_info);

        notify_subscribers(params);
    }
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

        if (pkg->command == CMD_MARKET_LIST) {
            on_market_update(rpc_reply->result);
        } else {
            log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        }
    } while(0);

    reply_release(rpc_reply);
}

static void poll_market_list(void)
{
    static uint32_t sequence = 0;

    if (!rpc_clt_connected(matchengine)) {
        log_error("matchengine does not connected");
        return ;
    }
    stat_market_poll();
    
    json_t *params = json_array();
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_MARKET_LIST;
    pkg.sequence  = ++sequence;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(matchengine, &pkg);
    free(pkg.body);
    json_decref(params);
}

static void on_poll_market(nw_timer *timer, void *privdata)
{
    poll_market_list();
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        poll_market_list();
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

int init_market(void)
{
    int ret = init_dict_market();
    if (ret != 0) {
        return ret;
    }

    dict_sub = common_create_ses_dict(16);
    if (dict_sub == NULL) {
        return -__LINE__;
    }

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;
    
    matchengine = rpc_clt_create(&settings.matchengine, &ct);
    if (matchengine == NULL) {
        log_stderr("rpc_clt_create matchengine failed");
        return -__LINE__;
    }
    if (rpc_clt_start(matchengine) < 0) {
        log_stderr("rpc_clt_start matchengine failed");
        return -__LINE__;
    }
    
    nw_timer_set(&update_market_timer, settings.poll_market_interval, true, on_poll_market, NULL);
    nw_timer_start(&update_market_timer);

    return 0;
}

int market_subscribe(nw_ses *ses)
{
    log_info("market_subscribe:%p", ses);
    dict_add(dict_sub, ses, NULL);
    return 0;
}

int market_unsubscribe(nw_ses *ses)
{
    log_info("market_unsubscribe:%p", ses);
    dict_delete(dict_sub, ses);
    return 0;
}

int market_send_last(nw_ses *ses)
{
    if (last_market_info == NULL) {
        return 0;
    }
    return notify_message(ses, CMD_LP_MARKET_UPDATE, last_market_info);
}

size_t market_subscribe_number(void)
{
    return dict_size(dict_sub);
}

dict_t* get_market(void)
{
    return dict_market;
}

bool market_exists(const char *market)
{
    return (dict_find(dict_market, market) != NULL);
}