/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# include "lp_state.h"
# include "lp_common_struct.h"
# include "lp_market.h"
# include "lp_server.h"
# include "lp_statistic.h"

static dict_t *dict_sub = NULL; 
static dict_t *dict_market_state = NULL; 

static rpc_clt *marketprice = NULL;
static nw_state *state_context;
static nw_timer update_state_timer;

struct state_data {
    char market[MARKET_NAME_MAX_LEN];
};

struct market_state_val {
    json_t *last;
};

static void *dict_market_state_val_dup(const void *key)
{
    struct market_state_val *obj = malloc(sizeof(struct market_state_val));
    memcpy(obj, key, sizeof(struct market_state_val));
    return obj;
}

static void dict_market_state_val_free(void *val)
{
    struct market_state_val *obj = val;
    if (obj->last) {
        json_decref(obj->last);
    }
    free(obj);
}

static int init_dict_market_state(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = common_str_hash_func;
    dt.key_dup        = common_str_const_dup;
    dt.key_destructor = common_str_free;
    dt.key_compare    = common_str_compare;
    dt.val_dup        = dict_market_state_val_dup;
    dt.val_destructor = dict_market_state_val_free;

    dict_market_state = dict_create(&dt, 64);
    if (dict_market_state == NULL) {
        log_stderr("dict_create failed");
        return -__LINE__;
    }
    return 0;
}

static json_t *get_notify_full(void)
{
    json_t *result = json_object();
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market_state);
    while ((entry = dict_next(iter)) != NULL) {
        struct market_state_val *info = entry->val;
        json_object_set(result, entry->key, info->last);
    }
    dict_release_iterator(iter);
    return result;
}

static void notify_subscribers(void)
{ 
    if (dict_size(dict_sub) == 0) {
        return ;
    }
    if (dict_size(dict_market_state) == 0) {
        log_info("no state data availabe, maybe this is the first time to notify");
        return ;
    }

    json_t *result = get_notify_full();
    int count = 0;
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_sub);
    while ((entry = dict_next(iter)) != NULL) {
        nw_ses *ses = entry->key;
        notify_message(ses, CMD_LP_STATE_UPDATE, result);
        ++count;
    }
    dict_release_iterator(iter);
    json_decref(result);
 
    stat_state_update(count);
    return ;
}

static void on_poll_state(nw_timer *timer, void *privdata)
{
    if (!rpc_clt_connected(marketprice)) {
        log_error("marketprice does not connected");
        return ;
    }

    dict_t *dict_market = get_market();
    if (dict_size(dict_market) == 0) {
        log_warn("no markets, this maybe a bug");
        return ;
    }
    
    notify_subscribers();

    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        json_t *params = json_array();
        json_array_append_new(params, json_string(market));
        json_array_append_new(params, json_integer(86400));

        nw_state_entry *state_entry = nw_state_add(state_context, settings.backend_timeout, 0);
        struct state_data *state = state_entry->data;
        strncpy(state->market, market, MARKET_NAME_MAX_LEN - 1);
        
        rpc_pkg pkg;
        memset(&pkg, 0, sizeof(pkg));
        pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
        pkg.command   = CMD_MARKET_STATUS;
        pkg.sequence  = state_entry->id;
        pkg.body      = json_dumps(params, 0);
        pkg.body_size = strlen(pkg.body);

        rpc_clt_send(marketprice, &pkg);
        free(pkg.body);
        json_decref(params);
    }
    dict_release_iterator(iter);
}

static void on_state_update(const char *market, json_t *result)
{
    dict_entry *entry = dict_find(dict_market_state, market);
    if (entry == NULL) {
        struct market_state_val val;
        memset(&val, 0, sizeof(val));
        val.last = result;
        json_incref(val.last);

        dict_add(dict_market_state, (char*)market, &val);
        return ;
    }
    
    struct market_state_val *info = entry->val;
    json_decref(info->last);
    info->last = result;
    json_incref(info->last);
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    REPLY_TRACE_LOG(ses, pkg);

    nw_state_entry *state_entry = nw_state_get(state_context, pkg->sequence);
    if (state_entry == NULL) {
        return;
    }

    ut_rpc_reply_t *rpc_reply = reply_load(pkg->body, pkg->body_size);
    if (rpc_reply == NULL) {
        log_fatal("reply_load returns NULL, system maybe lack memory");
        nw_state_del(state_context, pkg->sequence);
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
        
        struct state_data *state = state_entry->data;
        const char *market = state->market;
        if (pkg->command == CMD_MARKET_STATUS) {
            on_state_update(market, rpc_reply->result);
        } else {
            log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
        }
    } while(0);

    reply_release(rpc_reply);
    nw_state_del(state_context, pkg->sequence);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_timeout(nw_state_entry *entry)
{
    log_fatal("query market state, state id: %u", entry->id);
}

int init_state(void)
{
    int ret = init_dict_market_state();
    if (ret < 0) {
        return ret;
    }
    dict_sub = common_create_ses_dict(16);
    if (dict_sub == NULL) {
        return -__LINE__;
    }
    
    nw_state_type st;
    memset(&st, 0, sizeof(st));
    st.on_timeout = on_timeout;
    state_context = nw_state_create(&st, sizeof(struct state_data));
    if (state_context == NULL) {
        log_stderr("nw_state_create failed");
        return -__LINE__;
    }

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;
    
    marketprice = rpc_clt_create(&settings.marketprice, &ct);
    if (marketprice == NULL) {
        log_stderr("rpc_clt_create marketprice failed");
        return -__LINE__;
    }
    if (rpc_clt_start(marketprice) < 0) {
        log_stderr("rpc_clt_start marketprice failed");
        return -__LINE__;
    }
    
    nw_timer_set(&update_state_timer, settings.poll_depth_interval, true, on_poll_state, NULL);
    nw_timer_start(&update_state_timer);

    return 0;
}

int state_subscribe(nw_ses *ses)
{
    log_info("state_subscribe:%p", ses);
    dict_add(dict_sub, ses, NULL);
    return 0;
}

int state_unsubscribe(nw_ses *ses)
{
    log_info("state_unsubscribe:%p", ses);
    dict_delete(dict_sub, ses);
    return 0;
}

int state_send_last(nw_ses *ses)
{
    json_t *result = get_notify_full();
    int ret = notify_message(ses, CMD_LP_STATE_UPDATE, result);
    json_decref(result);
    return ret;
}

size_t state_subscribe_number(void)
{
    return dict_size(dict_sub);
}

void fini_state(void) 
{
    dict_release(dict_sub);
    dict_release(dict_market_state);
}