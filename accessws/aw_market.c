/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/01/28, create
 */

# include "aw_market.h"
# include "aw_http.h"
# include "aw_common.h"

static dict_t *dict_market = NULL;
static rpc_clt *longpoll = NULL;

static int init_dict_market()
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_str_hash_func;
    dt.key_dup        = dict_str_dup;
    dt.key_destructor = dict_str_free;
    dt.key_compare    = dict_str_compare;

    dict_market = dict_create(&dt, 64);
    if (dict_market == NULL) {
        log_stderr("dict_market dict_create failed");
        return -__LINE__;
    }

    return 0;
}

static void update_market(json_t *market_array)
{
    const size_t array_size = json_array_size(market_array);
    if (array_size == 0) {
        return ;
    }

    dict_clear(dict_market);

    for (size_t i = 0; i < array_size; ++i) {
        json_t *item = json_array_get(market_array, i);
        char *name = strdup(json_string_value(json_object_get(item, "name")));
        dict_add(dict_market, name, NULL);
        free(name);
    }
}

static void fetch_markets()
{
    json_t *params = json_array();
    send_http_request("market.list", params, update_market);
}

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u, reply: %s",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence, reply_str);

    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (reply == NULL) {
        sds hex = hexdump(pkg->body, pkg->body_size);
        log_fatal("invalid reply from: %s, cmd: %u, reply: \n%s", nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
        sdsfree(hex);
        sdsfree(reply_str);
        return;
    }

    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || !json_is_null(error) || result == NULL) {
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        json_decref(reply);
        return;
    }
    sdsfree(reply_str);

    switch (pkg->command) {
        case CMD_LP_MARKET_SUBSCRIBE:
            log_trace("market subscribe success");
            break;
        case CMD_LP_MARKET_UNSUBSCRIBE:
            log_trace("market unsubscribe success");
            break;
        case CMD_LP_MARKET_UPDATE:
            log_trace("update market");
            update_market(result);
            break;
        default:
            log_error("recv unknown command: %u from: %s", pkg->command, nw_sock_human_addr(&ses->peer_addr));
            break;
    }

    json_decref(reply);
}

static void subscribe_market(void)
{
    json_t *params = json_array();
    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_LP_MARKET_SUBSCRIBE;
    pkg.sequence  = 0;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(longpoll, &pkg);
    free(pkg.body);
    json_decref(params);
}

static void on_backend_connect(nw_ses *ses, bool result)
{
    if (result) {
        subscribe_market();
    } else {
        log_error("can not connect to longpoll...");
    }
}

int init_market(void)
{
    int ret = init_dict_market();
    if (ret != 0) {
        return ret;
    }

    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    longpoll = rpc_clt_create(&settings.longpoll, &ct);
    if (longpoll == NULL) {
        log_stderr("longpoll rpc_clt_create failed");
        return -__LINE__;
    }
    if (rpc_clt_start(longpoll) < 0){
        log_stderr("longpoll rpc_clt_start failed");
        return -__LINE__;
    }
    
    fetch_markets();

    return 0;
}

json_t* get_market_array(void)
{
    if (dict_size(dict_market) == 0) {
        return NULL;
    }
    
    json_t *market_array = json_array();
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ( (entry = dict_next(iter)) != NULL) {
        const char *market = entry->key;
        json_array_append_new(market_array, json_string(market));
    }

    dict_release_iterator(iter);
    return market_array;
}

bool market_exists(const char *market)
{
    return (dict_find(dict_market, market) != NULL);
}

dict_t* get_market(void)
{
    return dict_market;
}

void fini_market(void)
{
    dict_release(dict_market);
}