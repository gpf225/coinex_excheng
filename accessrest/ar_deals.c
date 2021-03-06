/*
 * Description: 
 *     History: ouxiangyang, 2019/04/19, create
 */

# include "ar_config.h"
# include "ar_deals.h"
# include "ar_server.h"
# include "ar_ticker.h"

static dict_t *dict_deals;
static rpc_clt *cache_deals;

struct deals_val {
    list_t *deals;
    uint64_t last_id;
};

// dict deals
static void *dict_deals_val_dup(const void *val)
{
    struct deals_val *obj = malloc(sizeof(struct deals_val));
    memcpy(obj, val, sizeof(struct deals_val));
    return obj;
}

static void dict_deals_val_free(void *val)
{
    struct deals_val *obj = val;
    if (obj->deals)
        list_release(obj->deals);
    free(obj);
}

static void list_free(void *value)
{
    json_decref(value);
}

// deals reply
static json_t *pack_deals_result(list_t *deals, uint32_t limit, int64_t last_id)
{
    int count = 0;
    json_t *result = json_array();
    list_iter *iter = list_get_iterator(deals, LIST_START_HEAD);
    list_node *node;
    while ((node = list_next(iter)) != NULL) {
        json_t *deal = node->value;
        uint64_t id = json_integer_value(json_object_get(deal, "id"));
        if (id <= last_id) {
            break;
        }

        json_t *unit = json_object();
        json_object_set(unit, "id", json_object_get(deal, "id"));
        json_object_set(unit, "type", json_object_get(deal, "type"));
        json_object_set(unit, "price", json_object_get(deal, "price"));
        json_object_set(unit, "amount", json_object_get(deal, "amount"));
        double date = json_real_value(json_object_get(deal, "time"));
        json_object_set_new(unit, "date", json_integer((time_t)date));
        json_object_set_new(unit, "date_ms", json_integer((int64_t)(date * 1000)));
        json_array_append_new(result, unit);

        count += 1;
        if (count == limit) {
            break;
        }
    }
    list_release_iterator(iter);

    return result;
}

// deals update
static int on_deals_update(json_t *result_array, nw_ses *ses, rpc_pkg *pkg)
{
    const char *market = json_string_value(json_array_get(result_array, 0));
    json_t *result = json_array_get(result_array, 1);
    if (market == NULL || result == NULL) {
        sds reply_str = sdsnewlen(pkg->body, pkg->body_size);
        log_error("error reply from: %s, cmd: %u, reply: %s", nw_sock_human_addr(&ses->peer_addr), pkg->command, reply_str);
        sdsfree(reply_str);
        return -__LINE__;
    }

    log_trace("deals update, market: %s", market);
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry == NULL) {
        struct deals_val val;
        memset(&val, 0, sizeof(val));

        list_type lt;
        memset(&lt, 0, sizeof(lt));
        lt.free = list_free;

        val.deals = list_create(&lt);
        if (val.deals == NULL)
            return -__LINE__;

        entry = dict_add(dict_deals, (char *)market, &val);
        if (entry == NULL) {
            list_release(val.deals);
            return -__LINE__;
        }
    }

    if (!json_is_array(result))
        return -__LINE__;
    size_t array_size = json_array_size(result);
    if (array_size == 0)
        return 0;

    json_t *first = json_array_get(result, 0);
    uint64_t first_id = json_integer_value(json_object_get(first, "id"));    
    if (first_id == 0)
        return -__LINE__;

    struct deals_val *obj = entry->val;
    log_info("deal sub reply, market: %s, array_size: %zd, last_id: %zd, first_id: %zd", market, array_size, obj->last_id, first_id);

    for (size_t i = array_size; i > 0; --i) {
        json_t *deal = json_array_get(result, i - 1);

        uint64_t id = json_integer_value(json_object_get(deal, "id"));
        if (id > obj->last_id) {
            json_incref(deal);
            list_add_node_head(obj->deals, deal);  
        }
    }

    obj->last_id = first_id;

    while (obj->deals->len > settings.deal_max) {
        list_del(obj->deals, list_tail(obj->deals));
    }

    return 0;
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

static void on_backend_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    log_trace("recv pkg from: %s, cmd: %u, sequence: %u",
            nw_sock_human_addr(&ses->peer_addr), pkg->command, pkg->sequence);
    json_t *reply = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (!reply) {
        log_error("json_loadb fail");
        goto clean;
    }
    json_t *error = json_object_get(reply, "error");
    if (!error) {
        log_error("error param not find");
        goto clean;
    }
    if (!json_is_null(error)) {
        log_error("error is not null");
        goto clean;
    }

    json_t *result = json_object_get(reply, "result");
    if (!result) {
        log_error("result param not find");
        goto clean;
    }

    switch (pkg->command) {
    case CMD_CACHE_DEALS_UPDATE:
        on_deals_update(result, ses, pkg);
        break;
    default:
        break;
    }

clean:
    if (reply)
        json_decref(reply);

    return;
}

void direct_deals_result(nw_ses *ses, const char *market, int limit, uint64_t last_id)
{
    bool is_reply = false;
    dict_entry *entry = dict_find(dict_deals, market);
    if (entry != NULL) {
        struct deals_val *val = entry->val;
        if (val->deals != NULL) {
            is_reply = true;
            json_t *result = pack_deals_result(val->deals, limit, last_id);
            reply_json(ses, result);
            json_decref(result);
        }
    }

    if (!is_reply) {
        reply_result_null(ses);
        log_error("deals not find result, market: %s", market);
    }

    return;
}

int init_deals(void)
{
    rpc_clt_type ct;
    memset(&ct, 0, sizeof(ct));
    ct.on_connect = on_backend_connect;
    ct.on_recv_pkg = on_backend_recv_pkg;

    cache_deals = rpc_clt_create(&settings.cache_deals, &ct);
    if (cache_deals == NULL)
        return -__LINE__;
    if (rpc_clt_start(cache_deals) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = str_dict_hash_function;
    dt.key_compare    = str_dict_key_compare;
    dt.key_dup        = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.val_dup        = dict_deals_val_dup;
    dt.val_destructor = dict_deals_val_free;
    dict_deals = dict_create(&dt, 64);
    if (dict_deals == NULL)
        return -__LINE__;

    return 0;
}

