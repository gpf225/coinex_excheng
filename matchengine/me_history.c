/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/06, create
 */

# include "me_config.h"
# include "me_history.h"
# include "me_balance.h"
# include "me_message.h"

static MYSQL *mysql_conn;
static nw_job *job;
static dict_t *dict_sql;
static nw_timer timer;

enum {
    HISTORY_USER_BALANCE,
    HISTORY_USER_ORDER,
    HISTORY_USER_STOP,
    HISTORY_USER_DEAL,
    HISTORY_ORDER_DETAIL,
    HISTORY_ORDER_DEAL,
};

struct dict_sql_key {
    uint32_t type;
    uint32_t hash;
};

struct dict_sql_val {
    sds     sql;
};

static uint32_t dict_sql_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_sql_key));
}

static void *dict_sql_key_dup(const void *key)
{
    struct dict_sql_key *obj = malloc(sizeof(struct dict_sql_key));
    memcpy(obj, key, sizeof(struct dict_sql_key));
    return obj;
}

static int dict_sql_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct dict_sql_key));
}

static void dict_sql_key_free(void *key)
{
    free(key);
}

static void dict_sql_val_free(struct dict_sql_val *val)
{
    if (val->sql) {
        sdsfree(val->sql);
    }
    free(val);
}

static void *on_job_init(void)
{
    return mysql_connect(&settings.db_history);
}

static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    struct dict_sql_val *val = entry->request;
    log_trace("exec sql: %s", val->sql);
    while (true) {
        int ret = mysql_real_query(conn, val->sql, sdslen(val->sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_fatal("exec sql: %s fail: %d %s", val->sql, mysql_errno(conn), mysql_error(conn));
            usleep(1000 * 1000);
            continue;
        }
        break;
    }
}

static void on_job_cleanup(nw_job_entry *entry)
{
    dict_sql_val_free(entry->request);
}

static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

static void on_timer(nw_timer *t, void *privdata)
{
    dict_iterator *iter = dict_get_iterator(dict_sql);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        nw_job_add(job, 0, entry->val);
        dict_delete(dict_sql, entry->key);
    }
    dict_release_iterator(iter);
}

int init_history(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_history.charset) != 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_sql_hash_function;
    dt.key_compare    = dict_sql_key_compare;
    dt.key_dup        = dict_sql_key_dup;
    dt.key_destructor = dict_sql_key_free;

    dict_sql = dict_create(&dt, 1024);
    if (dict_sql == 0) {
        return -__LINE__;
    }

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_init    = on_job_init;
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;
    jt.on_release = on_job_release;

    job = nw_job_create(&jt, settings.history_thread);
    if (job == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.5, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int fini_history(void)
{
    on_timer(NULL, NULL);

    usleep(100 * 1000);
    nw_job_release(job);

    return 0;
}

static sds sql_append_mpd(sds sql, mpd_t *val, bool comma)
{
    char *str = mpd_to_sci(val, 0);
    sql = sdscatprintf(sql, "'%s'", str);
    if (comma) {
        sql = sdscatprintf(sql, ", ");
    }
    free(str);
    return sql;
}

static struct dict_sql_val *get_sql(struct dict_sql_key *key)
{
    dict_entry *entry = dict_find(dict_sql, key);
    if (!entry) {
        struct dict_sql_val *val = (struct dict_sql_val *)malloc(sizeof(struct dict_sql_val));
        memset(val, 0, sizeof(struct dict_sql_val));

        val->sql = sdsempty();
        entry = dict_add(dict_sql, key, val);
        if (entry == NULL) {
            dict_sql_val_free(val);
            return NULL;
        }
    }
    return entry->val;
}

static int append_user_order(order_t *order)
{
    struct dict_sql_key key;
    key.hash = order->user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_ORDER;
    struct dict_sql_val *val = get_sql(&key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `order_history_%u` (`id`, `create_time`, `finish_time`, `user_id`, "
                "`market`, `source`, `fee_asset`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, "
                "`deal_stock`, `deal_money`, `deal_fee`, `asset_fee`, `fee_discount`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, '%s', '%s', '%s', %u, %u, ", order->id,
        order->create_time, order->update_time, order->user_id, order->market, order->source, order->fee_asset ? order->fee_asset : "", order->type, order->side);
    sql = sql_append_mpd(sql, order->price, true);
    sql = sql_append_mpd(sql, order->amount, true);
    sql = sql_append_mpd(sql, order->taker_fee, true);
    sql = sql_append_mpd(sql, order->maker_fee, true);
    sql = sql_append_mpd(sql, order->deal_stock, true);
    sql = sql_append_mpd(sql, order->deal_money, true);
    sql = sql_append_mpd(sql, order->deal_fee, true);
    sql = sql_append_mpd(sql, order->asset_fee, true);
    sql = sql_append_mpd(sql, order->fee_discount, false);
    sql = sdscatprintf(sql, ")");

    val->sql = sql;
    profile_inc("history_user_order", 1);

    return 0;
}

static int append_order_detail(order_t *order)
{
    struct dict_sql_key key;
    key.hash = order->id % HISTORY_HASH_NUM;
    key.type = HISTORY_ORDER_DETAIL;
    struct dict_sql_val *val = get_sql(&key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `order_detail_%u` (`id`, `create_time`, `finish_time`, `user_id`, "
                "`market`, `source`, `fee_asset`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, "
                "`deal_stock`, `deal_money`, `deal_fee`, `asset_fee`, `fee_discount`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, '%s', '%s', '%s', %u, %u, ", order->id,
        order->create_time, order->update_time, order->user_id, order->market, order->source, order->fee_asset ? order->fee_asset : "", order->type, order->side);
    sql = sql_append_mpd(sql, order->price, true);
    sql = sql_append_mpd(sql, order->amount, true);
    sql = sql_append_mpd(sql, order->taker_fee, true);
    sql = sql_append_mpd(sql, order->maker_fee, true);
    sql = sql_append_mpd(sql, order->deal_stock, true);
    sql = sql_append_mpd(sql, order->deal_money, true);
    sql = sql_append_mpd(sql, order->deal_fee, true);
    sql = sql_append_mpd(sql, order->asset_fee, true);
    sql = sql_append_mpd(sql, order->fee_discount, false);
    sql = sdscatprintf(sql, ")");

    val->sql = sql;
    profile_inc("history_order_detail", 1);

    return 0;
}

static int append_order_deal(double t, uint32_t user_id, uint64_t deal_id, uint64_t order_id, uint64_t deal_order_id, int role,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *fee_asset, mpd_t *fee, const char *deal_fee_asset, mpd_t *deal_fee)
{
    struct dict_sql_key key;
    key.hash = order_id % HISTORY_HASH_NUM;
    key.type = HISTORY_ORDER_DEAL;
    struct dict_sql_val *val = get_sql(&key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `order_deal_history_%u` (`id`, `time`, `user_id`, `deal_id`, `order_id`, `deal_order_id`, `role`, "
                "`price`, `amount`, `deal`, `fee`, `deal_fee`, `fee_asset`, `deal_fee_asset`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(NULL, %f, %u, %"PRIu64", %"PRIu64", %"PRIu64", %d, ", t, user_id, deal_id, order_id, deal_order_id, role);
    sql = sql_append_mpd(sql, price, true);
    sql = sql_append_mpd(sql, amount, true);
    sql = sql_append_mpd(sql, deal, true);
    sql = sql_append_mpd(sql, fee, true);
    sql = sql_append_mpd(sql, deal_fee, true);
    sql = sdscatprintf(sql, "'%s', '%s')", fee_asset, deal_fee_asset);

    val->sql = sql;
    profile_inc("history_order_deal", 1);

    return 0;
}

static int append_user_deal(double t, uint32_t user_id, const char *market, uint64_t deal_id, uint64_t order_id, uint64_t deal_order_id, int side, int role,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *fee_asset, mpd_t *fee, const char *deal_fee_asset, mpd_t *deal_fee)
{
    struct dict_sql_key key;
    key.hash = user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_DEAL;
    struct dict_sql_val *val = get_sql(&key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `user_deal_history_%u` (`id`, `time`, `user_id`, `market`, `deal_id`, `order_id`, `deal_order_id`, "
                "`side`, `role`, `price`, `amount`, `deal`, `fee`, `deal_fee`, `fee_asset`, `deal_fee_asset`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(NULL, %f, %u, '%s', %"PRIu64", %"PRIu64", %"PRIu64", %d, %d, ", t, user_id, market, deal_id, order_id, deal_order_id, side, role);
    sql = sql_append_mpd(sql, price, true);
    sql = sql_append_mpd(sql, amount, true);
    sql = sql_append_mpd(sql, deal, true);
    sql = sql_append_mpd(sql, fee, true);
    sql = sql_append_mpd(sql, deal_fee, true);
    sql = sdscatprintf(sql, "'%s', '%s')", fee_asset, deal_fee_asset);

    val->sql = sql;
    profile_inc("history_user_deal", 1);

    return 0;
}

static int append_user_balance(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change, mpd_t *balance, const char *detail)
{
    struct dict_sql_key key;
    key.hash = user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_BALANCE;
    struct dict_sql_val *val = get_sql(&key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `balance_history_%u` (`id`, `time`, `user_id`, `asset`, `business`, `change`, `balance`, `detail`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    char buf[10 * 1024];
    sql = sdscatprintf(sql, "(NULL, %f, %u, '%s', '%s', ", t, user_id, asset, business);
    sql = sql_append_mpd(sql, change, true);
    sql = sql_append_mpd(sql, balance, true);
    mysql_real_escape_string(mysql_conn, buf, detail, strlen(detail));
    sql = sdscatprintf(sql, "'%s')", buf);
    
    val->sql = sql;
    profile_inc("history_user_balance", 1);

    return 0;
}

int append_order_history(order_t *order)
{
    if (settings.history_mode & HISTORY_MODE_DIRECT) {
        append_user_order(order);
        append_order_detail(order);  
    }
    if (settings.history_mode & HISTORY_MODE_KAFKA) {
        json_t *order_info = get_order_info(order);
        push_his_order_message(order_info); 
        json_decref(order_info);
    }

    return 0;
}

int append_stop_history(stop_t *stop, int status)
{
    if (settings.history_mode & HISTORY_MODE_DIRECT) {
        struct dict_sql_key key;
        key.hash = stop->user_id % HISTORY_HASH_NUM;
        key.type = HISTORY_USER_STOP;
        struct dict_sql_val *val = get_sql(&key);
        if (val == NULL) 
            return -__LINE__;

        sds sql = val->sql;
        if (sdslen(sql) == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `stop_history_%u` (`id`, `create_time`, `finish_time`, `user_id`, `market`, `source`, "
                    "`fee_asset`, `t`, `side`, `status`, `stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`, `fee_discount`) VALUES ", key.hash);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, '%s', '%s', '%s', %u, %u, %d, ", stop->id, stop->create_time, stop->update_time,
                stop->user_id, stop->market, stop->source, stop->fee_asset, stop->type, stop->side, status);
        sql = sql_append_mpd(sql, stop->stop_price, true);
        sql = sql_append_mpd(sql, stop->price, true);
        sql = sql_append_mpd(sql, stop->amount, true);
        sql = sql_append_mpd(sql, stop->taker_fee, true);
        sql = sql_append_mpd(sql, stop->maker_fee, true);
        sql = sql_append_mpd(sql, stop->fee_discount, false);
        sql = sdscatprintf(sql, ")");

        val->sql = sql;
    }

    if (settings.history_mode & HISTORY_MODE_KAFKA) {
        json_t *order_info = get_stop_info(stop);
        json_object_set_new(order_info, "status", json_integer(status));
        push_his_stop_message(order_info); 
        json_decref(order_info);
    }
    return 0;
}

static json_t* get_deals_json(double t, uint64_t deal_id, order_t *ask, int ask_role, order_t *bid, int bid_role,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *ask_fee_asset, mpd_t *ask_fee, const char *bid_fee_asset, mpd_t *bid_fee)
{
    json_t *obj = json_object();
    json_object_set_new(obj, "time", json_real(t));
    json_object_set_new(obj, "market", json_string(ask->market));
    json_object_set_new(obj, "deal_id", json_integer(deal_id));
    json_object_set_new(obj, "ask_user_id", json_integer(ask->user_id));
    json_object_set_new(obj, "bid_user_id", json_integer(bid->user_id));
    json_object_set_new(obj, "ask_order_id", json_integer(ask->id));
    json_object_set_new(obj, "bid_order_id", json_integer(bid->id));
    json_object_set_new(obj, "ask_side", json_integer(ask->side));
    json_object_set_new(obj, "bid_side", json_integer(bid->side));
    json_object_set_new(obj, "ask_role", json_integer(ask_role));
    json_object_set_new(obj, "bid_role", json_integer(bid_role));

    json_object_set_new_mpd(obj, "price", price);
    json_object_set_new_mpd(obj, "amount", amount);
    json_object_set_new_mpd(obj, "deal", deal);
    json_object_set_new_mpd(obj, "ask_fee", ask_fee);
    json_object_set_new_mpd(obj, "bid_fee", bid_fee);
    json_object_set_new(obj, "ask_fee_asset", json_string(ask_fee_asset));
    json_object_set_new(obj, "bid_fee_asset", json_string(bid_fee_asset));

    return obj;
}

int append_order_deal_history(double t, uint64_t deal_id, order_t *ask, int ask_role, order_t *bid, int bid_role,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *ask_fee_asset, mpd_t *ask_fee, const char *bid_fee_asset, mpd_t *bid_fee)
{
    if (settings.history_mode & HISTORY_MODE_DIRECT) {
        append_order_deal(t, ask->user_id, deal_id, ask->id, bid->id, ask_role, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        append_order_deal(t, bid->user_id, deal_id, bid->id, ask->id, bid_role, price, amount, deal, bid_fee_asset, bid_fee, ask_fee_asset, ask_fee);

        append_user_deal(t, ask->user_id, ask->market, deal_id, ask->id, bid->id, ask->side, ask_role, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        append_user_deal(t, bid->user_id, ask->market, deal_id, bid->id, ask->id, bid->side, bid_role, price, amount, deal, bid_fee_asset, bid_fee, ask_fee_asset, ask_fee);
    }

    if (settings.history_mode & HISTORY_MODE_KAFKA) {
        json_t *obj = get_deals_json(t, deal_id, ask, ask_role, bid, bid_role, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
        push_his_deal_message(obj); 
        json_decref(obj);
    }

    return 0;
}

static json_t* get_balance_json(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change,  mpd_t *balance, const char *detail)
{
    json_t *obj = json_object();
    json_object_set_new(obj, "time", json_real(t));
    json_object_set_new(obj, "user_id", json_integer(user_id));
    json_object_set_new(obj, "asset", json_string(asset));
    json_object_set_new(obj, "business", json_string(business));
    json_object_set_new_mpd(obj, "change", change);
    json_object_set_new_mpd(obj, "balance", balance);
    json_object_set_new(obj, "detail", json_string(detail));
    return obj;
}

int append_user_balance_history(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change, const char *detail)
{
    if (settings.history_mode & HISTORY_MODE_DIRECT) {
        mpd_t *balance = balance_total(user_id, asset);
        append_user_balance(t, user_id, asset, business, change, balance, detail);
        mpd_del(balance);
    }
    if (settings.history_mode & HISTORY_MODE_KAFKA) {
        mpd_t *balance = balance_total(user_id, asset);
        json_t *obj = get_balance_json(t, user_id, asset, business, change, balance, detail);
        push_his_balance_message(obj); 
        mpd_del(balance);
        json_decref(obj);
    }

    return 0;
}

bool is_history_block(void)
{
    if (job->request_count >= MAX_PENDING_HISTORY) {
        return true;
    }
    return false;
}

sds history_status(sds reply)
{
    return sdscatprintf(reply, "history pending %d\n", job->request_count);
}

