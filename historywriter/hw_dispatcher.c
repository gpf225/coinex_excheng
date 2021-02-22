/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_writer.h"
# include "hw_dispatcher.h"

# define MAX_SQL_SIZE (1000 * 1000)

enum {
    HISTORY_USER_DEAL,
    HISTORY_USER_STOP,
    HISTORY_USER_ORDER,
    HISTORY_USER_BALANCE,
};

struct dict_sql_key {
    uint32_t db;
    uint32_t hash;
    uint32_t type;
};

static MYSQL *mysql_conn;
static nw_timer timer;
static dict_t *dict_sql;

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

static uint32_t get_db_num(uint32_t user_id)
{
    return (user_id % (settings.db_history_count * HISTORY_HASH_NUM)) / HISTORY_HASH_NUM;
}

static uint32_t get_hash_num(uint32_t user_id)
{
    return user_id % HISTORY_HASH_NUM;
}

static sds get_sql(struct dict_sql_key *key)
{
    dict_entry *entry = dict_find(dict_sql, key);
    if (!entry) {
        entry = dict_add(dict_sql, key, sdsempty());
    }
    return entry->val;
}

static int set_sql(struct dict_sql_key *key, sds sql)
{
    if (sdslen(sql) > MAX_SQL_SIZE) {
        submit_job(key->db, sql);
        dict_delete(dict_sql, key);
        return 0;
    }

    dict_entry *entry = dict_find(dict_sql, key);
    if (entry == NULL)
        return -__LINE__;

    entry->val = sql;
    return 0;
}

static void flush_sql(void)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_sql);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_sql_key *key = entry->key;
        submit_job(key->db, entry->val);
        dict_delete(dict_sql, key);
    }
    dict_release_iterator(iter);
}

static void on_timer(nw_timer *t, void *privdata)
{
    flush_sql();
}

int init_dispatcher(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_histories[0].charset) != 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_sql_hash_function;
    dt.key_compare    = dict_sql_key_compare;
    dt.key_dup        = dict_sql_key_dup;
    dt.key_destructor = dict_sql_key_free;

    dict_sql = dict_create(&dt, 1024);
    if (dict_sql == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.5, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

void fini_dispatcher(void)
{
    flush_sql();
}

static int append_user_deal(double t, uint32_t user_id, uint32_t account, uint32_t deal_user_id,  uint32_t deal_account, const char *market,
        uint64_t deal_id, uint64_t order_id, uint64_t deal_order_id, int side, int role, const char *price, const char *amount, const char *deal,
        const char *fee_asset, const char *fee, const char *deal_fee_asset, const char *deal_fee)
{
    struct dict_sql_key key;
    key.db   = get_db_num(user_id);
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_DEAL;
    sds sql  = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `user_deal_history_%u` (`time`, `user_id`, `account`, `deal_user_id`, `deal_account`, `market`, `deal_id`, `order_id`, `deal_order_id`, "
                "`side`, `role`, `price`, `amount`, `deal`, `fee`, `deal_fee`, `fee_asset`, `deal_fee_asset`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%f, %u, %u, %u, %u, '%s', %"PRIu64", %"PRIu64", %"PRIu64", %d, %d, ", t, user_id, account, deal_user_id, deal_account, market, deal_id,
            order_id, deal_order_id, side, role);
    sql = sdscatprintf(sql, "'%s', '%s', '%s', '%s', '%s', '%s', '%s')", price, amount, deal, fee, deal_fee, fee_asset ? fee_asset : "", deal_fee_asset ? deal_fee_asset : "");

    set_sql(&key, sql);
    profile_inc("history_user_deal", 1);

    return 0;
}

int dispatch_deal(json_t *msg)
{
    double time                 = json_real_value(json_object_get(msg, "time"));
    uint64_t deal_id            = json_integer_value(json_object_get(msg, "deal_id"));
    uint64_t ask_order_id       = json_integer_value(json_object_get(msg, "ask_order_id"));
    uint64_t bid_order_id       = json_integer_value(json_object_get(msg, "bid_order_id"));
    uint32_t ask_user_id        = json_integer_value(json_object_get(msg, "ask_user_id"));
    uint32_t ask_account        = json_integer_value(json_object_get(msg, "ask_account"));
    uint32_t bid_user_id        = json_integer_value(json_object_get(msg, "bid_user_id"));
    uint32_t bid_account        = json_integer_value(json_object_get(msg, "bid_account"));
    int ask_side                = json_integer_value(json_object_get(msg, "ask_side"));
    int bid_side                = json_integer_value(json_object_get(msg, "bid_side"));
    int ask_role                = json_integer_value(json_object_get(msg, "ask_role"));
    int bid_role                = json_integer_value(json_object_get(msg, "bid_role"));
    
    const char *market          = json_string_value(json_object_get(msg, "market"));
    const char *amount          = json_string_value(json_object_get(msg, "amount"));
    const char *price           = json_string_value(json_object_get(msg, "price"));
    const char *deal            = json_string_value(json_object_get(msg, "deal"));
    const char *ask_fee         = json_string_value(json_object_get(msg, "ask_fee"));
    const char *bid_fee         = json_string_value(json_object_get(msg, "bid_fee"));
    const char *ask_fee_asset   = json_string_value(json_object_get(msg, "ask_fee_asset"));
    const char *bid_fee_asset   = json_string_value(json_object_get(msg, "bid_fee_asset"));
    
    append_user_deal(time, ask_user_id, ask_account, bid_user_id, bid_account, market, deal_id, ask_order_id, bid_order_id, ask_side, ask_role,
            price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
    append_user_deal(time, bid_user_id, bid_account, ask_user_id, ask_account, market, deal_id, bid_order_id, ask_order_id, bid_side, bid_role,
            price, amount, deal, bid_fee_asset, bid_fee, ask_fee_asset, ask_fee);
   
    return 0;
}

int dispatch_stop(json_t *msg)
{
    double create_time          = json_real_value(json_object_get(msg, "ctime"));
    double update_time          = json_real_value(json_object_get(msg, "mtime"));
    uint64_t stop_id            = json_integer_value(json_object_get(msg, "id"));
    uint32_t user_id            = json_integer_value(json_object_get(msg, "user"));
    uint32_t account            = json_integer_value(json_object_get(msg, "account"));
    uint32_t option             = json_integer_value(json_object_get(msg, "option"));
    int type                    = json_integer_value(json_object_get(msg, "type"));
    int side                    = json_integer_value(json_object_get(msg, "side"));
    int status                  = json_integer_value(json_object_get(msg, "status"));
    
    const char *market          = json_string_value(json_object_get(msg, "market"));
    const char *source          = json_string_value(json_object_get(msg, "source"));
    const char *fee_asset       = json_string_value(json_object_get(msg, "fee_asset"));
    const char *fee_discount    = json_string_value(json_object_get(msg, "fee_discount"));
    const char *stop_price      = json_string_value(json_object_get(msg, "stop_price"));
    const char *price           = json_string_value(json_object_get(msg, "price"));
    const char *amount          = json_string_value(json_object_get(msg, "amount"));
    const char *taker_fee       = json_string_value(json_object_get(msg, "taker_fee"));
    const char *maker_fee       = json_string_value(json_object_get(msg, "maker_fee"));
    const char *client_id       = json_string_value(json_object_get(msg, "client_id"));

    struct dict_sql_key key;
    key.db   = get_db_num(user_id);
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_STOP;
    sds sql  = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `stop_history_%u` (`order_id`, `create_time`, `finish_time`, `user_id`, `account`, `option`, `market`, `source`, "
                "`fee_asset`, `t`, `side`, `status`, `stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`, `fee_discount`, `client_id` ) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, %u, %u, '%s', '%s', '%s', %u, %u, %d, ", stop_id, create_time, update_time,
            user_id, account, option, market, source, fee_asset ? fee_asset : "", type, side, status);
    sql = sdscatprintf(sql, "'%s', '%s', '%s', '%s', '%s', '%s', '%s')", stop_price, price, amount, taker_fee, maker_fee, fee_discount,
            client_id ? client_id : "");

    set_sql(&key, sql);
    profile_inc("history_user_stop", 1);
 
    return 0;
}

int dispatch_order(json_t *msg)
{
    double create_time          = json_real_value(json_object_get(msg, "ctime"));
    double update_time          = json_real_value(json_object_get(msg, "mtime"));
    uint64_t order_id           = json_integer_value(json_object_get(msg, "id"));
    uint32_t user_id            = json_integer_value(json_object_get(msg, "user"));
    uint32_t account            = json_integer_value(json_object_get(msg, "account"));
    uint32_t option             = json_integer_value(json_object_get(msg, "option"));
    int type                    = json_integer_value(json_object_get(msg, "type"));
    int side                    = json_integer_value(json_object_get(msg, "side"));
    
    const char *market          = json_string_value(json_object_get(msg, "market"));
    const char *source          = json_string_value(json_object_get(msg, "source"));
    const char *fee_asset       = json_string_value(json_object_get(msg, "fee_asset"));
    const char *fee_discount    = json_string_value(json_object_get(msg, "fee_discount"));
    const char *price           = json_string_value(json_object_get(msg, "price"));
    const char *amount          = json_string_value(json_object_get(msg, "amount"));
    const char *taker_fee       = json_string_value(json_object_get(msg, "taker_fee"));
    const char *maker_fee       = json_string_value(json_object_get(msg, "maker_fee"));
    const char *deal_stock      = json_string_value(json_object_get(msg, "deal_stock"));
    const char *deal_money      = json_string_value(json_object_get(msg, "deal_money"));
    const char *money_fee       = json_string_value(json_object_get(msg, "money_fee"));
    const char *stock_fee       = json_string_value(json_object_get(msg, "stock_fee"));
    const char *asset_fee       = json_string_value(json_object_get(msg, "asset_fee"));
    const char *client_id       = json_string_value(json_object_get(msg, "client_id"));

    struct dict_sql_key key;
    key.db   = get_db_num(user_id);
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_ORDER;
    sds sql  = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `order_history_%u` (`order_id`, `create_time`, `finish_time`, `user_id`, `account`, `option`, "
                "`market`, `source`, `fee_asset`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, "
                "`deal_stock`, `deal_money`, `money_fee`, `stock_fee`, `asset_fee`, `fee_discount`, `client_id`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, %u, %u, '%s', '%s', '%s', %u, %u, ", order_id,
        create_time, update_time, user_id, account, option, market, source, fee_asset ? fee_asset : "", type, side);
    sql = sdscatprintf(sql, "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", 
        price, amount, taker_fee, maker_fee, deal_stock, deal_money, money_fee, stock_fee, asset_fee, fee_discount,
        client_id ? client_id : "");

    set_sql(&key, sql);
    profile_inc("history_user_order", 1);

    return 0;
}

int dispatch_balance(json_t *msg)
{
    double time             = json_real_value(json_object_get(msg, "time"));
    uint32_t user_id        = json_integer_value(json_object_get(msg, "user_id"));
    uint32_t account        = json_integer_value(json_object_get(msg, "account"));
    const char *asset       = json_string_value(json_object_get(msg, "asset"));
    const char *business    = json_string_value(json_object_get(msg, "business"));
    const char *detail      = json_string_value(json_object_get(msg, "detail"));
    const char *change      = json_string_value(json_object_get(msg, "change"));
    const char *balance     = json_string_value(json_object_get(msg, "balance"));

    struct dict_sql_key key;
    key.db   = get_db_num(user_id);
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_BALANCE;
    sds sql  = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `balance_history_%u` (`time`, `user_id`, `account`, `asset`, `business`, `change`, `balance`, `detail`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%f, %u, %u, '%s', '%s', '%s', '%s'", time, user_id, account, asset, business, change, balance);
    size_t detail_len = strlen(detail);
    char buf[detail_len * 2 + 1];
    mysql_real_escape_string(mysql_conn, buf, detail, detail_len);
    sql = sdscatprintf(sql, ", '%s')", buf);
    
    set_sql(&key, sql);
    profile_inc("history_user_balance", 1);

    return 0;
}

