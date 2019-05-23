/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/26, create
 */

# include "hw_dispatcher.h"
# include "hw_job.h"
# include "hw_message.h"
# include "hw_dump.h"
# include "ut_mysql.h"
# include "ut_profile.h"
# include "ut_define.h"

struct dict_sql_key {
    uint32_t hash;
    uint32_t type;
};

static dict_t **dict_sqls = NULL;
static history_job_t **jobs = NULL;
static MYSQL *mysql_conn = NULL;
static nw_timer timer;

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

static dict_t* create_dict_sql(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_sql_hash_function;
    dt.key_compare    = dict_sql_key_compare;
    dt.key_dup        = dict_sql_key_dup;
    dt.key_destructor = dict_sql_key_free;

    return dict_create(&dt, 1024);
}

static uint32_t get_db_no(uint32_t user_id)
{
    return (user_id % (settings.db_histories.count * HISTORY_HASH_NUM)) / HISTORY_HASH_NUM;
}

static uint32_t get_hash_num(uint32_t user_id)
{
    return user_id % HISTORY_HASH_NUM;
}

static struct job_val* get_sql(uint32_t user_id, struct dict_sql_key *key)
{
    uint32_t db =  get_db_no(user_id);
    dict_t *dict_sql = dict_sqls[db];
    dict_entry *entry = dict_find(dict_sql, key);
    if (!entry) {
        struct job_val *val = (struct job_val *)malloc(sizeof(struct job_val));
        memset(val, 0, sizeof(struct job_val));
       
        val->db = db;
        val->type = key->type;
        val->sql = sdsempty();
        entry = dict_add(dict_sql, key, val);
        if (entry == NULL) {
            job_val_free(val);
            return NULL;
        }
    }
    return entry->val;
}

static void on_dipatch_job(dict_t *dict_sql, int db)
{
    dict_iterator *iter = dict_get_iterator(dict_sql);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        submit_job(jobs[db], entry->val);
        dict_delete(dict_sql, entry->key);
    }
    dict_release_iterator(iter);
}

static bool is_history_block(void)
{
    int db_count = settings.db_histories.count;
    for (int i = 0; i < db_count; ++i) {
        if (jobs[i]->job->request_count >= MAX_PENDING_HISTORY) {
            return true;
        }
    }

    return false;
}

static void on_timer(nw_timer *t, void *privdata)
{
    if (is_history_block()) {
        log_warn("history block to much, stop historywriter and check why this happen.");
        message_stop(2);
        nw_timer_stop(t);
        return ;
    }
    int db_count = settings.db_histories.count;
    for (int i = 0; i < db_count; ++i) {
        on_dipatch_job(dict_sqls[i], i);
    }
}

int init_dispatcher(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_histories.configs[0].charset) != 0)
        return -__LINE__;

    int db_count = settings.db_histories.count;
    dict_sqls = malloc(sizeof(dict_t) * db_count);
    for (int i = 0; i < db_count; ++i) {
        dict_sqls[i] = create_dict_sql();
        if (dict_sqls[i] == NULL) {
            log_stderr("create dict failed at:%d", i);
            return -__LINE__;
        }
    }

    jobs = malloc(sizeof(history_job_t) * db_count);
    for (int i = 0; i < db_count; ++i) {
        jobs[i] = create_history_job(i);
        if (jobs[i] == NULL) {
            log_stderr("create job failed at:%d", i);
            return -__LINE__;
        }
    }

    nw_timer_set(&timer, settings.flush_his_interval, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int dispatch_order(json_t *msg)
{
    const double create_time = json_real_value(json_object_get(msg, "ctime"));
    const double update_time = json_real_value(json_object_get(msg, "mtime"));
    const uint64_t order_id = json_integer_value(json_object_get(msg, "id"));
    const uint32_t user_id  = json_integer_value(json_object_get(msg, "user"));
    const uint32_t account  = json_integer_value(json_object_get(msg, "account"));
    const int type = json_integer_value(json_object_get(msg, "type"));
    const int side = json_integer_value(json_object_get(msg, "side"));
    
    const char *market       = json_string_value(json_object_get(msg, "market"));
    const char *source       = json_string_value(json_object_get(msg, "source"));
    const char *fee_asset    = json_string_value(json_object_get(msg, "fee_asset"));
    const char *fee_discount = json_string_value(json_object_get(msg, "fee_discount"));
    const char *price        = json_string_value(json_object_get(msg, "price"));
    const char *amount       = json_string_value(json_object_get(msg, "amount"));
    const char *taker_fee    = json_string_value(json_object_get(msg, "taker_fee"));
    const char *maker_fee    = json_string_value(json_object_get(msg, "maker_fee"));
    const char *deal_stock   = json_string_value(json_object_get(msg, "deal_stock"));
    const char *deal_money   = json_string_value(json_object_get(msg, "deal_money"));
    const char *deal_fee     = json_string_value(json_object_get(msg, "deal_fee"));
    const char *asset_fee    = json_string_value(json_object_get(msg, "asset_fee"));

    struct dict_sql_key key;
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_ORDER;
    struct job_val *val = get_sql(user_id, &key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `order_history_%u` (`order_id`, `create_time`, `finish_time`, `user_id`, `account`, "
                "`market`, `source`, `fee_asset`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, "
                "`deal_stock`, `deal_money`, `deal_fee`, `asset_fee`, `fee_discount`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, %u, '%s', '%s', '%s', %u, %u, ", order_id,
        create_time, update_time, user_id, account, market, source, fee_asset ? fee_asset : "", type, side);
    sql = sdscatprintf(sql, "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", 
        price, amount, taker_fee, maker_fee, deal_stock, deal_money, deal_fee, asset_fee, fee_discount);
    val->sql = sql;
    profile_inc("history_user_order", 1);

    return 0;
}

int dispatch_stop(json_t *msg)
{
    const double create_time = json_real_value(json_object_get(msg, "ctime"));
    const double update_time = json_real_value(json_object_get(msg, "mtime"));
    const uint64_t stop_id = json_integer_value(json_object_get(msg, "id"));
    const uint32_t user_id = json_integer_value(json_object_get(msg, "user"));
    const uint32_t account = json_integer_value(json_object_get(msg, "account"));
    const int type   = json_integer_value(json_object_get(msg, "type"));
    const int side   = json_integer_value(json_object_get(msg, "side"));
    const int status = json_integer_value(json_object_get(msg, "status"));
    
    const char *market       = json_string_value(json_object_get(msg, "market"));
    const char *source       = json_string_value(json_object_get(msg, "source"));
    const char *fee_asset    = json_string_value(json_object_get(msg, "fee_asset"));
    const char *fee_discount = json_string_value(json_object_get(msg, "fee_discount"));
    const char *stop_price   = json_string_value(json_object_get(msg, "stop_price"));
    const char *price        = json_string_value(json_object_get(msg, "price"));
    const char *amount       = json_string_value(json_object_get(msg, "amount"));
    const char *taker_fee    = json_string_value(json_object_get(msg, "taker_fee"));
    const char *maker_fee    = json_string_value(json_object_get(msg, "maker_fee"));

    struct dict_sql_key key;
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_STOP;
    struct job_val *val = get_sql(user_id, &key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `stop_history_%u` (`order_id`, `create_time`, `finish_time`, `user_id`, `account`, `market`, `source`, "
                "`fee_asset`, `t`, `side`, `status`, `stop_price`, `price`, `amount`, `taker_fee`, `maker_fee`, `fee_discount`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, %u, '%s', '%s', '%s', %u, %u, %d, ", stop_id, create_time, update_time,
            user_id, account, market, source, fee_asset ? fee_asset : "", type, side, status);
    sql = sdscatprintf(sql, "'%s', '%s', '%s', '%s', '%s', '%s')", stop_price, price, amount, taker_fee, maker_fee, fee_discount);
    val->sql = sql;
    profile_inc("history_user_stop", 1);
 
    return 0;
}

static int append_user_deal(double t, uint32_t user_id, uint32_t account, uint32_t deal_user_id, uint32_t deal_account, const char *market, uint64_t deal_id, uint64_t order_id, 
        uint64_t deal_order_id, int side, int role, const char *price, const char *amount, const char *deal, const char *fee_asset, const char *fee, const char *deal_fee_asset, const char *deal_fee)
{
    struct dict_sql_key key;
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_DEAL;
    struct job_val *val = get_sql(user_id, &key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `user_deal_history_%u` (`time`, `user_id`, `account`, `deal_user_id`, `deal_account`, `market`, `deal_id`, `order_id`, `deal_order_id`, "
                "`side`, `role`, `price`, `amount`, `deal`, `fee`, `deal_fee`, `fee_asset`, `deal_fee_asset`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%f, %u, %u, %u, %u,'%s', %"PRIu64", %"PRIu64", %"PRIu64", %d, %d, ", t, user_id, account, deal_user_id, deal_account, market, deal_id, order_id, deal_order_id, side, role);
    sql = sdscatprintf(sql, "'%s', '%s', '%s', '%s', '%s', '%s', '%s')", price, amount, deal, fee, deal_fee, fee_asset ? fee_asset : "", deal_fee_asset ? deal_fee_asset : "");
    val->sql = sql;
    profile_inc("history_user_deal", 1);

    return 0;
}

int dispatch_deal(json_t *msg)
{
    const double time = json_real_value(json_object_get(msg, "time"));
    const uint64_t deal_id      = json_integer_value(json_object_get(msg, "deal_id"));
    const uint64_t ask_order_id = json_integer_value(json_object_get(msg, "ask_order_id"));
    const uint64_t bid_order_id = json_integer_value(json_object_get(msg, "bid_order_id"));
    const uint32_t ask_user_id  = json_integer_value(json_object_get(msg, "ask_user_id"));
    const uint32_t ask_account  = json_integer_value(json_object_get(msg, "ask_account"));
    const uint32_t bid_user_id  = json_integer_value(json_object_get(msg, "bid_user_id"));
    const uint32_t bid_account  = json_integer_value(json_object_get(msg, "bid_account"));
    const int ask_side     = json_integer_value(json_object_get(msg, "ask_side"));
    const int bid_side     = json_integer_value(json_object_get(msg, "bid_side"));
    const int ask_role     = json_integer_value(json_object_get(msg, "ask_role"));
    const int bid_role     = json_integer_value(json_object_get(msg, "bid_role"));
    
    const char *market        = json_string_value(json_object_get(msg, "market"));
    const char *amount        = json_string_value(json_object_get(msg, "amount"));
    const char *price         = json_string_value(json_object_get(msg, "price"));
    const char *deal          = json_string_value(json_object_get(msg, "deal"));
    const char *ask_fee       = json_string_value(json_object_get(msg, "ask_fee"));
    const char *bid_fee       = json_string_value(json_object_get(msg, "bid_fee"));
    const char *ask_fee_asset = json_string_value(json_object_get(msg, "ask_fee_asset"));
    const char *bid_fee_asset = json_string_value(json_object_get(msg, "bid_fee_asset"));
    
    append_user_deal(time, ask_user_id, ask_account, bid_user_id, bid_account, market, deal_id, ask_order_id, bid_order_id, ask_side, ask_role, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
    append_user_deal(time, bid_user_id, bid_account, ask_user_id, ask_account, market, deal_id, bid_order_id, ask_order_id, bid_side, bid_role, price, amount, deal, bid_fee_asset, bid_fee, ask_fee_asset, ask_fee);
   
    return 0;
}

int dispatch_balance(json_t *msg)
{
    const double time = json_real_value(json_object_get(msg, "time"));
    const uint32_t user_id = json_integer_value(json_object_get(msg, "user_id"));
    const uint32_t account = json_integer_value(json_object_get(msg, "account"));
    const char *asset    = json_string_value(json_object_get(msg, "asset"));
    const char *business = json_string_value(json_object_get(msg, "business"));
    const char *detail   = json_string_value(json_object_get(msg, "detail"));
    const char *change   = json_string_value(json_object_get(msg, "change"));
    const char *balance  = json_string_value(json_object_get(msg, "balance"));

    struct dict_sql_key key;
    key.hash = get_hash_num(user_id);
    key.type = HISTORY_USER_BALANCE;
    struct job_val *val = get_sql(user_id, &key);
    if (val == NULL) 
        return -__LINE__;

    sds sql = val->sql;
    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `balance_history_%u` (`time`, `user_id`, `account`, `asset`, `business`, `change`, `balance`, `detail`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%f, %u, %u, '%s', '%s', '%s', '%s'", time, user_id, account, asset, business, change, balance);
    char buf[10 * 1024] = {0};
    mysql_real_escape_string(mysql_conn, buf, detail, strlen(detail));
    sql = sdscatprintf(sql, ", '%s')", buf);
    
    val->sql = sql;
    profile_inc("history_user_balance", 1);

    return 0;
}

sds history_status(sds reply)
{
    int db_count = settings.db_histories.count;
    for (int i = 0; i < db_count; ++i) {
        reply = sdscatprintf(reply, "history db[%d] pending %d\n", i, jobs[i]->job->request_count);
    }

    return reply;
}

int dump_hisotry(void)
{
    nw_timer_start(&timer);
    printf("going to sleep 2 seconds");
    sleep(2);

    dump_uncompleted_history(dict_sqls, jobs);
    job_quit(1);
    return 0;
}