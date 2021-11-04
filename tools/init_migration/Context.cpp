#include "Context.h"
#include "Progress.h"

int Context::init() {
    reporter_ =  make_shared<Reporter>();
    time_counter_ = make_shared<TimeCounter>(reporter_);
    Progress::printer_ = reporter_.get();
    if (load_config())
        return -1;
    if (load_currency())
        return -1;
    if (load_market())
        return -1;

    redisContext *redis_ctx = get_redis_connection();
    if (redis_ctx==NULL)
        return -2;

    return 0;
}

int Context::load_config() {
    src_db_conn_ = mysql_connect(&settings_.db_src);
    if (src_db_conn_==nullptr) {
        log_error("coonect src db fail");
        return -1;
    }

    for (size_t i=0;i<DB_HISTORY_COUNT;i++) {
        MYSQL *conn = mysql_connect(&settings.db_histories[i]);
        if (conn==nullptr) {
            log_error("connect history db faile");
            return -1;
        }
        if (mysql_autocommit(conn , 0)) {
            log_error("mysql_autocommit fail");
        }
        history_db_conns_.push_back(conn);
    }
    log_db_conn_ = mysql_connect(&settings.db_log);
    if (log_db_conn_==nullptr) {
        log_error("connect log db fail");
        return -1;
    }
    mysql_autocommit(log_db_conn_ , 0);
//    summary_db_conn_ = mysql_connect(&settings.db_summary);
//    if (summary_db_conn_==nullptr) {
//        log_error("connect summary db fail");
//        return -1;
//    }

    return 0;
}


int Context::load_currency() {
    string sql = "select currency_id,symbol from core_currency";
    int ret = mysql_real_query(src_db_conn_, sql.c_str(), sql.length());
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(src_db_conn_), mysql_error(src_db_conn_));
        return -1;
    }

    MYSQL_RES *result = mysql_store_result(src_db_conn_);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)))  {
        shared_ptr<Coin> coin = make_shared<Coin>();
        coin->id_ = strtol(row[0],nullptr,0);
        coin->name_ = row[1];
        coins_[coin->id_] = coin;
    }

    return 0;
}

int Context::load_market() {
    string sql = "select contract_id,symbol,from_fixed from core_contract where appl_id=1";
    int ret = mysql_real_query(src_db_conn_, sql.c_str(), sql.length());
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(src_db_conn_), mysql_error(src_db_conn_));
        return -1;
    }

    MYSQL_RES *result = mysql_store_result(src_db_conn_);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)))  {
        shared_ptr<Market> market = make_shared<Market>();
        market->id_ = strtol(row[0],nullptr,0);
        market->name_ = row[1];
        market->from_fixed = strtol(row[2],nullptr,0);
        markets_[market->id_] = market;
    }
    return 0;
}
