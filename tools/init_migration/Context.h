#ifndef CONTEXT_H
#define CONTEXT_H

#include<vector>
#include<string>
#include<map>
#include<memory>
#include "Config.h"
#include "Reporter.h"
#include "TimeCounter.h"

using namespace std;
extern "C" {
# include "ut_mysql.h"
# include "ut_redis.h"
}

struct Coin {
    int id_;
    string name_;
};

struct Market {
    int id_; /// 交易对ID
    string name_; /// 交易对名称
    uint8_t from_fixed;
};

struct TypeOrder {
    uint8_t type_; /// 1-order 2-stop
    void *order_;
    TypeOrder(uint8_t type,void *order):type_(type),order_(order) {
    }
};

class Context {
public:
    Context(struct settings &settings):settings_(settings) {
    }
    int init();

    const Coin* get_coin(int id) const {
        auto iter = coins_.find(id);
        return iter==coins_.end() ? nullptr : iter->second.get();
    }
    const Market* get_market(int id) const {
        auto iter = markets_.find(id);
        return iter==markets_.end() ? nullptr : iter->second.get();
    }
    const void* get_type_order(const string &order_id) const {
        auto iter = orders_->find(order_id);
        return iter==orders_->end() ? nullptr : iter->second.get()->order_;
    }

    redisContext *get_redis_connection()   {
        return redis_connect(&settings_.redis);
    }

    struct settings &settings_;

    MYSQL* src_db_conn_;
    vector<MYSQL*> history_db_conns_;
    MYSQL* log_db_conn_;
    MYSQL* summary_db_conn_;

    map<int,shared_ptr<Coin>> coins_;
    map<int,shared_ptr<Market>> markets_;
    map<string,shared_ptr<TypeOrder>> *orders_;
    time_t timestamp_;

    shared_ptr<Reporter> reporter_;
    shared_ptr<TimeCounter> time_counter_;
private:
    int load_config();
    int load_currency();
    int load_market();
};

#endif // CONTEXT_H
