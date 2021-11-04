#include "Runner.h"

# include<memory>

using namespace std;

extern "C" {
# include "ut_log.h"
}


const char *config_file_name = "config.json";

Runner::Runner() {
}

Runner::~Runner() {
}

int init_log(void) {
    default_dlog = dlog_init(settings.log.path, settings.log.shift, settings.log.max, settings.log.num, settings.log.keep);
    if (default_dlog == NULL)
        return -__LINE__;
    default_dlog_flag = dlog_read_flag(settings.log.flag);

    return 0;
}

int Runner::init() {
    printf("init config...\n");
    int ret = init_config(config_file_name);
    if (ret) {
        printf("init config error\n");
        return -1;
    }
    printf("init log...\n");
    ret = init_log();
    if (ret) {
        printf("init log error,ret=%d,errno=%d\n",ret,errno);
        return -1;
    }

    if (init_mpd()) {
        log_error("init mpd error");
        return -1;
    }

    printf("context init...\n");
    ctx_ = make_shared<Context>(settings);
    if (ctx_->init()) {
        log_error("context init error");
        return -1;
    }

    printf("runner::init ok\n");
    return 0;
}

const char version[] = "0.9";

int Runner::run() {
    printf("version:%s\n",version);

    if (ctx_->settings_.mode&0x02) {
        ctx_->time_counter_->start("clear data");
        clear_data();
        flush_redis();
        ctx_->time_counter_->watch();
    }
    if (ctx_->settings_.mode&0x01) {
        order_transfer_ = make_shared<OrderTransfer>(*this->ctx_.get());
        order_transfer_->run();

        deal_transfer_ = make_shared<DealTransfer>(*this->ctx_.get());
        deal_transfer_->run();

        account_transfer_ = make_shared<AccountTransfer>(*this->ctx_.get());
        account_transfer_->run();

        if (init_slice_history())
            return -1;
        if (new_slice_order_table())
            return -1;

    }
    if (ctx_->settings_.mode&0x04) {
        kline_transfer_ = make_shared<KlineTransfer>(*this->ctx_.get());
        kline_transfer_->run();
    }

    if (commit())
        return -1;

    ctx_->reporter_->output();

    return 0;
}


int Runner::init_slice_history() {
    SQLExecutor executor(ctx_->log_db_conn_);
    string sql = Utils::format_string("insert into slice_history(time, end_oper_id, end_order_id, end_deals_id)"
        " values(%llu,%llu,%llu,%llu)",
        ctx_->timestamp_,
        0,
        order_transfer_->get_order_id_start(),
        deal_transfer_->get_deal_id_start()
        );
    if (executor.exec(sql))
        return -1;

    return 0;
}

int Runner::new_slice_order_table() {
    MYSQL *conn = ctx_->log_db_conn_;
    char table_patterns[][32] = {"slice_order","slice_update","slice_stop"};
    for (size_t i=0;i<sizeof(table_patterns)/sizeof(table_patterns[0]);i++) {
        string table_name = Utils::format_string("%s_%d",table_patterns[i],ctx_->timestamp_);
        string sql = Utils::format_string("CREATE TABLE IF NOT EXISTS `%s` LIKE `%s_example`", table_name.c_str(),table_patterns[i]);
        int ret = mysql_real_query(conn, sql.c_str(), sql.length());
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(conn), mysql_error(conn));
            return -1;
        }
    }

    return 0;
}

int Runner::commit() {
    for (auto conn : ctx_->history_db_conns_) {
        if (mysql_commit(conn)) {
            log_error("commit fail: %d %s", mysql_errno(conn), mysql_error(conn));
            return -1;
        }
    }
    if (mysql_commit(ctx_->log_db_conn_)) {
        log_error("commit fail: %d %s", mysql_errno(ctx_->log_db_conn_), mysql_error(ctx_->log_db_conn_));
        return -1;
    }

    return 0;
}

void Runner::clear_data() {
    for (uint8_t db_id=0;db_id<DB_HISTORY_COUNT;db_id++) {
        for (uint16_t slice_id = 0;slice_id<HISTORY_HASH_NUM;slice_id++) {
            char tables[][30] = {"order_history","stop_history","user_deal_history"};
            for (size_t i=0;i<sizeof(tables)/sizeof(tables[0]);i++) {//  90/30
                SQLExecutor executor(ctx_->history_db_conns_[db_id]);
                string table_name = Utils::format_string("%s_%d",tables[i],slice_id);
                string sql = Utils::format_string("truncate table %s",table_name.c_str());
                executor.exec(sql);
            }
        }
    }
}

void Runner::flush_redis() {
    redisContext *redis_ctx = ctx_->get_redis_connection();
    redisAppendCommand(redis_ctx, "flushall");
    redisReply *reply = NULL;
    if (redisGetReply(redis_ctx,(void **)&reply) == REDIS_OK)
        freeReplyObject(reply);
}


