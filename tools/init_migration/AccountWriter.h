#ifndef ACCOUNTWRITER_H_INCLUDED
#define ACCOUNTWRITER_H_INCLUDED

#include "Writer.h"

class BalanceWriter : public Writer {
    static const uint8_t FIELD_NUM = 6;
    const FieldTypeFormat _fields_[FIELD_NUM] = {
        {"user_id","%llu"},
        {"account","%d"},
        {"asset","'%s'"},
        {"t","%d"},
        {"balance","%s"},
        {"update_time","%f"}
    };
public:
    BalanceWriter(const Context &ctx):Writer(ctx){
        fields_ = const_cast<FieldTypeFormat*>(&_fields_[0]);
        field_num_ = FIELD_NUM;
        field_list_ = generate_field_list();
        table_tag_ = "slice_balance";
    }

    const string generate_value_list(const balance_t &balance) {
        return Utils::format_string(generate_value_format().c_str(),
          balance.user_id,
          balance.account,
          balance.asset,
          balance.t,
          balance.balance ? Utils::mpd_to_string(balance.balance).c_str():"0",
          balance.update_time
          );
    }

    int batch_insert(CLargeStringArray& value_vec,int exec_count) {
        string table_name = generate_table_name();
        SQLExecutor executor(ctx_.log_db_conn_);
        return Utils::batch_insert(&executor,table_name.c_str(),field_list_.c_str(),value_vec,exec_count);
    }


    int generate_table() {
        string table_name = generate_table_name();
        MYSQL *conn = ctx_.log_db_conn_;
        string sql = Utils::format_string("CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_balance_example`", table_name.c_str());
        int ret = mysql_real_query(conn, sql.c_str(), sql.length());
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(conn), mysql_error(conn));
            return -1;
        }
        return 0;
    }

private:
    const string generate_table_name() {
        return Utils::format_string("%s_%d",table_tag_.c_str(),ctx_.timestamp_);
    }
};

#endif // ACCOUNTWRITER_H_INCLUDED
