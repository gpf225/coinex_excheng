#ifndef KLINEWRITER_H_INCLUDED
#define KLINEWRITER_H_INCLUDED


#include "Writer.h"

class KlineWriter : public Writer {
    static const uint8_t FIELD_NUM = 9;
    const FieldTypeFormat _fields_[FIELD_NUM] = {
        {"market","'%s'"},
        {"t","%d"},
        {"timestamp","%llu"},
        {"open","%s"},
        {"close","%s"},
        {"high","%s"},
        {"low","%s"},
        {"volume","%s"},
        {"deal","%s"}
    };
public:
    KlineWriter(const Context &ctx):Writer(ctx){
        fields_ = const_cast<FieldTypeFormat*>(&_fields_[0]);
        field_num_ = FIELD_NUM;
        field_list_ = generate_field_list();
        table_tag_ = "kline_history";
    }

    const string generate_value_list(const kline_info_t &kline_info) {
        return Utils::format_string(generate_value_format().c_str(),
          kline_info.market,
          kline_info.type,
          kline_info.timestamp,
          kline_info.open ? Utils::mpd_to_string(kline_info.open).c_str() : "0",
          kline_info.close ? Utils::mpd_to_string(kline_info.close).c_str() : "0",
          kline_info.high ? Utils::mpd_to_string(kline_info.high).c_str() : "0",
          kline_info.low ? Utils::mpd_to_string(kline_info.low).c_str() : "0",
          kline_info.volume ? Utils::mpd_to_string(kline_info.volume).c_str() : "0",
          kline_info.deal ? Utils::mpd_to_string(kline_info.deal).c_str() : "0"
          );
    }

    int generate_table() {
        const string table_name = generate_table_name();
        if (checked_tables_.find(table_name)!=checked_tables_.end())
            return 0;
        MYSQL *conn = ctx_.log_db_conn_;
        string sql = Utils::format_string("CREATE TABLE IF NOT EXISTS `%s` LIKE `kline_history_example`", table_name.c_str());
        int ret = mysql_real_query(ctx_.log_db_conn_, sql.c_str(), sql.length());
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(conn), mysql_error(conn));
            return -1;
        }

        checked_tables_[table_name] = 0;
        return 0;
    }

    int batch_insert(CLargeStringArray& value_vec,int exec_count) {
        const string table_name = generate_table_name();
        SQLExecutor executor(ctx_.log_db_conn_);
        return Utils::batch_insert(&executor,table_name.c_str(),field_list_.c_str(),value_vec,exec_count);
    }

    uint32_t segment_id_; /// kline to be divided into multiple tables according to YYYYMM,like a segment
private:
    string generate_table_name() {
        return Utils::format_string("%s_%d",table_tag_.c_str(),segment_id_);
    }
    map<string,uint8_t> checked_tables_; /// store name of tables which already check existed
};




#endif // KLINEWRITER_H_INCLUDED
