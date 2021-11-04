#ifndef WRITER_H_INCLUDED
#define WRITER_H_INCLUDED


#include "Utils.h"
#include "Context.h"
#include "Order.h"
#include "Deal.h"
#include "Account.h"

struct FieldTypeFormat {
    const char* name_;
    const char* fmt_;
};


class Writer {
public:
    int batch_insert(uint8_t db_id,uint16_t slice_id,CLargeStringArray& value_vec,int exec_count) {
        string table_name = Utils::format_string("%s_%d",table_tag_.c_str(),slice_id);
        SQLExecutor executor(ctx_.history_db_conns_[db_id]);
        return Utils::batch_insert(&executor,table_name.c_str(),field_list_.c_str(),value_vec,exec_count);
    }
    const Context &ctx_;
protected:
    Writer(const Context &ctx):ctx_(ctx) {
    }
protected:
    FieldTypeFormat *fields_;
    uint8_t field_num_;

    string field_list_;
    const string delimiter_ = ",";

    string table_tag_;
    const string generate_field_list() {
        string s;
        for (int i=0;i<field_num_;i++) {
            s = s+"`"+fields_[i].name_+"`"+delimiter_;
        }
        s.erase(s.length()-delimiter_.length(),delimiter_.length());
        return s;
    }

    const string generate_value_format() {
        string s;
        for (int i=0;i<field_num_;i++) {
            s = s+fields_[i].fmt_+delimiter_;
        }
        s.erase(s.length()-delimiter_.length(),delimiter_.length());
        return s;
    }
};


#endif // WRITER_H_INCLUDED
