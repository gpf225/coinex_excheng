#ifndef UTILS_H_INCLUDED
#define UTILS_H_INCLUDED

#include <stdarg.h>
#include <string>
#include <vector>
using namespace std;

extern "C" {
#include "ut_decimal.h"
#include "ut_mysql.h"
}
class CLargeStringArray;

class SQLExecutor {
public:
    SQLExecutor(MYSQL *conn):conn_(conn) {
    }

    int exec(const string&sql) {
        int ret = mysql_real_query(conn_, sql.c_str(), sql.length());
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(conn_), mysql_error(conn_));
            return -1;
        }
        return 0;
    }
private:
    MYSQL *conn_;
};


class Utils {
public:
    static string format_string(const char *format,...);
    static string format_string(const char *format,va_list args);
    static int batch_insert(SQLExecutor *executor,const char* table_name, const char* field_list, CLargeStringArray& value_vec,int exec_count) ;

    static string mpd_to_string(mpd_t *value);


    static char* dup_string(const string &s);
    static int FormatString(char **ppbuf,const char *format,...);
    static void FormatString(string &s,const char *format,va_list args);
    static int FormatString(char **ppbuf,const char *format,va_list args);

    static int AppendString(char **ppbuf,char *s);
    static int StringCatenate(char **ppbuf,const char *format,...);
};

void error(int exit_code, int err_code, const char *fmt,...);

class CLargeStringArray {
	vector<char*> val_vec_;

	bool auto_delete_;
public:
	CLargeStringArray(bool auto_delete=true):auto_delete_(auto_delete) {
	}
	~CLargeStringArray() {
		if (auto_delete_) {
			vector<char*>::iterator iter = val_vec_.begin();
			while(iter!=val_vec_.end()) {
				delete []*iter;
				iter++;
			}
		}
	}
	void SetAutoDelete(bool value) { auto_delete_ = value;}
	int Add(char *s) {
		val_vec_.push_back(s);
		return 0;
	}
	unsigned int GetCount() const {
		return (int)val_vec_.size();
	}
	char* Get(unsigned int index) {
		return val_vec_[index];
	}
	char* operator[](unsigned int index) {
		return val_vec_[index];
	}
};

#endif // UTILS_H_INCLUDED
