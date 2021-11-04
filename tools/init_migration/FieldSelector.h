#ifndef FIELDSELECTOR_H_INCLUDED
#define FIELDSELECTOR_H_INCLUDED

#include<string>
#include<map>
# include <mysql/mysql.h>

using namespace std;
class FieldSelector {
public:
    FieldSelector(MYSQL_RES *res):res_(res) {
        MYSQL_FIELD *field;
        for(uint16_t i = 0; (field = mysql_fetch_field(res)); i++) {
            name_map_[field->name] = i;
        }
    }

    int get_field_index(const string &name) {
        auto it =  name_map_.find(name);
        return it!=name_map_.end() ? it->second : -1;
    }
    int operator[](const string &name) {
        return get_field_index(name);
    }
private:
    const MYSQL_RES *res_;
    map<string,uint16_t> name_map_;
};


#endif // FIELDSELECTOR_H_INCLUDED
