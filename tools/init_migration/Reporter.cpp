
#include "Reporter.h"
#include<time.h>
#include<algorithm>
#include "Utils.h"

extern "C" {
    #include "ut_log.h"
}
void Reporter::add(const string &msg) {
    Item *item = new Item();
    item->ts_ = time(nullptr);
    item->msg_ = msg;
    items_.push_back(shared_ptr<Item>(item));

    cout<<msg<<endl;
}

void Reporter::add(const char *format,...) {
    va_list args;
    va_start( args, format );
    add(Utils::format_string(format,args));
    va_end(args);
}

void Reporter::output() {
    for_each(items_.begin(),items_.end(),[&](shared_ptr<Item> &item) {
             struct tm  *tm = localtime(&item->ts_);
             const uint8_t BUF_SIZE = 30;
             char buffer[BUF_SIZE];
             strftime(buffer, BUF_SIZE, "%Y-%m-%d %H:%M:%S", tm);
             log_info("%s %s",buffer,item->msg_.c_str());
             });

    dlog_flush_all();
}
