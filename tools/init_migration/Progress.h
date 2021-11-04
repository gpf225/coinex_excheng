#ifndef PROGRESS_H_INCLUDED
#define PROGRESS_H_INCLUDED

#include <stdint.h>
#include "Reporter.h"

class Progress {
    uint64_t max_value_;
    uint16_t round_num_;
    uint64_t step_;
    uint16_t scale_ = 0;
    string tip_;
public:
    Progress(uint64_t max_value,const char *tip=nullptr,uint16_t round_num=10):max_value_(max_value),round_num_(round_num) {
        step_ = max_value/round_num_;
        tip_ = tip ? tip : "handle";
    }
    void step_it(uint64_t value) {
        if (value%step_==0) {
            scale_ = (value*100)/max_value_;
            printer_->add("%s %d%%(%lu/%lu)...",tip_.c_str(),scale_,value,max_value_);
        }
    }
    void step_end() {
        if (scale_<100)
            printer_->add("%s %d%%(%lu/%lu)",100,tip_.c_str(),max_value_,max_value_);
    }
    static Reporter *printer_;
};

#endif // PROGRESS_H_INCLUDED
