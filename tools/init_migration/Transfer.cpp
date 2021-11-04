
#include "Transfer.h"
#include "Utils.h"

int Transfer::run() {
    ctx_.time_counter_->start(Utils::format_string("%s init",name_.c_str()));
    if (init())
        return -1;

    ctx_.time_counter_->watch(Utils::format_string("%s handle",name_.c_str()));
    int ret = handle();
    ctx_.time_counter_->watch();

    report();
    return ret;
}


void Transfer::report() {
    ctx_.reporter_->add(Utils::format_string("%s statistic:",name_.c_str()));
    ctx_.reporter_->add(Utils::format_string("- total number:%d",stat_->total_));
    ctx_.reporter_->add(Utils::format_string("- success number:%d",stat_->success_));
    ctx_.reporter_->add(Utils::format_string("- fail number:%d",stat_->fail_));
    ctx_.reporter_->add(Utils::format_string("- error number:%d",stat_->error_num_));
}
