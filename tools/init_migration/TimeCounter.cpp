
#include "TimeCounter.h"
#include "Utils.h"


void TimeCounter::start(const string &tip) {
    start_ = std::chrono::steady_clock::now();
    printer_->output(tip+"...");
}

void TimeCounter::watch(const string &tip) {
    std::chrono::duration<double> diff = std::chrono::steady_clock::now()-start_;
    string info = Utils::format_string("elapsed %.3f(ms)",diff.count()*1000);
    printer_->output(info);
    if (!tip.empty()) {
        start(tip);
    }
}
