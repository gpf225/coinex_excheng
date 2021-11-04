#ifndef TIMER_H_INCLUDED
#define TIMER_H_INCLUDED

#include <string>
#include <chrono>
#include <memory>
#include "Printer.h"

using namespace std;

class TimeCounter {
public:
    TimeCounter() {
        printer_ = make_shared<Printer>();
    }
    explicit TimeCounter(const shared_ptr<Printer> &printer):printer_(printer) {
    }

    void start(const string &tip);

    void watch(const string &tip="");

private:
    shared_ptr<Printer> printer_;
    std::chrono::steady_clock::time_point start_;
};


#endif // TIMER_H_INCLUDED
