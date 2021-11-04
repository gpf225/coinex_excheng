#ifndef REPORTER_H_INCLUDED
#define REPORTER_H_INCLUDED

#include<string>
#include<vector>
#include<memory>
#include "Printer.h"
using namespace std;


class Reporter : public Printer{
    struct Item {
        time_t ts_;
        string msg_;
    };
    vector<shared_ptr<Item>> items_;
public:
    void add(const string &msg);
    void add(const char *format,...);
    void output(const string &info) {
        add(info);
    }

    void output();
};

#endif // REPORTER_H_INCLUDED
