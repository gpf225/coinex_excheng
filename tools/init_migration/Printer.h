#ifndef PRINT_H_INCLUDED
#define PRINT_H_INCLUDED


#include <iostream>
#include <string>

using namespace std;

class Printer {
public:
    virtual void output(const string &info) {
        cout << info << endl;
    }
};


#endif // PRINT_H_INCLUDED
