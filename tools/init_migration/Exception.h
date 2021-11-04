#ifndef EXCEPTION_H_INCLUDED
#define EXCEPTION_H_INCLUDED

 #include<stdexcept>
 #include<string>

 using namespace std;
class ValidDataException : public out_of_range {
public:
    ValidDataException(const string &err):out_of_range(err) {
    }
};


#endif // EXCEPTION_H_INCLUDED
