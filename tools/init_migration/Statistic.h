#ifndef STATISTICS_H_INCLUDED
#define STATISTICS_H_INCLUDED

struct Statistic {
    uint32_t total_ = 0;
    uint32_t success_ = 0;
    uint32_t fail_ = 0;

    uint32_t error_num_ = 0;
};


#endif // STATISTICS_H_INCLUDED
