#ifndef ASSET_H_INCLUDED
#define ASSET_H_INCLUDED

#include "ut_decimal.h"

struct asset_type {
    int prec_save;
    int prec_show;
    mpd_t *min;
};

#endif // ASSET_H_INCLUDED
