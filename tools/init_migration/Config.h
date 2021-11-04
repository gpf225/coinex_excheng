#ifndef CONFIG_H_INCLUDED
#define CONFIG_H_INCLUDED

extern "C" {
# include "ut_config.h"
}

//#define _LITE_DATA_

#ifdef _LITE_DATA_
const uint8_t DB_HISTORY_COUNT = 1;
const uint8_t HISTORY_HASH_NUM = 1;
const uint16_t DEFAULT_ROW_LIMIT = 1;
const uint8_t SRC_SLICE_NUM = 10;
#else
const uint8_t DB_HISTORY_COUNT = 5;
const uint8_t HISTORY_HASH_NUM = 100;
const uint16_t DEFAULT_ROW_LIMIT = 1000;
const uint8_t SRC_SLICE_NUM = 10;
#endif

struct settings {
    log_cfg             log;

    mysql_cfg           db_src;
    mysql_cfg           db_log;
    mysql_cfg           db_summary;


    mysql_cfg           *db_histories;

    int                 row_limit = DEFAULT_ROW_LIMIT;
    int                 mode = 7; /// bit0-transfer data bit1-clean data bit2-kline

    redis_cfg           redis;
    int                 pipeline_len_max;
    int                 min_max;
    int                 hour_max;
    int                 deal_summary_max;
};

extern struct settings settings;
int init_config(const char *path);

# undef ERR_RET_LN
# define ERR_RET_LN(x) do { \
    if ((x) < 0) { \
        return -__LINE__; \
    } \
} while (0)

# undef ERR_RET
# define ERR_RET(x) do { \
    int __ret = (x); \
    if (__ret < 0) { \
        return __ret; \
    } \
} while (0)

#endif // CONFIG_H_INCLUDED
