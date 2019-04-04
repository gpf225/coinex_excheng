/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# include "dm_dbpool.h"

static MYSQL *old_conn = NULL;

static MYSQL *new_conn0 = NULL;
static MYSQL *new_conn1 = NULL;
static MYSQL *new_conn2 = NULL;
static MYSQL *new_conn3 = NULL;
static MYSQL *new_conn4 = NULL;

int init_dbpool(void)
{
    old_conn = mysql_connect(&settings.db_history);
    if (old_conn == NULL) {
        return -__LINE__;
    }

    new_conn0 = mysql_connect(&settings.db_histories.configs[0]);
    if (new_conn0 == NULL) {
        return -__LINE__;
    }

    new_conn1 = mysql_connect(&settings.db_histories.configs[1]);
    if (new_conn1 == NULL) {
        return -__LINE__;
    }

    new_conn2 = mysql_connect(&settings.db_histories.configs[2]);
    if (new_conn2 == NULL) {
        return -__LINE__;
    }

    new_conn3 = mysql_connect(&settings.db_histories.configs[3]);
    if (new_conn3 == NULL) {
        return -__LINE__;
    }

    new_conn4 = mysql_connect(&settings.db_histories.configs[4]);
    if (new_conn4 == NULL) {
        return -__LINE__;
    }

    return 0;
}

MYSQL* get_old_db_connection(void)
{
    return old_conn;
}

MYSQL* get_new_db_connection(uint32_t user_id)
{
    int hash = (user_id % (settings.db_histories.count * HISTORY_HASH_NUM)) / HISTORY_HASH_NUM;
    switch (hash) {
        case 0:
            return new_conn0;
        case 1:
            return new_conn1;
        case 2:
            return new_conn2;
        case 3:
            return new_conn3;
        case 4:
            return new_conn4;
        default:
            return NULL;
    }
    return NULL;
}