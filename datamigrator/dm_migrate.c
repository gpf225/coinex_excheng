/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/04/01, create
 */

# include "dm_migrate.h"
# include "dm_user.h"
# include "dm_stop.h"
# include "dm_order.h"
# include "dm_deal.h"
# include "dm_balance.h"
# include "dm_dbpool.h"

static volatile bool is_migrate_cancel = 0;
static uint32_t has_migrated = 0;
static uint32_t last_user_id = 0;

static int migrate_data(uint32_t user_id, double migrate_start_time, double migrate_end_time, double stop_migrate_end_time)
{
    int ret = stop_migrate(user_id, migrate_start_time, stop_migrate_end_time);  
    if (ret != 0) {
        log_error("user_id:%u migrate stop failed, ret:%d", user_id, ret);
        return ret;
    }

    ret = order_migrate(user_id, migrate_start_time, migrate_end_time);
    if (ret != 0) {
        log_error("user_id:%u migrate order failed, ret:%d", user_id, ret);
        return ret;
    }

    ret = deal_migrate(user_id, migrate_start_time, migrate_end_time);
    if (ret != 0) {
        log_error("user_id:%u migrate deal failed, ret:%d", user_id, ret);
        return ret;
    }

    ret = balance_migrate(user_id, migrate_start_time, migrate_end_time);
    if (ret != 0) {
        log_error("user_id:%u migrate balance failed, ret:%d", user_id, ret);
        return ret;
    }

    return ret;
}

static int clear_stop(uint32_t user_id, double migrate_start_time, double migrate_end_time)
{
    MYSQL *conn = get_new_db_connection(user_id);
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DELETE from stop_history_%u WHERE `user_id`='%u' AND `finish_time` <= '%f' AND `finish_time` > '%f'",
            user_id % HISTORY_HASH_NUM, user_id, migrate_start_time, migrate_end_time);
    
    log_trace("sql:%s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);
    return 0;
}

static int clear_order(uint32_t user_id, double migrate_start_time, double migrate_end_time)
{
    MYSQL *conn = get_new_db_connection(user_id);
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DELETE from order_history_%u WHERE `user_id`='%u' AND `finish_time` <= '%f' AND `finish_time` > '%f'",
            user_id % HISTORY_HASH_NUM, user_id, migrate_start_time, migrate_end_time);
    
    log_trace("sql:%s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);
    return 0;
}

static int clear_deal(uint32_t user_id, double migrate_start_time, double migrate_end_time)
{
    MYSQL *conn = get_new_db_connection(user_id);
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DELETE from user_deal_history_%u WHERE `user_id`='%u' AND `time` <= '%f' AND `time` > '%f'",
            user_id % HISTORY_HASH_NUM, user_id, migrate_start_time, migrate_end_time);
    
    log_trace("sql:%s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);
    return 0;
}
static int clear_balance(uint32_t user_id, double migrate_start_time, double migrate_end_time)
{
    MYSQL *conn = get_new_db_connection(user_id);
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DELETE from balance_history_%u WHERE `user_id`='%u' AND `time` <= '%f' AND `time` > '%f'",
            user_id % HISTORY_HASH_NUM, user_id, migrate_start_time, migrate_end_time);
    
    log_trace("sql:%s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);
    return 0;
}

static int migrate_user(uint32_t user_id)
{
    const double migrate_start_time = settings.migirate_start_time;
    double migrate_end_time = settings.migirate_end_time;
    if (settings.migrate_mode == MIGRATE_MODE_FULL) {
        migrate_end_time = order_get_end_time(user_id, migrate_start_time, settings.least_day_per_user, settings.max_order_per_user);
        if (migrate_end_time < 0.9) {
            log_error("could not get user_id:%d migration end time", user_id);
            return -__LINE__;
        }
    }
    log_info("user_id:%u migrate_start_time:%f migrate_end_time:%f settings.migirate_end_time:%f", user_id, migrate_start_time, migrate_end_time, settings.migirate_end_time);
    
    int ret = migrate_data(user_id, migrate_start_time, migrate_end_time, settings.migirate_end_time);
    if (ret == 0) {
        log_info("user_id:%u migration finished.", user_id);
        return 0;
    }

    if (ret != -1) {
        return ret;
    }

    log_info("going to clear user:%d history, and try migration again", user_id);
    ret = clear_stop(user_id, migrate_start_time, settings.migirate_end_time);
    if (ret != 0) {
        return ret;
    }
    
    ret = clear_order(user_id, migrate_start_time, migrate_end_time);
    if (ret != 0) {
        return ret;
    }
    ret = clear_deal(user_id, migrate_start_time, migrate_end_time);
    if (ret != 0) {
        return ret;
    }
    ret = clear_balance(user_id, migrate_start_time, migrate_end_time);
    if (ret != 0) {
        return ret;
    }

    ret = migrate_data(user_id, migrate_start_time, migrate_end_time, settings.migirate_end_time);
    if (ret != 0) {
        log_error("user_id:%u migration failed, ret:%d", user_id, ret);
        return ret;
    }
    return 0;
}

static void *thread_routine(void *data)
{
    bool error = false;
    log_info("start migration thread");
    while (true) {
        user_list_t *user_list = get_next_user_list();
            if (user_list == NULL) {
            log_error("could not get user list");
            error = true;
            break;
        }

        if (user_list->size == 0) {
            log_info("no more users, migration completed.");
            user_list_free(user_list);
            error = false;
            break;
        }
        
        for (uint32_t i = 0; i < user_list->size; ++i) {
            if (is_migrate_cancel) {
                log_info("want to stop migration, last completed user_id:%u", last_user_id);
                break;
            }
            last_user_id = user_list->users[i];
            int ret = migrate_user(last_user_id);
            if (ret != 0) {
                error = true;
                break;
            }
            ++has_migrated;
        }
        user_list_free(user_list);
        if (error) {
            break; 
        }
        
        log_trace("is_migrate_cancel:%s", is_migrate_cancel ? "true" : "false");
        if (is_migrate_cancel) {
            log_info("want to stop migration, last completed user_id:%u", last_user_id);
            error = false;
            break;
        }
    }
    
    log_info("stop migration thread, has_migrated:%u last_user_id:%d error: %s", has_migrated, last_user_id, error ? "error" : "none");
    signal_exit = true;
    return NULL;
}

static int start_migration(void)
{
    pthread_t tid = 0;
    if (pthread_create(&tid, NULL, thread_routine, NULL) != 0) {
        log_error("start migration thread failed.");
        return -__LINE__;
    }
    return 0;
}

int start_migrate(void)
{
    int ret = start_migration();
    if (ret != 0) {
        return ret;
    }
    
    return 0;
}

void migrate_cancel(void)
{
    is_migrate_cancel = true;
    log_trace("is_migrate_cancel:%s", is_migrate_cancel ? "true" : "false");
}

sds migrate_status(sds reply)
{
    reply = sdscatprintf(reply, "has_migrated:%u\n", has_migrated);
    reply = sdscatprintf(reply, "last_user_id:%u\n", last_user_id);
    return reply;
}
