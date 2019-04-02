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

static bool is_stop_migrate = false;
static uint32_t has_migrated = 0;
static uint32_t last_user_id = 0;

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
    log_info("user_id:%d migrate_end_time:%f settings.migirate_end_time:%f", user_id, migrate_end_time, settings.migirate_end_time);

    int ret = stop_migrate(user_id, migrate_start_time, settings.migirate_end_time);  // stop的end_time为配置的时间，表示迁移所有stop
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
            last_user_id = user_list->users[i];
            int ret = migrate_user(last_user_id);
            if (ret != 0) {
                error = true;
                break;
            }
        }
        user_list_free(user_list);
        if (error) {
            break; 
        }
       
        if (is_stop_migrate) {
            log_info("want to stop migration, last completed user_id:%u", last_user_id);
            error = false;
            break;
        }
        ++has_migrated;
    }
    
    log_info("stop migration thread, has_migrated:%u last_user_id:%d error: %s", has_migrated, last_user_id, error ? "error" : "none");
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
    is_stop_migrate = true;
}

sds migrate_status(sds reply)
{
    reply = sdscatprintf(reply, "has_migrated:%u\n", has_migrated);
    reply = sdscatprintf(reply, "last_user_id:%u\n", last_user_id);
    return reply;
}
