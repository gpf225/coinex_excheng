/*
 * Description: 
 *     History: zhoumugui@viabtc, 2019/03/29, create
 */

# include "dm_user.h"
# include "ut_mysql.h"

static MYSQL *user_conn = NULL;
static uint32_t start_user_id = 0;  
static uint32_t end_user_id = 0;
static bool has_more = true;

int init_user(uint32_t start_uid, uint32_t last_uid)
{
    user_conn = mysql_connect(&settings.db_user);
    if (user_conn == NULL) {
        log_error("connect user database failed.");
        return -__LINE__;
    }
    start_user_id = start_uid;
    end_user_id = last_uid;
    return 0;
}

user_list_t *get_next_user_list(void)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `user_id` from user WHERE `user_id` > '%u' AND `user_id` <= '%u' ORDER BY `user_id` ASC LIMIT %u", start_user_id, end_user_id, MAX_USER_SIZE);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(user_conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(user_conn), mysql_error(user_conn));
        sdsfree(sql);
        return NULL;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(user_conn); 
    user_list_t *user_list = malloc(sizeof(user_list_t));
    memset(user_list, 0, sizeof(user_list_t));
    user_list->size = mysql_num_rows(result);
    if (user_list->size == 0) {
        log_info("no more users, start_user_id:%u", start_user_id);
        mysql_free_result(result);
        has_more = false;
        return user_list;
    }

    for (size_t i = 0; i < user_list->size; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        user_list->users[i] = strtoull(row[0], NULL, 0);
    }
    mysql_free_result(result);

    start_user_id = user_list->users[user_list->size - 1];
    log_info("fetch %u users, first user_id:%u start_user_id:%u", user_list->size, user_list->users[0], start_user_id);
    if (user_list->size != MAX_USER_SIZE) {
        has_more = false;
    }
    return user_list;
}

void user_list_free(user_list_t *obj)
{
    free(obj);
}

bool user_has_more(void)
{
    return has_more;
}