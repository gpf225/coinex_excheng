/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "dr_config.h"
# include "dr_deal.h"
# include "dr_history.h"

#define TYPE_BID_VOLUME             1
#define TYPE_ASK_VOLUME             2
#define TYPE_BID_DEAL               3
#define TYPE_ASK_DEAL               4
#define TYPE_TAKER_BID_VOLUME       5
#define TYPE_TAKER_ASK_VOLUME       6
#define TYPE_TAKER_BID_TRADE_NUM    7
#define TYPE_TAKER_ASK_TRADE_NUM    8
#define TYPE_TAKER_TOTAL_TRADE_NUM  9

static dict_t *dict_market;
static dict_t *dict_fee;

struct dict_market_key {
    char market[MARKET_NAME_MAX_LEN];
    char stock[STOCK_NAME_MAX_LEN];
};

struct dict_market_val {
    dict_t     *dict_minute;
    skiplist_t *minute_list;
};

struct dict_minute_val {
    dict_t *dict_user;
};

struct dict_user_key {
    uint32_t user_id;
};

struct dict_user_val {
    char     *volume_bid;
    char     *volume_ask;
    char     *deal_bid;
    char     *deal_ask;
    char     *volume_taker_bid;
    char     *volume_taker_ask;
    uint32_t trade_num_taker_ask;
    uint32_t trade_num_taker_bid;
    uint32_t trade_num_total; 
};

struct dict_fee_val {
    char        *fee;
};

struct list_sort_integer_val {
    uint64_t   num;
    uint32_t   user_id;
};

struct list_sort_mpd_val {
    mpd_t     *num;
    uint32_t  user_id;
};

// dict_market
static uint32_t dict_market_hash_function(const void *key)
{
    const struct dict_market_key *obj = key;
    return dict_generic_hash_function(obj->market, strlen(obj->market));
}

static int dict_market_key_compare(const void *key1, const void *key2)
{
    const struct dict_market_key *obj1 = key1;
    const struct dict_market_key *obj2 = key2;

    return strcmp(obj1->market, obj2->market);
}

static void *dict_market_key_dup(const void *key)
{
    struct dict_market_key *obj = malloc(sizeof(struct dict_market_key));
    memcpy(obj, key, sizeof(struct dict_market_key));
    return obj;
}

static void dict_market_key_free(void *key)
{
    free(key);
}

static void *dict_market_val_dup(const void *val)
{
    struct dict_market_val *obj = malloc(sizeof(struct dict_market_val));
    memcpy(obj, val, sizeof(struct dict_market_val));
    return obj;
}

static void dict_market_val_free(void *val)
{
    struct dict_market_val *obj = val;
    if (obj->dict_minute != NULL)
        dict_release(obj->dict_minute);
    if (obj->minute_list != NULL)
        skiplist_release(obj->minute_list);
    free(obj);
}

// dict_minute
static uint32_t dict_minute_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(time_t));
}

static int dict_minute_key_compare(const void *key1, const void *key2)
{
    const time_t *obj1 = key1;
    const time_t *obj2 = key2;

    if (*obj1 == *obj2)
        return 0;

    return *obj1 > *obj2 ? 1 : -1;
}

static void *dict_minute_key_dup(const void *key)
{
    time_t *obj = malloc(sizeof(time_t));
    memcpy(obj, key, sizeof(time_t));
    return obj;
}

static void dict_minute_key_free(void *key)
{
    free(key);
}

static void *dict_minute_val_dup(const void *val)
{
    struct dict_minute_val *obj = malloc(sizeof(struct dict_minute_val));
    memcpy(obj, val, sizeof(struct dict_minute_val));
    return obj;
}

static void dict_minute_val_free(void *val)
{
    struct dict_minute_val *obj = val;
    if (obj->dict_user != NULL)
        dict_release(obj->dict_user);
    free(obj);
}

// dict_user
static uint32_t dict_user_hash_function(const void *key)
{
    const uint32_t *obj = key;
    return *obj;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    const uint32_t *obj1 = key1;
    const uint32_t *obj2 = key2;

    if (*obj1 == *obj2) {
        return 0;
    }

    return 1;
}

static void *dict_user_key_dup(const void *key)
{
    uint32_t *obj = malloc(sizeof(uint32_t));
    memcpy(obj, key, sizeof(uint32_t));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void *dict_user_val_dup(const void *val)
{
    struct dict_user_val *obj = malloc(sizeof(struct dict_user_val));
    memcpy(obj, val, sizeof(struct dict_user_val));
    return obj;
}

static void dict_user_val_free(void *val)
{
    struct dict_user_val *obj = val;
    if (obj->volume_bid)
        free(obj->volume_bid);
    if (obj->volume_ask)
        free(obj->volume_ask);
    if (obj->deal_bid)
        free(obj->deal_bid);
    if (obj->deal_ask)
        free(obj->deal_ask);
    if (obj->volume_taker_bid)
        free(obj->volume_taker_bid);
    if (obj->volume_taker_ask)
        free(obj->volume_taker_ask);

    free(obj);
}

// dict fee
static uint32_t dict_fee_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_fee_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *dict_fee_key_dup(const void *key)
{
    return strdup(key);
}

static void *dict_fee_val_dup(const void *val)
{
    return strdup(val);
}

static void dict_fee_key_free(void *key)
{
    free(key);
}

static void dict_fee_val_free(void *val)
{
    free(val);
}

static void on_list_free(void *val)
{
    free(val);
}

static int minute_skiplist_compare(const void *value1, const void *value2)
{
    const time_t *time1 = value1;
    const time_t *time2 = value2;

    return *time1 - *time2;
}

static void *minute_skiplist_dup(void *val)
{
    time_t *obj = malloc(sizeof(time_t));
    *obj = *((time_t*)val);

    return obj;
}

static void minute_skiplist_free(void *val)
{
    free(val);
}

static int sort_integer_list_compare(const void *value1, const void *value2)
{
    const struct list_sort_integer_val *val1 = value1;
    const struct list_sort_integer_val *val2 = value2;

    if (val2->num != val1->num) {
        return val2->num - val1->num;
    } else {
        return val2->user_id - val1->user_id;
    }
}

static void sort_integer_list_free(void *obj)
{
    free(obj);
}

static int sort_mpd_list_compare(const void *value1, const void *value2)
{
    const struct list_sort_mpd_val *val1 = value1;
    const struct list_sort_mpd_val *val2 = value2;

    int ret = mpd_cmp(val2->num, val1->num, &mpd_ctx);

    if (ret != 0) {
        return ret;
    } else {
        return val2->user_id - val1->user_id;
    }
}

static void sort_mpd_list_free(void *value)
{
    struct list_sort_mpd_val *obj = value;
    if (obj->num)
        mpd_del(obj->num);
    free(obj);
}

//dict_market_val.minute_list
static skiplist_t *create_minute_skiplist()
{
    skiplist_type type;
    memset(&type, 0, sizeof(type));
    type.compare = minute_skiplist_compare;
    type.dup     = minute_skiplist_dup;
    type.free    = minute_skiplist_free;

    return skiplist_create(&type);
}

//integer skiplist
static skiplist_t *create_sort_integer_list()
{
    skiplist_type type;
    memset(&type, 0, sizeof(type));
    type.compare = sort_integer_list_compare;
    type.free    = sort_integer_list_free;

    return skiplist_create(&type);
}

//mpd skiplist
static skiplist_t *create_sort_mpd_list()
{
    skiplist_type type;
    memset(&type, 0, sizeof(type));
    type.compare = sort_mpd_list_compare;
    type.free = sort_mpd_list_free;

    return skiplist_create(&type);
}

static dict_t *create_minute_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function   = dict_minute_hash_function;
    type.key_compare     = dict_minute_key_compare;
    type.key_dup         = dict_minute_key_dup;
    type.val_dup         = dict_minute_val_dup;
    type.key_destructor  = dict_minute_key_free;
    type.val_destructor  = dict_minute_val_free;

    return dict_create(&type, 64);     
}

static dict_t *create_user_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = dict_user_hash_function;
    type.key_compare    = dict_user_key_compare;
    type.key_dup        = dict_user_key_dup;
    type.val_dup        = dict_user_val_dup;
    type.key_destructor = dict_user_key_free;
    type.val_destructor = dict_user_val_free;

    return dict_create(&type, 64);     
}

static dict_t *create_fee_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = dict_fee_hash_function;
    type.key_compare    = dict_fee_key_compare;
    type.key_dup        = dict_fee_key_dup;
    type.val_dup        = dict_fee_val_dup;
    type.key_destructor = dict_fee_key_free;
    type.val_destructor = dict_fee_val_free;

    return dict_create(&type, 4);
}

static void free_user_val(struct dict_user_val *obj)
{
    if (obj->volume_bid)
        free(obj->volume_bid);
    if (obj->volume_ask)
        free(obj->volume_ask);
    if (obj->deal_bid)
        free(obj->deal_bid);
    if (obj->deal_ask)
        free(obj->deal_ask);
    if (obj->volume_taker_bid)
        free(obj->volume_taker_bid);
    if (obj->volume_taker_ask)
        free(obj->volume_taker_ask);
}

static int process_new_minute(bool is_taker, dict_t *dict_minute, time_t minute, uint32_t user_id, int side, 
        const char *market, const char *volume_str, const char *deal_str, const char *fee_asset, const char *fee_str)
{
    dict_t *dict_user = create_user_dict();
    if (dict_user == NULL) {
        log_fatal("create_user_dict fail");
        return -__LINE__;
    }

    struct dict_user_val user_val;
    memset(&user_val, 0, sizeof(user_val));
    user_val.trade_num_total++;

    if (side == MARKET_TRADE_SIDE_SELL) {
        user_val.volume_ask = strdup(volume_str);
        user_val.deal_ask   = strdup(deal_str);
        user_val.volume_bid = strdup("0");
        user_val.deal_bid   = strdup("0");

        if (is_taker) {
            user_val.trade_num_taker_ask++;
            user_val.volume_taker_ask = strdup(volume_str);  
            user_val.volume_taker_bid = strdup("0"); 
        } else {
            user_val.volume_taker_ask = strdup("0");
            user_val.volume_taker_bid = strdup("0");
        }
    } else {
        user_val.volume_bid = strdup(volume_str);
        user_val.deal_bid   = strdup(deal_str);
        user_val.volume_ask = strdup("0");
        user_val.deal_ask   = strdup("0");
        if (is_taker) {
            user_val.trade_num_taker_bid++;
            user_val.volume_taker_bid = strdup(volume_str); 
            user_val.volume_taker_ask = strdup("0");     
        } else {
            user_val.volume_taker_ask = strdup("0");
            user_val.volume_taker_bid = strdup("0"); 
        }
    }

    dict_entry *entry = dict_add(dict_user, &user_id, &user_val);
    if (entry == NULL) {
        log_fatal("dict_add fail");
        dict_release(dict_user);
        free_user_val(&user_val);
        return -__LINE__;
    }

    //add dict_user to dict_minute
    struct dict_minute_val minute_val;
    minute_val.dict_user = dict_user;

    entry = dict_add(dict_minute, &minute, &minute_val);
    if (entry == NULL) {
        log_fatal("dict_add fail");
        dict_release(dict_user);
        return -__LINE__;
    }

    return 0;
}

static int process_new_market(bool is_taker, struct dict_market_key *p_market_key, time_t minute, uint32_t user_id, int type, 
        const char *market, const char *volume_str, const char *deal_str, const char *fee_asset, const char *fee)
{
    dict_t *dict_minute = create_minute_dict();
    if (dict_minute == NULL) {
        log_fatal("create_minute_dict fail");
        return -__LINE__;
    }

    int ret = process_new_minute(is_taker, dict_minute, minute, user_id, type, market, volume_str, deal_str, fee_asset, fee);
    if (ret != 0) {
        dict_release(dict_minute);
        return -__LINE__;
    }

    // add dict_minute to dict_market
    struct dict_market_val market_val;
    memset(&market_val, 0, sizeof(market_val));
    market_val.dict_minute = dict_minute;
    market_val.minute_list = create_minute_skiplist();

    if (skiplist_insert(market_val.minute_list, &minute) == NULL) {
        log_fatal("dict_add fail");
        dict_release(dict_minute);
        return -__LINE__;
    }

    if (dict_add(dict_market, p_market_key, &market_val) == NULL) {
        log_fatal("dict_add fail");
        dict_release(dict_minute);
        return -__LINE__;
    }

    return 0;
}

static void add_fee(const char *fee_asset, mpd_t *fee, const char *fee_str)
{
    dict_entry *entry = dict_find(dict_fee, fee_asset);
    if (entry != NULL) {
        char *fee_old_str = entry->val;
        mpd_t *fee_old = decimal(fee_old_str, 0);

        mpd_t *fee_total = mpd_qncopy(mpd_zero);
        mpd_add(fee_total, fee_old, fee, &mpd_ctx);
        char *fee_total_str = mpd_to_sci(fee_total, 0);
        free(entry->val);
        entry->val = fee_total_str;

        mpd_del(fee_old);
        mpd_del(fee_total);

    } else {
        dict_add(dict_fee, (char *)fee_asset, (char *)fee_str);
    }
}

static int update_user_val(bool is_taker, dict_t *dict_user, uint32_t user_id, int side, const char *fee_asset, 
        mpd_t *volume, mpd_t *deal, mpd_t *fee, const char *volume_str, const char *deal_str, const char *fee_str)
{
    bool is_new_user = false;
    struct dict_user_val user_val;
    memset(&user_val, 0, sizeof(user_val));
    struct dict_user_val *p_user_val = &user_val;

    dict_entry *entry = dict_find(dict_user, &user_id);
    if (entry != NULL) { // find user
        p_user_val = entry->val;
    } else { // new user
        is_new_user = true;
    }

    p_user_val->trade_num_total++;

    if (is_new_user) {
        if (side == MARKET_TRADE_SIDE_SELL) {
            p_user_val->volume_ask = strdup(volume_str);
            p_user_val->deal_ask   = strdup(deal_str);
            p_user_val->volume_bid = strdup("0");
            p_user_val->deal_bid   = strdup("0");

            if (is_taker) {
                p_user_val->trade_num_taker_ask++;
                p_user_val->volume_taker_ask = strdup(volume_str);
                p_user_val->volume_taker_bid = strdup("0");
            } else {
                p_user_val->volume_taker_ask = strdup("0");
                p_user_val->volume_taker_bid = strdup("0");
            }
        } else {
            p_user_val->volume_bid = strdup(volume_str);
            p_user_val->deal_bid   = strdup(deal_str);
            p_user_val->volume_ask = strdup("0");
            p_user_val->deal_ask   = strdup("0");
            if (is_taker) {
                p_user_val->trade_num_taker_bid++;
                p_user_val->volume_taker_bid = strdup(volume_str);
                p_user_val->volume_taker_ask = strdup("0");
            } else {
                p_user_val->volume_taker_ask = strdup("0");
                p_user_val->volume_taker_bid = strdup("0");
            }
        }

        if (dict_add(dict_user, &user_id, p_user_val) == NULL) {
            log_fatal("dict_add fail");
            return -__LINE__;
        }
    } else {
        if (side == MARKET_TRADE_SIDE_SELL) {
            mpd_t *ask_volume_old   = decimal(p_user_val->volume_ask, 0);
            mpd_t *ask_volume_total = mpd_qncopy(mpd_zero);
            mpd_add(ask_volume_total, ask_volume_old, volume, &mpd_ctx);
            free(p_user_val->volume_ask);
            p_user_val->volume_ask = mpd_to_sci(ask_volume_total, 0);

            mpd_del(ask_volume_old);
            mpd_del(ask_volume_total);

            mpd_t *ask_deal_old   = decimal(p_user_val->deal_ask, 0);
            mpd_t *ask_deal_total = mpd_qncopy(mpd_zero);
            mpd_add(ask_deal_total, ask_deal_old, deal, &mpd_ctx);
            free(p_user_val->deal_ask);
            p_user_val->deal_ask  = mpd_to_sci(ask_deal_total, 0);

            mpd_del(ask_deal_old);
            mpd_del(ask_deal_total);

            if (is_taker) {
                p_user_val->trade_num_taker_ask++;
                mpd_t *taker_ask_volume_old   = decimal(p_user_val->volume_taker_ask, 0);
                mpd_t *taker_ask_volume_total = mpd_qncopy(mpd_zero);
                mpd_add(taker_ask_volume_total, taker_ask_volume_old, volume, &mpd_ctx);
                free(p_user_val->volume_taker_ask);
                p_user_val->volume_taker_ask = mpd_to_sci(taker_ask_volume_total, 0);

                mpd_del(taker_ask_volume_old);
                mpd_del(taker_ask_volume_total);
            }
        } else {
            mpd_t *bid_volume_old   = decimal(p_user_val->volume_bid, 0);
            mpd_t *bid_volume_total = mpd_qncopy(mpd_zero);
            mpd_add(bid_volume_total, bid_volume_old, volume, &mpd_ctx);
            free(p_user_val->volume_bid);
            p_user_val->volume_bid  = mpd_to_sci(bid_volume_total, 0);

            mpd_del(bid_volume_old);
            mpd_del(bid_volume_total);

            mpd_t *bid_deal_old   = decimal(p_user_val->deal_bid, 0);
            mpd_t *bid_deal_total = mpd_qncopy(mpd_zero);
            mpd_add(bid_deal_total, bid_deal_old, deal, &mpd_ctx);
            free(p_user_val->deal_bid);
            p_user_val->deal_bid  = mpd_to_sci(bid_deal_total, 0);

            mpd_del(bid_deal_old);
            mpd_del(bid_deal_total);

            if (is_taker) {
                p_user_val->trade_num_taker_bid++;
                mpd_t *taker_bid_volume_old   = decimal(p_user_val->volume_taker_bid, 0);
                mpd_t *taker_bid_volume_total = mpd_qncopy(mpd_zero);
                mpd_add(taker_bid_volume_total, taker_bid_volume_old, volume, &mpd_ctx);
                free(p_user_val->volume_taker_bid);
                p_user_val->volume_taker_bid = mpd_to_sci(taker_bid_volume_total, 0);

                mpd_del(taker_bid_volume_old);
                mpd_del(taker_bid_volume_total);
            }
        }
    }

    return 0;
}

static int count_user_val(dict_t *save_dict_user, uint32_t user_id, struct dict_user_val *in_user_val, int data_type)
{
    bool is_new_user = false;
    struct dict_user_val user_val;
    memset(&user_val, 0, sizeof(user_val));
    struct dict_user_val *p_user_val = &user_val;

    dict_entry *entry = dict_find(save_dict_user, &user_id);
    if (entry != NULL) { // find user
        p_user_val = entry->val;
    } else { // new user
        is_new_user = true;
    }

    if (is_new_user) { // new user
        if (data_type == 0) {
            p_user_val->volume_bid = strdup(in_user_val->volume_bid);
            p_user_val->volume_ask = strdup(in_user_val->volume_ask);
            p_user_val->deal_bid = strdup(in_user_val->deal_bid);
            p_user_val->deal_ask = strdup(in_user_val->deal_ask);
            p_user_val->volume_taker_bid = strdup(in_user_val->volume_taker_bid);
            p_user_val->volume_taker_ask = strdup(in_user_val->volume_taker_ask);
            p_user_val->trade_num_taker_bid = in_user_val->trade_num_taker_bid;
            p_user_val->trade_num_taker_ask = in_user_val->trade_num_taker_ask;
            p_user_val->trade_num_total = in_user_val->trade_num_total;
        } else if (data_type == TYPE_BID_VOLUME){
            p_user_val->volume_bid = strdup(in_user_val->volume_bid);  
        } else if (data_type == TYPE_ASK_VOLUME){
            p_user_val->volume_ask = strdup(in_user_val->volume_ask);   
        } else if (data_type == TYPE_BID_DEAL){
            p_user_val->deal_bid = strdup(in_user_val->deal_bid);   
        } else if (data_type == TYPE_ASK_DEAL){
            p_user_val->deal_ask = strdup(in_user_val->deal_ask);   
        } else if (data_type == TYPE_TAKER_BID_VOLUME){
            p_user_val->volume_taker_bid = strdup(in_user_val->volume_taker_bid);   
        } else if (data_type == TYPE_TAKER_ASK_VOLUME){
            p_user_val->volume_taker_ask = strdup(in_user_val->volume_taker_ask);   
        } else if (data_type == TYPE_TAKER_BID_TRADE_NUM){
            p_user_val->trade_num_taker_bid = in_user_val->trade_num_taker_bid;   
        } else if (data_type == TYPE_TAKER_ASK_TRADE_NUM){
            p_user_val->trade_num_taker_ask = in_user_val->trade_num_taker_ask;   
        } else if (data_type == TYPE_TAKER_TOTAL_TRADE_NUM){
            p_user_val->trade_num_total = in_user_val->trade_num_total;   
        }

        if (dict_add(save_dict_user, &user_id, p_user_val) == NULL) {
            log_fatal("dict_add fail");
            free_user_val(p_user_val);
            return -__LINE__;
        }
    } else { // old user
        if (data_type == 0) {
            mpd_t *volume_bid_old = decimal(p_user_val->volume_bid, 0);
            mpd_t *volume_bid = decimal(in_user_val->volume_bid, 0);
            mpd_t *volume_bid_total = mpd_qncopy(mpd_zero);
            mpd_add(volume_bid_total, volume_bid_old, volume_bid, &mpd_ctx);
            free(p_user_val->volume_bid);
            p_user_val->volume_bid = mpd_to_sci(volume_bid_total, 0);
            mpd_del(volume_bid_total);
            mpd_del(volume_bid_old);
            mpd_del(volume_bid);

            mpd_t *volume_ask_old = decimal(p_user_val->volume_ask, 0);
            mpd_t *volume_ask = decimal(in_user_val->volume_ask, 0);
            mpd_t *volume_ask_total = mpd_qncopy(mpd_zero);
            mpd_add(volume_ask_total, volume_ask_old, volume_ask, &mpd_ctx);
            free(p_user_val->volume_ask);
            p_user_val->volume_ask = mpd_to_sci(volume_ask_total, 0);
            mpd_del(volume_ask_total);
            mpd_del(volume_ask_old);
            mpd_del(volume_ask);

            mpd_t *deal_bid_old = decimal(p_user_val->deal_bid, 0);
            mpd_t *deal_bid = decimal(in_user_val->deal_bid, 0);
            mpd_t *deal_bid_total = mpd_qncopy(mpd_zero);
            mpd_add(deal_bid_total, deal_bid_old, deal_bid, &mpd_ctx);
            free(p_user_val->deal_bid);
            p_user_val->deal_bid = mpd_to_sci(deal_bid_total, 0);
            mpd_del(deal_bid_total);
            mpd_del(deal_bid_old);
            mpd_del(deal_bid);

            mpd_t *deal_ask_old = decimal(p_user_val->deal_ask, 0);
            mpd_t *deal_ask = decimal(in_user_val->deal_ask, 0);
            mpd_t *deal_ask_total = mpd_qncopy(mpd_zero);
            mpd_add(deal_ask_total, deal_ask_old, deal_ask, &mpd_ctx);
            free(p_user_val->deal_ask);
            p_user_val->deal_ask = mpd_to_sci(deal_ask_total, 0);
            mpd_del(deal_ask_total);
            mpd_del(deal_ask_old);
            mpd_del(deal_ask);

            mpd_t *volume_taker_bid_old = decimal(p_user_val->volume_taker_bid, 0);
            mpd_t *volume_taker_bid = decimal(in_user_val->volume_taker_bid, 0);
            mpd_t *volume_taker_bid_total = mpd_qncopy(mpd_zero);
            mpd_add(volume_taker_bid_total, volume_taker_bid_old, volume_taker_bid, &mpd_ctx);
            free(p_user_val->volume_taker_bid);
            p_user_val->volume_taker_bid = mpd_to_sci(volume_taker_bid_total, 0);
            mpd_del(volume_taker_bid_total);
            mpd_del(volume_taker_bid_old);
            mpd_del(volume_taker_bid);

            mpd_t *volume_taker_ask_old = decimal(p_user_val->volume_taker_ask, 0);
            mpd_t *volume_taker_ask = decimal(in_user_val->volume_taker_ask, 0);
            mpd_t *volume_taker_ask_total = mpd_qncopy(mpd_zero);
            mpd_add(volume_taker_ask_total, volume_taker_ask_old, volume_taker_ask, &mpd_ctx);
            free(p_user_val->volume_taker_ask);
            p_user_val->volume_taker_ask = mpd_to_sci(volume_taker_ask_total, 0);
            mpd_del(volume_taker_ask_total);
            mpd_del(volume_taker_ask_old);
            mpd_del(volume_taker_ask);

            p_user_val->trade_num_taker_ask += in_user_val->trade_num_taker_ask;
            p_user_val->trade_num_taker_bid += in_user_val->trade_num_taker_bid;
            p_user_val->trade_num_total     += in_user_val->trade_num_total;
        } else if (data_type == TYPE_BID_VOLUME){
            mpd_t *bid_volume_total = mpd_qncopy(mpd_zero);
            mpd_t *bid_volume_old = decimal(p_user_val->volume_bid, 0);
            mpd_t *volume_bid = decimal(in_user_val->volume_bid, 0);
            mpd_add(bid_volume_total, bid_volume_old, volume_bid, &mpd_ctx);
            free(p_user_val->volume_bid);
            p_user_val->volume_bid = mpd_to_sci(bid_volume_total, 0);
            mpd_del(bid_volume_total);
            mpd_del(bid_volume_old);
            mpd_del(volume_bid);
        } else if (data_type == TYPE_ASK_VOLUME){
            mpd_t *ask_volume_total = mpd_qncopy(mpd_zero);
            mpd_t *ask_volume_old = decimal(p_user_val->volume_ask, 0);
            mpd_t *volume_ask = decimal(in_user_val->volume_ask, 0);
            mpd_add(ask_volume_total, ask_volume_old, volume_ask, &mpd_ctx);
            free(p_user_val->volume_ask);
            p_user_val->volume_ask = mpd_to_sci(ask_volume_total, 0);
            mpd_del(ask_volume_total);
            mpd_del(ask_volume_old);
            mpd_del(volume_ask);  
        } else if (data_type == TYPE_BID_DEAL){
            mpd_t *bid_deal_total = mpd_qncopy(mpd_zero);
            mpd_t *bid_deal_old = decimal(p_user_val->deal_bid, 0);
            mpd_t *deal_bid = decimal(in_user_val->deal_bid, 0);
            mpd_add(bid_deal_total, bid_deal_old, deal_bid, &mpd_ctx);
            free(p_user_val->deal_bid);
            p_user_val->deal_bid = mpd_to_sci(bid_deal_total, 0);
            mpd_del(bid_deal_total);
            mpd_del(bid_deal_old);
            mpd_del(deal_bid);   
        } else if (data_type == TYPE_ASK_DEAL){
            mpd_t *ask_deal_total = mpd_qncopy(mpd_zero);
            mpd_t *ask_deal_old = decimal(p_user_val->deal_ask, 0);
            mpd_t *deal_ask = decimal(in_user_val->deal_ask, 0);
            mpd_add(ask_deal_total, ask_deal_old, deal_ask, &mpd_ctx);
            free(p_user_val->deal_ask);
            p_user_val->deal_ask = mpd_to_sci(ask_deal_total, 0);
            mpd_del(ask_deal_total);
            mpd_del(ask_deal_old);
            mpd_del(deal_ask);  
        } else if (data_type == TYPE_TAKER_BID_VOLUME){
            mpd_t *taker_bid_volume_total = mpd_qncopy(mpd_zero);
            mpd_t *taker_bid_volume_old = decimal(p_user_val->volume_taker_bid, 0);
            mpd_t *volume_taker_bid = decimal(in_user_val->volume_taker_bid, 0);
            mpd_add(taker_bid_volume_total, taker_bid_volume_old, volume_taker_bid, &mpd_ctx);
            free(p_user_val->volume_taker_bid);
            p_user_val->volume_taker_bid = mpd_to_sci(taker_bid_volume_total, 0);
            mpd_del(taker_bid_volume_total);
            mpd_del(taker_bid_volume_old);
            mpd_del(volume_taker_bid);   
        } else if (data_type == TYPE_TAKER_ASK_VOLUME){
            mpd_t *taker_ask_volume_total = mpd_qncopy(mpd_zero);
            mpd_t *taker_ask_volume_old = decimal(p_user_val->volume_taker_ask, 0);
            mpd_t *volume_taker_ask = decimal(in_user_val->volume_taker_ask, 0);
            mpd_add(taker_ask_volume_total, taker_ask_volume_old, volume_taker_ask, &mpd_ctx);
            free(p_user_val->volume_taker_ask);
            p_user_val->volume_taker_ask = mpd_to_sci(taker_ask_volume_total, 0);
            mpd_del(taker_ask_volume_total);
            mpd_del(taker_ask_volume_old);
            mpd_del(volume_taker_ask);  
        } else if (data_type == TYPE_TAKER_BID_TRADE_NUM){
            user_val.trade_num_taker_bid += in_user_val->trade_num_taker_bid;   
        } else if (data_type == TYPE_TAKER_ASK_TRADE_NUM){
            user_val.trade_num_taker_ask += in_user_val->trade_num_taker_ask;   
        } else if (data_type == TYPE_TAKER_TOTAL_TRADE_NUM){
            user_val.trade_num_total += in_user_val->trade_num_total;   
        }
    }

    return 0;
}

static void clean_expired_data(skiplist_t *minute_list, dict_t *dict_minute, time_t minute, const char *market)
{
    if (skiplist_insert(minute_list, &minute) == NULL) {
        log_fatal("skiplist_insert fail");
    }
 
    time_t cur_minute = time(NULL);
    skiplist_node *node;
    int keep_second = settings.keep_day * 24 * 3600;

    skiplist_iter *iter = skiplist_get_iterator(minute_list);
    while ((node = skiplist_next(iter)) != NULL) {
        time_t clean_minute = (*(time_t*)(node->value));
        if (cur_minute - clean_minute > keep_second) {
            dict_entry *entry = dict_find(dict_minute, &clean_minute);
            if (entry != NULL) {
                dict_delete(dict_minute, entry->key);
                log_info("delete expired minute: %zd, cur_minute: %zd, keep_second: %d, market: %s", clean_minute, cur_minute, keep_second, market);
            }
            skiplist_delete(minute_list, node);
        } else {
            break;
        }
    }
    skiplist_release_iterator(iter);
}

static int count_dict_user_data(dict_t *input_dict_user, dict_t *save_dict_user, int data_type)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(input_dict_user);
    if (iter != NULL) {
        while ((entry = dict_next(iter)) != NULL) {
            uint32_t *p_user_id = entry->key;
            uint32_t user_id = *p_user_id;
            struct dict_user_val *user_val = entry->val;

            int ret = count_user_val(save_dict_user, user_id, user_val, data_type);
            if (ret != 0) {
                log_fatal("count_user_val fail");
                dict_release_iterator(iter);
                return -__LINE__;
            }
        }
        dict_release_iterator(iter);  
    } else {
        log_fatal("dict_get_iterator null");
    }

    return 0;
}

static int get_user_data(dict_t *save_dict_user, time_t start, time_t end, const char *market, int data_type)
{
    time_t cur_minute = time(NULL);
    if (start > cur_minute || start > end) {
        log_error("start: %zd, end: %zd, cur_minute: %zd", start, end, cur_minute);
        return -1;
    }

    struct dict_market_key market_key;
    memset(&market_key, 0, sizeof(market_key));
    strncpy(market_key.market, market, MARKET_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_market, &market_key);
    if (entry == NULL) {
        log_error("not find market, market: %s", market);
        return 0;
    } else {
        struct dict_market_val *market_val = entry->val;
        skiplist_node *node;

        skiplist_iter *iter = skiplist_get_iterator(market_val->minute_list);
        while ((node = skiplist_next(iter)) != NULL) {
            time_t *p_minute = node->value;
            time_t minute = *p_minute;
            if (minute > end)
                break;

            if (minute <= end && minute >= start) {
                entry = dict_find(market_val->dict_minute, &minute);
                if (entry == NULL)
                    continue;

                struct dict_minute_val *minute_val = entry->val;
                int ret = count_dict_user_data(minute_val->dict_user, save_dict_user, data_type);
                if (ret != 0) {
                    log_fatal("count_dict_user_data fail, ret: %d", ret);
                    skiplist_release_iterator(iter);
                    return -__LINE__;
                }
            }
        }
        skiplist_release_iterator(iter);    
    }

    return 0;
}

static int dump_to_db_day(const char *market, const char *stock, time_t start)
{
    time_t end = start + 86400;
    log_info("start: %zd, end: %zd, market: %s", start, end, market);

    dict_t *save_dict_user = create_user_dict();;
    if (save_dict_user == NULL) {
        log_fatal("create_user_dict fail");
        return -__LINE__;
    }

    int data_type = 0; // all data
    int ret = get_user_data(save_dict_user, start, end, market, data_type);
    if (ret != 0) {
        log_error("get_user_data fail, ret: %d", ret);
        dict_release(save_dict_user);
        return ret;
    }
    log_info("save_dict_user size: %d", dict_size(save_dict_user));

    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.free = on_list_free;
    list_t *list_deals = list_create(&lt);

    dict_entry *entry;
    dict_iterator *iter_dict = dict_get_iterator(save_dict_user);
    while ((entry = dict_next(iter_dict)) != NULL) {
        uint32_t *p_user_id = entry->key;
        uint32_t user_id = *p_user_id;
        struct dict_user_val *user_val = entry->val;

        struct deals_info *info = malloc(sizeof(struct deals_info));
        if (info == NULL) {
            log_fatal("malloc fail");
            dict_release(save_dict_user);
            list_release(list_deals);
            dict_release_iterator(iter_dict);
            return -__LINE__; 
        }

        memset(info, 0, sizeof(struct deals_info));
        info->user_id = user_id;
        info->volume_bid = user_val->volume_bid;
        info->volume_ask = user_val->volume_ask;
        info->deal_bid = user_val->deal_bid;
        info->deal_ask = user_val->deal_ask;
        info->volume_taker_bid = user_val->volume_taker_bid;
        info->volume_taker_ask = user_val->volume_taker_ask;
        info->trade_num_taker_bid = user_val->trade_num_taker_bid;
        info->trade_num_taker_ask = user_val->trade_num_taker_ask;
        info->trade_num_total = user_val->trade_num_total;
        list_add_node_tail(list_deals, info);
    }
    dict_release_iterator(iter_dict);

    log_info("market: %s, list_len: %ld", market, list_len(list_deals));
    
    dump_deals_to_db(list_deals, market, stock, start);
    dump_fee_to_db(dict_fee, start);

    dict_release(save_dict_user);
    list_release(list_deals);

    return 0;
}

static int store_to_top_skiplist(dict_t *save_dict_user, skiplist_t *top_list, int data_type)
{
    dict_entry *entry;
    dict_iterator *iter_dict = dict_get_iterator(save_dict_user);
    while ((entry = dict_next(iter_dict)) != NULL) {
        uint32_t *p_user_id = entry->key;
        uint32_t user_id = *p_user_id;
        struct dict_user_val *user_val = entry->val;
        void *value = NULL;

        if (data_type == TYPE_BID_VOLUME) {
            struct list_sort_mpd_val *sort_val = malloc(sizeof(struct list_sort_mpd_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = decimal(user_val->volume_bid, 0);
            value = sort_val;
        }  else if (data_type == TYPE_ASK_VOLUME){
            struct list_sort_mpd_val *sort_val = malloc(sizeof(struct list_sort_mpd_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = decimal(user_val->volume_ask, 0);
            value = sort_val;
        } else if (data_type == TYPE_BID_DEAL) {
            struct list_sort_mpd_val *sort_val = malloc(sizeof(struct list_sort_mpd_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = decimal(user_val->deal_bid, 0);
            value = sort_val;
        } else if (data_type == TYPE_ASK_DEAL){
            struct list_sort_mpd_val *sort_val = malloc(sizeof(struct list_sort_mpd_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = decimal(user_val->deal_ask, 0);
            value = sort_val;
        } else if (data_type == TYPE_TAKER_BID_VOLUME){
            struct list_sort_mpd_val *sort_val = malloc(sizeof(struct list_sort_mpd_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = decimal(user_val->volume_taker_bid, 0);
            value = sort_val;
        } else if (data_type == TYPE_TAKER_ASK_VOLUME){
            struct list_sort_mpd_val *sort_val = malloc(sizeof(struct list_sort_mpd_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = decimal(user_val->volume_taker_ask, 0);
            value = sort_val;
        } else if (data_type == TYPE_TAKER_BID_TRADE_NUM){
            struct list_sort_integer_val *sort_val = malloc(sizeof(struct list_sort_integer_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = user_val->trade_num_taker_bid;
            value = sort_val;
        } else if (data_type == TYPE_TAKER_ASK_TRADE_NUM){
            struct list_sort_integer_val *sort_val = malloc(sizeof(struct list_sort_integer_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = user_val->trade_num_taker_ask;
            value = sort_val;
        } else if (data_type == TYPE_TAKER_TOTAL_TRADE_NUM){
            struct list_sort_integer_val *sort_val = malloc(sizeof(struct list_sort_integer_val));
            if (sort_val == NULL) {
                log_fatal("malloc fail");
                dict_release_iterator(iter_dict);
                return -__LINE__;
            }

            sort_val->user_id = user_id;
            sort_val->num = user_val->trade_num_total;
            value = sort_val;
        }

        if (skiplist_insert(top_list, value) == NULL) {
            log_fatal("skiplist_insert fail");
            if (data_type == TYPE_TAKER_BID_TRADE_NUM || data_type == TYPE_TAKER_ASK_TRADE_NUM || data_type == TYPE_TAKER_TOTAL_TRADE_NUM) {
                free(value);
            } else {
                struct list_sort_mpd_val *sort_val = value;
                mpd_del(sort_val->num);
                free(sort_val);
            }
            dict_release_iterator(iter_dict);
            return -__LINE__;
        }
    }
    dict_release_iterator(iter_dict);

    return 0;
}

static int deal_rank_process(json_t **result, dict_t *save_dict_user, int data_type, int top_num)
{
    skiplist_t *top_list;
    if (data_type == TYPE_TAKER_ASK_TRADE_NUM || data_type == TYPE_TAKER_BID_TRADE_NUM || data_type == TYPE_TAKER_TOTAL_TRADE_NUM) {
        top_list = create_sort_integer_list();
    } else {
        top_list = create_sort_mpd_list();
    }

    int ret = store_to_top_skiplist(save_dict_user, top_list, data_type);
    if (ret != 0) {
        log_error("store_to_top_skiplist fail, ret: %d", ret);
        skiplist_release(top_list);
        dict_release(save_dict_user);
        return ret;
    }

    int total = 0;
    json_t *sort_array_obj = json_array();
    skiplist_node *node;

    skiplist_iter *iter_top = skiplist_get_iterator(top_list);
    while ((node = skiplist_next(iter_top)) != NULL) {
        if (data_type == TYPE_TAKER_ASK_TRADE_NUM || data_type == TYPE_TAKER_BID_TRADE_NUM || data_type == TYPE_TAKER_TOTAL_TRADE_NUM) {
            struct list_sort_integer_val *val = node->value;
            json_t *sort_obj = json_object();
            json_object_set_new(sort_obj, "user_id", json_integer(val->user_id));
            json_object_set_new(sort_obj, "num", json_integer(val->num));
            json_array_append_new(sort_array_obj, sort_obj);
        } else {
            struct list_sort_mpd_val *val = node->value;
            json_t *sort_obj = json_object();
            json_object_set_new(sort_obj, "user_id", json_integer(val->user_id));
            json_object_set_new_mpd(sort_obj, "num", val->num);
            json_array_append_new(sort_array_obj, sort_obj);
        }

        if (++total > top_num) {
            break;
        }
    }
    skiplist_release_iterator(iter_top);

    *result = json_object();
    json_object_set_new(*result, "sort", sort_array_obj);
    skiplist_release(top_list);

    return 0;
}

int deal_top_market(json_t **result, time_t start, time_t end, const char *market, int data_type, int top_num)
{
    log_info("start: %zd, end: %zd, market: %s, data_type: %d, top_num: %d", start, end, market, data_type, top_num);
    dict_t *save_dict_user = create_user_dict();;
    if (save_dict_user == NULL) {
        log_fatal("create_user_dict fail");
        return -__LINE__;
    }

    int ret = get_user_data(save_dict_user, start, end, market, data_type);
    if (ret != 0) {
        log_error("get_user_data fail, ret: %d", ret);
        dict_release(save_dict_user);
        return ret;
    }
    log_info("save_dict_user size: %d", dict_size(save_dict_user));

    if (dict_size(save_dict_user) == 0) {
        *result = json_object();
        json_t *sort_array_obj = json_array();
        json_object_set_new(*result, "sort", sort_array_obj);
        dict_release(save_dict_user);
        return 0;
    }

    ret = deal_rank_process(result, save_dict_user, data_type, top_num);
    dict_release(save_dict_user);

    return ret;
}

void clear_fee_dict(void)
{
    dict_clear(dict_fee);   
}

int dump_deal_and_fee(time_t start)
{
    int ret = 0;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_market);
    while ((entry = dict_next(iter)) != NULL) {
        struct dict_market_key *key = entry->key;
        int ret = dump_to_db_day(key->market, key->stock, start);
        if (ret != 0)
            break;
    }
    dict_release_iterator(iter);

    return ret;
}

int deals_process(bool is_taker, uint32_t user_id, time_t timestamp, int side, const char *market, const char *stock, 
        const char *volume_str, const char *price_str, const char *fee_asset, const char *fee_str)
{
    struct dict_market_key market_key;
    memset(&market_key, 0, sizeof(market_key));
    strncpy(market_key.market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(market_key.stock, stock, STOCK_NAME_MAX_LEN - 1);

    int interval_second = settings.interval_minute * 60;
    int error_line = 0;
    time_t minute = timestamp / interval_second * interval_second;

    mpd_t *price = NULL;
    if (!price_str || (price = decimal(price_str, 0)) == NULL) {
        log_error("price invalid");
        return  -__LINE__;
    }

    mpd_t *volume = NULL;
    if (!volume_str || (volume = decimal(volume_str, settings.prec)) == NULL) {
        log_error("volume invalid");
        mpd_del(price);
        return  -__LINE__;
    }

    mpd_t *fee = NULL;
    if (!fee_str || (fee = decimal(fee_str, settings.prec)) == NULL) {
        log_error("fee invalid");
        mpd_del(price);
        mpd_del(volume);
        return  -__LINE__;
    }

    mpd_t *deal = mpd_new(&mpd_ctx);
    mpd_mul(deal, price, volume, &mpd_ctx);
    mpd_rescale(deal, deal, -settings.prec, &mpd_ctx);

    char *volume_prec_str = mpd_to_sci(volume, 0);
    char *deal_prec_str = mpd_to_sci(deal, 0);
    char *fee_prec_str = mpd_to_sci(fee, 0);

    add_fee(fee_asset, fee, fee_prec_str);

    dict_entry *entry = dict_find(dict_market, &market_key);
    if (entry == NULL) { //market not find
        int ret = process_new_market(is_taker, &market_key, minute, user_id, side, market, volume_prec_str, deal_prec_str, fee_asset, fee_prec_str);
        if (ret != 0) {
            error_line = -__LINE__;
            goto error_process;  
        }
    } else { //market find
        struct dict_market_val *market_val = entry->val;
        dict_t *dict_minute = market_val->dict_minute;

        entry = dict_find(dict_minute, &minute);
        if (entry == NULL) { //new minute
            int ret = process_new_minute(is_taker, dict_minute, minute, user_id, side, market, volume_prec_str, deal_prec_str, fee_asset, fee_prec_str);
            if (ret != 0) {
                error_line = -__LINE__;
                goto error_process;  
            }

            clean_expired_data(market_val->minute_list, dict_minute, minute, market);
        } else { //the same minute
            struct dict_minute_val *p_minute_val = entry->val;
            int ret = update_user_val(is_taker, p_minute_val->dict_user, user_id, side, fee_asset, volume, deal, fee, volume_prec_str, deal_prec_str, fee_prec_str);
            if (ret != 0) {
                error_line = -__LINE__;
                goto error_process;
            }
        }
    }

error_process:
    mpd_del(price);
    mpd_del(volume);
    mpd_del(deal);
    mpd_del(fee);
    free(deal_prec_str);
    free(volume_prec_str);
    free(fee_prec_str);

    return error_line;
}

int init_deal(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = dict_market_hash_function;
    type.key_compare    = dict_market_key_compare;
    type.key_dup        = dict_market_key_dup;
    type.val_dup        = dict_market_val_dup;
    type.key_destructor = dict_market_key_free;
    type.val_destructor = dict_market_val_free;
    dict_market = dict_create(&type, 64);
    if (dict_market == NULL)
        return -__LINE__;

    dict_fee = create_fee_dict();
    if (dict_fee == NULL)
        return -__LINE__;
    
    return 0;
}

