# include "dr_config.h"
# include "dr_fee_rate.h"

static dict_t *dict_fee_rate;

mpd_t *mpd_interval;

static uint32_t dict_fee_rate_hash_function(const void *key)
{
    const struct dict_fee_rate_key *obj = key;
    return dict_generic_hash_function(obj->market, strlen(obj->market));
}

static int dict_fee_rate_key_compare(const void *key1, const void *key2)
{
    const struct dict_fee_rate_key *obj1 = key1;
    const struct dict_fee_rate_key *obj2 = key2;

    return strcmp(obj1->market, obj2->market);
}

static void *dict_fee_rate_key_dup(const void *key)
{
    struct dict_fee_rate_key *obj = malloc(sizeof(struct dict_fee_rate_key));
    memcpy(obj, key, sizeof(struct dict_fee_rate_key));
    return obj;
}

static void dict_fee_rate_key_free(void *key)
{
    free(key);
}

static void dict_fee_rate_val_free(void *val)
{
    struct dict_fee_rate_val *obj = val;
    for (int i = 0; i < MAX_GEAR; i++)
        mpd_del(obj->volume_gear[i]);

    free(obj);
}

// [0 ~ 0.0001) [0.0001, 0.0002) ...
static uint32_t get_gear(mpd_t *fee_rate)
{
    mpd_t *gear = mpd_new(&mpd_ctx);

    int i;
	for (i = MAX_GEAR-1; i >= 0; i--) {
    	mpd_set_i32(gear, i, &mpd_ctx);
        mpd_mul(gear, gear, mpd_interval, &mpd_ctx);

        if (mpd_cmp(fee_rate, gear, &mpd_ctx) >= 0) {
        	break;
        }
	}

	mpd_del(gear);
    return i;
}

int fee_rate_process(const char *market, const char *stock, mpd_t *fee_rate, mpd_t *volume)
{
	struct dict_fee_rate_key fee_rate_key;
    memset(&fee_rate_key, 0, sizeof(fee_rate_key));
    strncpy(fee_rate_key.market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(fee_rate_key.stock, stock, STOCK_NAME_MAX_LEN - 1);

    dict_entry *entry = dict_find(dict_fee_rate, &fee_rate_key);
    if (entry == NULL) {
		struct dict_fee_rate_val *fee_rate_val = malloc(sizeof(struct dict_fee_rate_val));
		entry = dict_add(dict_fee_rate, &fee_rate_key, fee_rate_val);	
		if (entry == NULL) {
        	log_fatal("dict_add fail");
        	return  -__LINE__;
		}

        for (int i = 0; i < MAX_GEAR; i++) {
            fee_rate_val->volume_gear[i] = mpd_new(&mpd_ctx);
            mpd_copy(fee_rate_val->volume_gear[i], mpd_zero, &mpd_ctx);
        }
    }

	uint32_t index = get_gear(fee_rate);
	struct dict_fee_rate_val *rate_val = entry->val;
    mpd_add(rate_val->volume_gear[index], rate_val->volume_gear[index], volume, &mpd_ctx);

    return 0;
}

dict_t *get_fee_rate_dict(void)
{
	return dict_fee_rate;
}

int init_fee_rate(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = dict_fee_rate_hash_function;
    type.key_compare    = dict_fee_rate_key_compare;
    type.key_dup        = dict_fee_rate_key_dup;
    type.key_destructor = dict_fee_rate_key_free;
    type.val_destructor = dict_fee_rate_val_free;
    dict_fee_rate = dict_create(&type, 64);
    if (dict_fee_rate == NULL)
        return -__LINE__;

    mpd_interval = mpd_new(&mpd_ctx);
    mpd_set_string(mpd_interval, "0.0001", &mpd_ctx);

    return 0;
}
