/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# include "ca_depth_common.h"

uint32_t dict_depth_key_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct depth_key));
}

int dict_depth_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct depth_key));
}

void *dict_depth_key_dup(const void *key)
{
    struct depth_key *obj = malloc(sizeof(struct depth_key));
    memcpy(obj, key, sizeof(struct depth_key));
    return obj;
}

void dict_depth_key_free(void *key)
{
    free(key);
}

uint32_t dict_ses_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(void *));
}

int dict_ses_hash_compare(const void *key1, const void *key2)
{
    return key1 == key2 ? 0 : 1;
}

void depth_set_key(struct depth_key *key, const char *market, const char *interval, uint32_t limit)
{
    memset(key, 0, sizeof(struct depth_key));
    strncpy(key->market, market, MARKET_NAME_MAX_LEN - 1);
    strncpy(key->interval, interval, INTERVAL_MAX_LEN - 1);
    key->limit = limit;
}