/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/28, create
 */

# ifndef _AR_DEPTH_COMMON_H_
# define _AR_DEPTH_COMMON_H_

# include "ar_config.h"

struct depth_key {
    char market[MARKET_NAME_MAX_LEN];
    char interval[INTERVAL_MAX_LEN];
    uint32_t limit;
};

uint32_t dict_depth_key_hash_func(const void *key);
int dict_depth_key_compare(const void *key1, const void *key2);
void *dict_depth_key_dup(const void *key);
void dict_depth_key_free(void *key);

uint32_t dict_ses_hash_func(const void *key);
int dict_ses_hash_compare(const void *key1, const void *key2);

void depth_set_key(struct depth_key *key, const char *market, const char *interval, uint32_t limit);

json_t *depth_get_result(json_t *result, uint32_t limit);

# endif