# ifndef _AR_CACHE_H_
# define _AR_CACHE_H_

struct cache_val {
    double      time_exp;
    json_t      *result;
};

int init_cache(void);
int check_cache(nw_ses *ses, sds key, uint32_t cmd, json_t *params);
void dict_replace_cache(sds cache_key, struct cache_val *val);

#endif
