# ifndef _AR_CACHE_H_
# define _AR_CACHE_H_

struct cache_exp_val {
    double      time_exp;
    json_t      *result;
};

int init_cache(void);
int check_exp_cache(nw_ses *ses, sds key, uint32_t cmd, json_t *params);
void dict_replace_cache(sds cache_key, struct cache_exp_val *val);

#endif

