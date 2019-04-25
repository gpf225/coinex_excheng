# ifndef _CA_DEPTH_H_
# define _CA_DEPTH_H_

struct dict_depth_key {
    char      market[MARKET_NAME_MAX_LEN];
    char      interval[INTERVAL_MAX_LEN];
};

int init_depth(void);
int depth_unsubscribe_all(nw_ses *ses);
int depth_unsubscribe(nw_ses *ses, const char *market, const char *interval);
int depth_subscribe(nw_ses *ses, const char *market, const char *interval);
int depth_request(nw_ses *ses, rpc_pkg *pkg, const char *market, int limit, const char *interval);
void depth_set_key(struct dict_depth_key *key, const char *market, const char *interval);
dict_t* dict_create_depth_session(void);

uint32_t dict_depth_hash_func(const void *key);
int dict_depth_key_compare(const void *key1, const void *key2);
void *dict_depth_key_dup(const void *key);
void dict_depth_key_free(void *key);

# endif