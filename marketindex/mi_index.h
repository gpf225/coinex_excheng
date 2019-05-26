# ifndef MI_INDEX_H
# define MI_INDEX_H

# include "mi_config.h"

int init_index(void);

int reload_index_config(void);
bool market_exists(const char *market_name);
json_t *get_market_list(void);
json_t *get_market_index(const char *market_name);

# endif

