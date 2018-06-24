/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# ifndef _ME_MARKET_H_
# define _ME_MARKET_H_

# include "me_config.h"

extern uint64_t order_id_start;
extern uint64_t deals_id_start;

typedef struct market_t {
    char            *name;
    char            *stock;
    char            *money;
    int             stock_prec;
    int             money_prec;
    int             fee_prec;
    mpd_t           *min_amount;

    dict_t          *orders;
    dict_t          *stops;
    dict_t          *user_orders;
    dict_t          *user_stops;

    skiplist_t      *asks;
    skiplist_t      *bids;
    skiplist_t      *stop_asks;
    skiplist_t      *stop_bids;

    mpd_t           *last;
} market_t;

typedef struct order_t {
    uint64_t        id;
    uint32_t        type;
    uint32_t        side;
    double          create_time;
    double          update_time;
    uint32_t        user_id;
    char            *market;
    char            *source;
    char            *fee_asset;
    mpd_t           *fee_discount;
    mpd_t           *price;
    mpd_t           *amount;
    mpd_t           *taker_fee;
    mpd_t           *maker_fee;
    mpd_t           *left;
    mpd_t           *frozen;
    mpd_t           *deal_stock;
    mpd_t           *deal_money;
    mpd_t           *deal_fee;
    mpd_t           *asset_fee;

    mpd_t           *fee_price;
} order_t;

typedef struct stop_t {
    uint64_t        id;
    uint32_t        type;
    uint32_t        side;
    double          create_time;
    double          update_time;
    uint32_t        user_id;
    char            *market;
    char            *source;
    char            *fee_asset;
    mpd_t           *fee_discount;
    mpd_t           *stop_price;
    mpd_t           *price;
    mpd_t           *amount;
    mpd_t           *taker_fee;
    mpd_t           *maker_fee;
} stop_t;

int init_market(void);

market_t *market_create(struct market *conf);
int market_update(market_t *m, struct market *conf);

int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount,
        mpd_t *price, mpd_t *taker_fee, mpd_t *maker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount);

int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount,
        mpd_t *taker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount);

int market_put_stop_limit(bool real, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *stop_price, mpd_t *price,
        mpd_t *taker_fee, mpd_t *maker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount);

int market_put_stop_market(bool real, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *stop_price,
        mpd_t *taker_fee, const char *source, const char *fee_asset, mpd_t *fee_discount);

json_t *get_order_info(order_t *order);
json_t *get_stop_info(stop_t *stop);

order_t *market_get_order(market_t *m, uint64_t order_id);
stop_t *market_get_stop(market_t *m, uint64_t order_id);

int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order);
int market_cancel_stop(bool real, json_t **result, market_t *m, stop_t *stop);

int market_put_order(market_t *m, order_t *order);
int market_put_stop(market_t *m, stop_t *stop);

skiplist_t *market_get_order_list(market_t *m, uint32_t user_id);
skiplist_t *market_get_stop_list(market_t *m, uint32_t user_id);

int market_get_status(market_t *m, size_t *user_count, size_t *ask_count, mpd_t *ask_amount, mpd_t *ask_value,
        size_t *bid_count, mpd_t *bid_amount, mpd_t *bid_value, mpd_t *last);

sds market_status(sds reply);

# endif

