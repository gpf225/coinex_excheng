/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/06, create
 */

# include "me_config.h"
# include "me_history.h"
# include "me_balance.h"
# include "me_message.h"

static json_t* get_deals_json(double t, uint64_t deal_id, order_t *ask, int ask_role, order_t *bid, int bid_role,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *ask_fee_asset, mpd_t *ask_fee, const char *bid_fee_asset, mpd_t *bid_fee)
{
    json_t *obj = json_object();
    json_object_set_new(obj, "time", json_real(t));
    json_object_set_new(obj, "market", json_string(ask->market));
    json_object_set_new(obj, "deal_id", json_integer(deal_id));
    json_object_set_new(obj, "ask_user_id", json_integer(ask->user_id));
    json_object_set_new(obj, "ask_account", json_integer(ask->account));
    json_object_set_new(obj, "bid_user_id", json_integer(bid->user_id));
    json_object_set_new(obj, "bid_account", json_integer(bid->account));
    json_object_set_new(obj, "ask_order_id", json_integer(ask->id));
    json_object_set_new(obj, "bid_order_id", json_integer(bid->id));
    json_object_set_new(obj, "ask_side", json_integer(ask->side));
    json_object_set_new(obj, "bid_side", json_integer(bid->side));
    json_object_set_new(obj, "ask_role", json_integer(ask_role));
    json_object_set_new(obj, "bid_role", json_integer(bid_role));

    json_object_set_new_mpd(obj, "price", price);
    json_object_set_new_mpd(obj, "amount", amount);
    json_object_set_new_mpd(obj, "deal", deal);
    json_object_set_new_mpd(obj, "ask_fee", ask_fee);
    json_object_set_new_mpd(obj, "bid_fee", bid_fee);
    json_object_set_new(obj, "ask_fee_asset", json_string(ask_fee_asset));
    json_object_set_new(obj, "bid_fee_asset", json_string(bid_fee_asset));

    return obj;
}

static json_t* get_balance_json(double t, uint32_t user_id, uint32_t account, const char *asset, const char *business, mpd_t *change,  mpd_t *balance, const char *detail)
{
    json_t *obj = json_object();
    json_object_set_new(obj, "time", json_real(t));
    json_object_set_new(obj, "user_id", json_integer(user_id));
    json_object_set_new(obj, "account", json_integer(account));
    json_object_set_new(obj, "asset", json_string(asset));
    json_object_set_new(obj, "business", json_string(business));
    json_object_set_new_mpd(obj, "change", change);
    json_object_set_new_mpd(obj, "balance", balance);
    json_object_set_new(obj, "detail", json_string(detail));

    return obj;
}

int append_stop_history(stop_t *stop, int status)
{
    json_t *order_info = get_stop_info(stop);
    json_object_set_new(order_info, "status", json_integer(status));
    push_his_stop_message(order_info);
    json_decref(order_info);
    return 0;
}

int append_deal_history(double t, uint64_t deal_id, order_t *ask, int ask_role, order_t *bid, int bid_role,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *ask_fee_asset, mpd_t *ask_fee, const char *bid_fee_asset, mpd_t *bid_fee)
{
    json_t *obj = get_deals_json(t, deal_id, ask, ask_role, bid, bid_role, price, amount, deal, ask_fee_asset, ask_fee, bid_fee_asset, bid_fee);
    push_his_deal_message(obj);
    json_decref(obj);
    return 0;
}

int append_order_history(order_t *order)
{
    json_t *order_info = get_order_info(order);
    push_his_order_message(order_info);
    json_decref(order_info);
    return 0;
}

int append_user_balance_history(double t, uint32_t user_id, uint32_t account, const char *asset, const char *business, mpd_t *change, const char *detail)
{
    mpd_t *balance = balance_total(user_id, account, asset);
    json_t *obj = get_balance_json(t, user_id, account, asset, business, change, balance, detail);
    push_his_balance_message(obj);
    mpd_del(balance);
    json_decref(obj);
    return 0;
}

