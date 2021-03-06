/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/08, create
 */

# include "me_config.h"
# include "me_message.h"

# include <librdkafka/rdkafka.h>

static rd_kafka_t *rk;

static rd_kafka_topic_t *rkt_deals;
static rd_kafka_topic_t *rkt_stops;
static rd_kafka_topic_t *rkt_orders;
static rd_kafka_topic_t *rkt_balances;
static rd_kafka_topic_t *rkt_his_deals;
static rd_kafka_topic_t *rkt_his_stops;
static rd_kafka_topic_t *rkt_his_orders;
static rd_kafka_topic_t *rkt_his_balances;

static list_t *list_deals;
static list_t *list_stops;
static list_t *list_orders;
static list_t *list_balances;
static list_t *list_his_deals;
static list_t *list_his_stops;
static list_t *list_his_orders;
static list_t *list_his_balances;

static nw_timer timer;

static void on_logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
    log_error("RDKAFKA-%i-%s: %s: %s\n", level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

static void on_delivery(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    if (rkmessage->err) {
        log_fatal("Message delivery failed (topic: %s, %zd bytes, partition: %"PRId32"): %s",
                rd_kafka_topic_name(rkmessage->rkt), rkmessage->len, rkmessage->partition, rd_kafka_err2str(rkmessage->err));
    } else {
        log_trace("Message delivery success (topic: %s, %zd bytes, partition: %"PRId32")",
                rd_kafka_topic_name(rkmessage->rkt), rkmessage->len, rkmessage->partition);
    }
}

static void produce_list(list_t *list, rd_kafka_topic_t *topic)
{
    list_node *node;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        int ret = rd_kafka_produce(topic, 0, RD_KAFKA_MSG_F_COPY, node->value, strlen(node->value), NULL, 0, NULL);
        if (ret == -1) {
            log_fatal("Failed to produce: %s to topic %s: %s\n", (char *)node->value,
                    rd_kafka_topic_name(topic), rd_kafka_err2str(rd_kafka_last_error()));
            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                break;
            }
        }
        list_del(list, node);
    }
    list_release_iterator(iter);
}

static void on_timer(nw_timer *t, void *privdata)
{
    if (list_len(list_deals) > 0) {
        produce_list(list_deals, rkt_deals);
    }
    if (list_len(list_stops) > 0) {
        produce_list(list_stops, rkt_stops);
    }
    if (list_len(list_orders) > 0) {
        produce_list(list_orders, rkt_orders);
    }
    if (list_len(list_balances) > 0) {
        produce_list(list_balances, rkt_balances);
    }
    if (list_len(list_his_deals) > 0) {
        produce_list(list_his_deals, rkt_his_deals);
    }
    if (list_len(list_his_stops) > 0) {
        produce_list(list_his_stops, rkt_his_stops);
    }
    if (list_len(list_his_orders) > 0) {
        produce_list(list_his_orders, rkt_his_orders);
    }
    if (list_len(list_his_balances) > 0) {
        produce_list(list_his_balances, rkt_his_balances);
    }

    rd_kafka_poll(rk, 0);
}

static void on_list_free(void *value)
{
    free(value);
}

int init_message(void)
{
    char errstr[1024];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", settings.brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        log_stderr("Set kafka brokers: %s fail: %s", settings.brokers, errstr);
        return -__LINE__;
    }
    if (rd_kafka_conf_set(conf, "queue.buffering.max.ms", "10", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        log_stderr("Set kafka buffering: %s fail: %s", settings.brokers, errstr);
        return -__LINE__;
    }
    rd_kafka_conf_set_log_cb(conf, on_logger);
    rd_kafka_conf_set_dr_msg_cb(conf, on_delivery);

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (rk == NULL) {
        log_stderr("Failed to create new producer: %s", errstr);
        return -__LINE__;
    }

    rkt_balances = rd_kafka_topic_new(rk, TOPIC_BALANCE, NULL);
    if (rkt_balances == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_orders = rd_kafka_topic_new(rk, TOPIC_ORDER, NULL);
    if (rkt_orders == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_deals = rd_kafka_topic_new(rk, TOPIC_DEAL, NULL);
    if (rkt_deals == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_stops = rd_kafka_topic_new(rk, TOPIC_STOP, NULL);
    if (rkt_stops == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_his_deals = rd_kafka_topic_new(rk, TOPIC_HIS_DEAL, NULL);
    if (rkt_his_deals == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_his_stops = rd_kafka_topic_new(rk, TOPIC_HIS_STOP, NULL);
    if (rkt_his_stops == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_his_orders = rd_kafka_topic_new(rk, TOPIC_HIS_ORDER, NULL);
    if (rkt_his_orders == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_his_balances = rd_kafka_topic_new(rk, TOPIC_HIS_BALANCE, NULL);
    if (rkt_his_balances == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }

    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.free = on_list_free;

    list_deals = list_create(&lt);
    if (list_deals == NULL)
        return -__LINE__;
    list_stops = list_create(&lt);
    if (list_stops == NULL)
        return -__LINE__;
    list_orders = list_create(&lt);
    if (list_orders == NULL)
        return -__LINE__;
    list_balances = list_create(&lt);
    if (list_balances == NULL)
        return -__LINE__;
    list_his_deals = list_create(&lt);
    if (list_his_deals == NULL)
        return -__LINE__;
    list_his_stops = list_create(&lt);
    if (list_his_stops == NULL)
        return -__LINE__;
    list_his_orders = list_create(&lt);
    if (list_his_orders == NULL)
        return -__LINE__;
    list_his_balances = list_create(&lt);
    if (list_his_balances == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int fini_message(void)
{
    on_timer(NULL, NULL);
    rd_kafka_flush(rk, 1000);

    rd_kafka_topic_destroy(rkt_deals);
    rd_kafka_topic_destroy(rkt_stops);
    rd_kafka_topic_destroy(rkt_orders);
    rd_kafka_topic_destroy(rkt_balances);
    rd_kafka_topic_destroy(rkt_his_deals);
    rd_kafka_topic_destroy(rkt_his_stops);
    rd_kafka_topic_destroy(rkt_his_orders);
    rd_kafka_topic_destroy(rkt_his_balances);
    rd_kafka_destroy(rk);

    return 0;
}

static int push_message(char *message, rd_kafka_topic_t *topic, list_t *list)
{
    log_trace("push %s message: %s", rd_kafka_topic_name(topic), message);

    if (list->len) {
        list_add_node_tail(list, message);
        return 0;
    }

    int ret = rd_kafka_produce(topic, 0, RD_KAFKA_MSG_F_COPY, message, strlen(message), NULL, 0, NULL);
    if (ret == -1) {
        profile_inc("message_push_fail", 1);
        log_fatal("Failed to produce: %s to topic %s: %s\n", message, rd_kafka_topic_name(topic), rd_kafka_err2str(rd_kafka_last_error()));
        if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            list_add_node_tail(list, message);
            return 0;
        }
        free(message);
        return -__LINE__;
    }
    free(message);

    return 0;
}

int push_deal_message(double t, uint64_t id, market_t *market, int side, order_t *ask, order_t *bid,
        mpd_t *price, mpd_t *amount, mpd_t *deal, const char *ask_fee_asset, mpd_t *ask_fee, const char *bid_fee_asset, mpd_t *bid_fee)
{
    json_t *message = json_object();

    json_object_set_new(message, "timestamp", json_real(t));
    json_object_set_new(message, "id", json_integer(id));
    json_object_set_new(message, "market", json_string(market->name));
    json_object_set_new(message, "stock", json_string(market->stock));
    json_object_set_new(message, "money", json_string(market->money));
    json_object_set_new(message, "side", json_integer(side));
    json_object_set_new(message, "ask_id", json_integer(ask->id));
    json_object_set_new(message, "bid_id", json_integer(bid->id));
    json_object_set_new(message, "ask_user_id", json_integer(ask->user_id));
    json_object_set_new(message, "ask_account", json_integer(ask->account));
    json_object_set_new(message, "bid_user_id", json_integer(bid->user_id));
    json_object_set_new(message, "bid_account", json_integer(bid->account));
    json_object_set_new(message, "ask_fee_asset", json_string(ask_fee_asset));
    json_object_set_new(message, "bid_fee_asset", json_string(bid_fee_asset));
    json_object_set_new_mpd(message, "price", price);
    json_object_set_new_mpd(message, "amount", amount);
    json_object_set_new_mpd(message, "deal", deal);
    json_object_set_new_mpd(message, "ask_fee", ask_fee);
    json_object_set_new_mpd(message, "bid_fee", bid_fee);
    if (ask->client_id) {
        json_object_set_new(message, "ask_client_id", json_string(ask->client_id));
    } else {
        json_object_set_new(message, "ask_client_id", json_string(""));
    }

    if (bid->client_id) {
        json_object_set_new(message, "bid_client_id", json_string(bid->client_id));
    } else {
        json_object_set_new(message, "bid_client_id", json_string(""));
    }

    push_message(json_dumps(message, 0), rkt_deals, list_deals);
    json_decref(message);
    profile_inc("message_deal", 1);

    return 0;
}

int push_stop_message(uint32_t event, stop_t *stop, market_t *market, int status)
{
    json_t *order_info = get_stop_info(stop);
    json_object_set_new(order_info, "status", json_integer(status));

    json_t *message = json_object();
    json_object_set_new(message, "event", json_integer(event));
    json_object_set_new(message, "order", order_info);

    push_message(json_dumps(message, 0), rkt_stops, list_stops);
    json_decref(message);
    profile_inc("message_stop", 1);

    return 0;
}

int push_order_message(uint32_t event, order_t *order, market_t *market)
{
    json_t *message = json_object();
    json_object_set_new(message, "event", json_integer(event));
    json_object_set_new(message, "order", get_order_info(order, true));
    json_object_set_new(message, "balance", get_order_balance(order, market));
    json_object_set_new(message, "stock", json_string(market->stock));
    json_object_set_new(message, "money", json_string(market->money));

    push_message(json_dumps(message, 0), rkt_orders, list_orders);
    json_decref(message);
    profile_inc("message_order", 1);

    return 0;
}

int push_balance_message(double t, uint32_t user_id, uint32_t account, const char *asset, mpd_t *available, mpd_t *frozen)
{
    json_t *message = json_object();
    json_object_set_new(message, "timestamp", json_real(t));
    json_object_set_new(message, "user_id", json_integer(user_id));
    json_object_set_new(message, "account", json_integer(account));
    json_object_set_new(message, "asset", json_string(asset));
    json_object_set_new_mpd(message, "available", available);
    json_object_set_new_mpd(message, "frozen", frozen);

    push_message(json_dumps(message, 0), rkt_balances, list_balances);
    json_decref(message);
    profile_inc("message_balance", 1);

    return 0;
}

int push_his_deal_message(json_t *msg)
{
    push_message(json_dumps(msg, 0), rkt_his_deals, list_his_deals);
    profile_inc("message_his_deal", 1);
    return 0;
}

int push_his_stop_message(json_t *msg)
{
    push_message(json_dumps(msg, 0), rkt_his_stops, list_his_stops);
    profile_inc("message_his_stop", 1);
    return 0;
}

int push_his_order_message(json_t *msg)
{
    push_message(json_dumps(msg, 0), rkt_his_orders, list_his_orders);
    profile_inc("message_his_order", 1);
    return 0;
}

int push_his_balance_message(json_t *msg)
{
    push_message(json_dumps(msg, 0), rkt_his_balances, list_his_balances);
    profile_inc("message_his_balance", 1);
    return 0;
}

bool is_message_block(void)
{
    if (list_deals->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_stops->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_orders->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_balances->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_his_deals->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_his_stops->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_his_orders->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_his_balances->len >= MAX_PENDING_MESSAGE)
        return true;

    return false;
}

sds message_status(sds reply)
{
    reply = sdscatprintf(reply, "message deals pending: %lu\n", list_deals->len);
    reply = sdscatprintf(reply, "message stops pending: %lu\n", list_stops->len);
    reply = sdscatprintf(reply, "message orders pending: %lu\n", list_orders->len);
    reply = sdscatprintf(reply, "message balances pending: %lu\n", list_balances->len);
    reply = sdscatprintf(reply, "message his_deals pending: %lu\n", list_his_deals->len);
    reply = sdscatprintf(reply, "message his_stops pending: %lu\n", list_his_stops->len);
    reply = sdscatprintf(reply, "message his_orders pending: %lu\n", list_his_orders->len);
    reply = sdscatprintf(reply, "message his_balances pending: %lu\n", list_his_balances->len);
    return reply;
}

