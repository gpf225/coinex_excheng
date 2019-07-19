/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "ah_message.h"
# include <librdkafka/rdkafka.h>

static rd_kafka_t *rk;
static rd_kafka_topic_t *rkt_notice;
static list_t *list_message;
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

static void on_list_free(void *value)
{
    free(value);
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
    if (list_len(list_message) > 0) {
        produce_list(list_message, rkt_notice);
    }

    rd_kafka_poll(rk, 0);
}

int init_message(void)
{
    char errstr[1024];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", settings.brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        log_stderr("Set kafka brokers: %s fail: %s", settings.brokers, errstr);
        return -__LINE__;
    }
    if (rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
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

    rkt_notice = rd_kafka_topic_new(rk, TOPIC_NOTICE, NULL);
    if (rkt_notice == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }

    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.free = on_list_free;

    list_message = list_create(&lt);
    if (list_message == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);

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

int notice_user_message(json_t *message, nw_ses *ses, int64_t id)
{
    if (!message || !json_object_get(message, "user_id")) {
        http_reply_error_invalid_argument(ses, id);
        return -__LINE__;
    }

    push_message(json_dumps(message, 0), rkt_notice, list_message);
    http_reply_success(ses, id);
    profile_inc("push_user_message", 1);

    return 0;
}

