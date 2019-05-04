/*
 * Description: 
 *     History: ouxiangyang, 2019/03/27, create
 */

# include "ts_config.h"
# include "ts_message.h"

static kafka_consumer_t *kafka_deals;
static kafka_consumer_t *kafka_orders;

static void on_deals_message(sds message, int64_t offset)
{
}

static void on_orders_message(sds message, int64_t offset)
{
}

int init_message(void)
{
    kafka_deals = kafka_consumer_create(&settings.deals, on_deals_message);
    if (kafka_deals == NULL)
        return -__LINE__;

    kafka_orders = kafka_consumer_create(&settings.orders, on_orders_message);
    if (kafka_orders == NULL)
        return -__LINE__;

    return 0;
}

