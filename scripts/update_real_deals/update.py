#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
在redis新增key: k:<marekta>:real_deals, 存放真实的交易数据，
第一次启动，需要从key: k:<market>:deals过滤真实交易存到k:<marekta>:real_deals中
"""
import os
import sys
import redis
import json

redis_host = "127.0.0.1"
redis_port = 6379

MAX_NUM = 10000

def get_redis_client():
    return redis.StrictRedis(redis_host, redis_port)

def get_deal_markets(client):
    results = client.scan_iter("k:*:deals")
    markets = set()
    for item in results:
        keys = item.split(":")
        if len(keys) == 3:
            markets.add(keys[1])
    return markets


def get_deals(client, market):
    key = "k:{}:deals".format(market)
    results = client.lrange(key, 0, MAX_NUM - 1)
    return results


def set_real_deals(client, market, real_deals):
    print("market:{}, deals: {}".format(market, real_deals))
    key = "k:{}:real_deals".format(market)
    client.lpush(key, *real_deals)


def main():
    client = get_redis_client()
    markets = get_deal_markets(client)
    for market in markets:
        real_deals = []
        deals = get_deals(client, market)
        for deal in deals:
            deal_json = json.loads(deal)
            if deal_json and deal_json.get('ask_user_id') != 0 and deal_json.get('bid_user_id') != 0:
                real_deals.append(deal)
        if real_deals:
            set_real_deals(client, market, real_deals)


if __name__ == '__main__':
    main()
