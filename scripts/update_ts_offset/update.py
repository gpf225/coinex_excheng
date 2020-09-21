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
import time

redis_host = "127.0.0.1"
redis_port = 6379

MAX_NUM = 10000

def get_redis_client():
    return redis.StrictRedis(redis_host, redis_port)

def update_offset(client):
    now = int(time.time())
    curr_hour = now / 3600 * 3600
    #end = curr_hour - 7 * 86400;
    end = curr_hour;
    start = curr_hour - 14 * 86400;
    while end > start :
        order_offset = client.hget("s:offset:orders", end)
        deal_offset = client.hget("s:offset:deals", end)
        print(end)
        if (order_offset is None) or (deal_offset is None) :
            end = end - 3600
            continue

        print(end)
        offset_new = {}
        offset_new['order_offset'] = int(order_offset)
        offset_new['deal_offset'] = int(deal_offset)
        offset_new_str = json.dumps(offset_new)
        client.hset("s:persist:offset", end, offset_new_str)
        break


def main():
    client = get_redis_client()
    update_offset(client)

if __name__ == '__main__':
    main()
