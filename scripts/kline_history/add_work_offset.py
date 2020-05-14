#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import traceback
import redis

'''

redis新建hash表"k:offset_worker"  表示各个work进程自己的offset，第一次部署把旧的k:offset设置到该hash，index的offset同理

'''

redis_host = "127.0.0.1"
redis_port = 6386

def get_redis_client():
    return redis.StrictRedis(redis_host, redis_port)

def get_deals_offset():
    redis_client = get_redis_client()
    offset = redis_client.get("k:offset")
    print "deals_offset: %s" % offset
    return offset

def get_index_offset():
    redis_client = get_redis_client()
    offset = redis_client.get("k:offset_index")
    print "index_offset: %s" % offset
    return offset

def set_deals_offset():
    redis_client = get_redis_client()
    key = "k:offset_worker"
    offset = get_deals_offset()

    redis_client.hset(key, 0, offset)
    dict_value = redis_client.hgetall(key)
    print "key:%s val:%s" % (key, dict_value)

def set_index_offset():
    redis_client = get_redis_client()
    key = "k:offset_worker_index"
    offset = get_index_offset()

    redis_client.hset(key, 0, offset)
    dict_value = redis_client.hgetall(key)
    print "key:%s val:%s" % (key, dict_value)

def main():
    set_deals_offset()
    print ""
    set_index_offset()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())