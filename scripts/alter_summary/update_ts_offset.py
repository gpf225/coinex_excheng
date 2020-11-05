#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
在mysql dump_history表中新增的deals_offset orders_offset 设置起始值
"""
import os
import sys
import redis
import json
import time
import MySQLdb

redis_host = "127.0.0.1"
redis_port = 6386

db_host = "localhost"
db_user = "root"
db_pwd = "shit"
db_name = "trade_summary"

def get_mysql_conn():
    return MySQLdb.connect(db_host, db_user, db_pwd, db_name, charset='utf8')

def get_redis_client():
    return redis.StrictRedis(redis_host, redis_port)

def update_offset(redis_conn, mysql_conn):
    now = int(time.time())
    end = now / 3600 * 3600
    start = end - 3 * 86400;
    while start < end :
        order_offset = redis_conn.hget("s:offset:orders", start)
        deal_offset = redis_conn.hget("s:offset:deals", start)
        print(start)
        if (order_offset is None) or (deal_offset is None) :
            start = start + 3600
            continue

        sql = "insert into `dump_history`(`time`, `trade_date`, `deals_offset`, `orders_offset`)values(%d, '1970-01-01', %d, %d)" % (now, int(deal_offset), int(order_offset))
        print "sql:", sql

        cursor = mysql_conn.cursor()
        cursor.execute(sql)
        mysql_conn.commit()
        break


def main():
    redis_conn = get_redis_client()
    mysql_conn = get_mysql_conn()
    update_offset(redis_conn, mysql_conn)

if __name__ == '__main__':
    main()
