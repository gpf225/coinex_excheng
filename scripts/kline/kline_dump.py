#encoding: utf-8
import os
import sys
import pymysql
import traceback
import decimal
import json
import time
import requests
import math
from redis import StrictRedis

'''
MYSQL_HOST = "coinexlog.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_PORT = 3306
MYSQL_USER = "coinex"
MYSQL_PASS = "hp1sXMJftZWPO5bQ2snu"
MYSQL_DB = "trade_log"

REDIS_HOST = "server.jb1xx2.ng.0001.apne1.cache.amazonaws.com"
REDIS_PORT = 6379
REDIS_DB = 0
'''

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "trade_log"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0

MARKET_URL = "http://127.0.0.1:8000/internal/exchange/market/index/config"

sql_data = {}

def create_table(db_conn, table):
    cursor = conn.cursor()
    sql = "CREATE TABLE IF NOT EXISTS `%s` like `kline_history_example`".format(table)
    cursor.execute(sql)
    cursor.close()

def db_execute(db_conn, table, insert_data):
    create_table(db_conn, table)
    sql = "INSERT INTO `{}`(`open`, `close`, `high`, `low`, `volume`, `deal`, `market`, `class`, `timestamp`)VALUES".format(table)
    count = 0
    for item in insert_data:
        if count > 0:
            sql = ',' + sql
        sql = sql + "('{0[0]}', '{0[1]}', '{0[2]}', '{0[3]}', '{0[4]}', '{0[5]}', '{0[6]}', {0[7]}, {0[8]})".format(item)
        count += 1

    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()

def flush_db(db_conn, kline_class, timestamp, kline_data):
    global sql_data

    local_time = time.localtime(table_start) 
    table_suffix = time.strftime("%Y%m", local_time)
    table = 'kline_history_{}'.format(table_suffix)

    sql_data.setdefault(table, [])
    kline_data.append(kline_class)
    kline_data.append(timestamp)
    sql_data[table].append(kline_data)

    if len(sql_data[table]) > 100:
        db_execute(db_conn, table, sql_data[table]):
        

def dump_kline(db_conn, redis_conn):
    for redis_key in redis_conn.scan_iter('k:*:1m'):
        data = redis_conn.hgetall(redis_key)
        for timestamp, value in data.items():
            items = redis_key.split(':')
            market = items[1]
            kline_class = 1
            kline_data = json.loads(value)
            flush_db(db_conn, kline_class, timestamp, kline_data)

    for redis_key in redis_conn.scan_iter('k:*:1h'):
        data = redis_conn.hgetall(redis_key)
        for timestamp, value in data.items():
            items = redis_key.split(':')
            market = items[1]
            kline_class = 2
            kline_data = json.loads(value)
            flush_db(db_conn, kline_class, timestamp, kline_data)

    for redis_key in redis_conn.scan_iter('k:*:1d'):
        data = redis_conn.hgetall(redis_key)
        for timestamp, value in data.items():
            items = redis_key.split(':')
            market = items[1]
            kline_class = 3
            kline_data = json.loads(value)
            flush_db(db_conn, kline_class, timestamp, kline_data)

    global sql_data
    for table, kline_data in sql_data:
        if len(kline_data) > 0:
            db_execute(db_conn, table, kline_data):
        
def main():
    redis_conn = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)

    dump_kline(db_conn, redis_conn)

    db_conn.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())
