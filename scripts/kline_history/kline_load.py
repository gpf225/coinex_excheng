#encoding: utf-8
import os
import sys
import pymysql
import traceback
import decimal
import json
import time
import datetime
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

MARKET_LIST_URL = "http://internal-web-internal-872360093.ap-northeast-1.elb.amazonaws.com//internal/exchange/market/list"
'''

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
#MYSQL_DB = "trade_log"
MYSQL_DB = "test_db"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 15

MARKET_LIST_URL = "http://127.0.0.1:8000/internal/exchange/market/list"

insert_data = {}

def flush_kline(redis_conn, key, subkey, kline_class, kline_data):
    global min_max
    global hour_max
    timestamp = subkey
    now = int(time.time())

    min_clear_tm = now / 60 * 60 - 60 * 24 * 30 * 60
    hour_clear_tm = now / 3600 * 3600 - 24 * 365 * 3 * 3600
    
    if kline_class == 1 and timestamp < min_clear_tm:
        return
    elif kline_class == 2 and timestamp < hour_clear_tm:
        return

    global insert_data
    insert_data.setdefault(key, {})
    insert_data[key][subkey] = json.dumps(kline_data)
    if len(insert_data[key]) > 1000:
        redis_conn.hmset(key, insert_data[key])
        print(key)
        insert_data[key] = {}

def get_redis_key(market, kline_class):
    if kline_class == 1:
        return "k:{}:1m".format(market)
    elif kline_class == 2:
        return "k:{}:1h".format(market)
    elif kline_class == 3:
        return "k:{}:1d".format(market)

def laod_table(db_conn, redis_conn, table_name, market_list):
    limit = 5000
    offset = 0
    
    while True:
        query_sql_str = "select `market`, `timestamp`, `t`, `open`, `close`, `high`, `low`, `volume`, `deal` from {} order by id asc limit {}, {}".format(table_name, offset, limit)
        cursor = db_conn.cursor()
        print(query_sql_str)

        res = {}
        try:
            cursor.execute(query_sql_str)
            res = cursor.fetchall()
        except Exception:
            print("exception: ",Exception)
                        
        for item in res:
            kline = []
            market_name = item[0]
            if market_name not in market_list:
                continue

            price_format = "%.{}f".format(int(market_list[item[0]]['money']['prec']))
            volume_format = "%.{}f".format(int(market_list[item[0]]['stock']['prec']))
            kline.append(price_format % decimal.Decimal(item[3]))
            kline.append(price_format % item[4])
            kline.append(price_format % item[5])
            kline.append(price_format % item[6])
            kline.append(volume_format % item[7])
            kline.append(price_format % item[8])

            key = get_redis_key(item[0], int(item[2]))
            flush_kline(redis_conn, key, int(item[1]), int(item[2]), kline)

        if len(res) < limit:
            offset = 0
            break
        else:
            offset = offset + limit

        cursor.close()
        if offset == 0:
            break

def kline_load(db_conn, redis_conn, market_list):
    query_table_str = "show tables like 'kline_history_2%'"
    cursor = db_conn.cursor()
    cursor.execute(query_table_str)
    res = cursor.fetchall()

    for item in res:
        table = item[0]
        print table
        laod_table(db_conn, redis_conn, table, market_list)
    cursor.close()

    global insert_data
    for key, value in insert_data.items():
        if len(value) > 0:
            redis_conn.hmset(key, value)
            print(key)
            insert_data[key] = {}

def main():
    redis_conn = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)

    market_list = {}
    res = requests.get(MARKET_LIST_URL, timeout=5).json()
    for market_info in res['data']:
        market_list[market_info['name']] = market_info

    kline_load(db_conn, redis_conn, market_list)

    db_conn.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())