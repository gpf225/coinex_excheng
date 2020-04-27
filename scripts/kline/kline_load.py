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

MARKET_INDEX_URL = "http://internal-web-internal-872360093.ap-northeast-1.elb.amazonaws.com/internal/exchange/market/index/config"
MARKET_LIST_URL = "http://internal-web-internal-872360093.ap-northeast-1.elb.amazonaws.com//internal/exchange/market/list"
'''

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "trade_log"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0

MARKET_INDEX_URL = "http://127.0.0.1:8000/internal/exchange/market/index/config"
MARKET_LIST_URL = "http://127.0.0.1:8000/internal/exchange/market/list"

insert_data = {}

def flush_kline(redis_conn, key, subkey, kline_data, is_end):
    global insert_data
    insert_data.setdefault(key, {})
    insert_data[key][subkey] = json.dumps(kline_data)
    if len(insert_data[key]) > 100:
        redis_conn.hmset(key, insert_data[key])
        print(insert_data[key])
        insert_data[key] = {}

def get_redis_key(market, kline_class):
    if kline_class == 1;
        return "k:{}:1m".format(market)
    elif kline_class == 2:
        return "k:{}:1h".format(market)
    elif kline_class == 3:
        return "k:{}:1d".format(market)

def get_next_month_first_day(year_month):
    datetime.datetime.strptime(year_month, "%Y%m")
    month = date.month + 1
    year = date.year + month / 12
    month = month % 12 + 1
    return "%d%02d" % (year, month)

def kline_load(db_conn, redis_conn, market_list, market_index):
    table_prefix = 'kline_history'
    table_start = '201904'
    table_end   = time.strftime('%Y%m', time.localtime())

    limit = 5000
    offset = 0

    while True:
        query_sql_str = "select market, timestamp, class, open, close, high, low, volume, deal from {}_{} order by id asc limit {}, {}".format(table_prefix, table_start, offset, limit)
        print(query_sql_str)

        res = {}
        try:
            cursor.execute(query_sql_str)
            res = cursor.fetchall()
        except Exception:
            print("exception: ",Exception)
                        
        for item in res:
            kline = []
            if item[0] in market_list:
                price_format = "%.{}f".format(int(market_list[item[0]]['money']['prec']))
                volume_format = "%.{}f".format(int(market_list[item[0]]['stock']['prec']))
                kline.append(price_format % decimal.Decimal(item[3]))
                kline.append(price_format % item[4])
                kline.append(price_format % item[5])
                kline.append(price_format % item[6])
                kline.append(volume_format % item[7])
                kline.append(price_format % item[8])
            elif item[0] in market_index:
                price_format = "%.{}f".format(int(market_index[item[0]]['money']['prec']))
                kline.append(price_format % decimal.Decimal(item[3]))
                kline.append(price_format % item[4])
                kline.append(price_format % item[5])
                kline.append(price_format % item[6])
                kline.append("0")
                kline.append("0")
            else:
                continue

            key = get_redis_key(item[0], int(item[2]))
            flush_kline(redis_conn, key, int(item[1]), kline)

        if len(res) < limit:
            table_start = get_next_month_first_day(table_start)
            offset = 0
        else:
            offset = offset + limit

        if int(table_start) >= int(time_end):
            break

    cursor.close()

    global insert_data
    for key, value in insert_data.items():
        if len(value) > 0:
            redis_conn.hmset(key, values)
            print(value)
            insert_data[key] = {}


def main():
    redis_conn = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)

    market_list = {}
    res = requests.get(MARKET_LIST_URL, timeout=5).json()
    for market_info in res['data']:
        market_list[market_info['name']] = market_info

    market_index = {}
    res = requests.get(MARKET_INDEX_URL, timeout=5).json()
    for market, market_info in res['data'].items():
        index_name = market + "_INDEX"
        market_index[index_name] = market_info

    kline_load(db_conn, redis_conn, market_list, market_index)

    db_conn.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())
