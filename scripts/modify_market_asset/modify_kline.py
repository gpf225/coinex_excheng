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
from datetime import datetime
from datetime import timedelta
import pymysql

'''
REDIS_HOST = "server.jb1xx2.ng.0001.apne1.cache.amazonaws.com"
REDIS_PORT = 6379
REDIS_DB = 0

MYSQL_HOST = "coinexlog.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_PORT = 3306
MYSQL_USER = "coinex"
MYSQL_PASS = "hp1sXMJftZWPO5bQ2snu"
MYSQL_DB = "trade_log"
'''

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "trade_log"

SOURCE_MARKET = "SIPCUSDT"
TARGET_MARKET = "SIMPLEUSDT"
        
def rename_kline(redis_conn):
    key_1s = "k:{}:1s"
    redis_conn.rename(key_1s.format(SOURCE_MARKET), key_1s.format(TARGET_MARKET))
    key_1m = "k:{}:1m"
    redis_conn.rename(key_1m.format(SOURCE_MARKET), key_1m.format(TARGET_MARKET))
    key_1h = "k:{}:1h"
    redis_conn.rename(key_1h.format(SOURCE_MARKET), key_1h.format(TARGET_MARKET))
    key_1d = "k:{}:1d"
    redis_conn.rename(key_1d.format(SOURCE_MARKET), key_1d.format(TARGET_MARKET))

def rename_deals(redis_conn):
    key_deals = "k:{}:deals"
    redis_conn.rename(key_deals.format(SOURCE_MARKET), key_deals.format(TARGET_MARKET))
    key_real_deals = "k:{}:real_deals"
    redis_conn.rename(key_real_deals.format(SOURCE_MARKET), key_real_deals.format(TARGET_MARKET))

def rename_last(redis_conn):
    key_last = "k:{}:last"
    redis_conn.rename(key_last.format(SOURCE_MARKET), key_last.format(TARGET_MARKET))

def modify_kline_history(db_conn):
    query_table_str = "show tables like 'kline_history_2%'"
    cursor = db_conn.cursor()
    cursor.execute(query_table_str)
    res = cursor.fetchall()

    for item in res:
        table = item[0]
        sql = "update {} set market='{}' where market='{}'".format(table, TARGET_MARKET, SOURCE_MARKET)
        print(sql)
        cursor.execute(sql)
        db_conn.commit()

    cursor.close()
        


def main():
    redis_conn = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    rename_kline(redis_conn)
    rename_deals(redis_conn)
    rename_last(redis_conn)

    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)
    modify_kline_history(db_conn)
    db_conn.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())
