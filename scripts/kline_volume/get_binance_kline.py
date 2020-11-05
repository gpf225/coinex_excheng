#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import json
import requests
import pymysql
from multiprocessing import Process

api_coinex_markets = "https://api.coinex.com/v1/market/list"

api_binance_markets = "https://api.binance.com/api/v3/exchangeInfo"
api_binance_kline = "https://api.binance.com/api/v3/klines"

process_count = 10
start_time = 1513900800 #2017-12-22 08:00:00

MYSQL_HOST = "192.168.0.95"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "kline_binance"

sql_data = {}
curr_table = ''

def rpc(url, params):
    headers = {"Content-Type": "application/json"};
    r = requests.get(url, params=params, headers=headers).json()
    return r

def get_coinex_markets():
    markets = rpc(api_coinex_markets, {})
    if markets['code'] != 0:
        return
    return markets['data']

def get_binance_markets():
    marketlist_binace = {}
    exchang_info = rpc(api_binance_markets, {})
    for market in exchang_info["symbols"]:
        if market['status'] == 'TRADING':
            marketlist_binace[market['symbol'].upper()] = market
    return marketlist_binace

def get_market_common(markets_coinex, markets_binance):
    market_list = list(set(markets_coinex).intersection(set(markets_binance)))
    return market_list

def get_market_diff(markets_coinex, markets_binance):
    market_list = list(set(markets_coinex).difference(set(markets_binance)))
    return market_list

def create_table(db_conn, table):
    cursor = db_conn.cursor()
    sql = "CREATE TABLE IF NOT EXISTS `{}` like `kline_history_example`".format(table)
    cursor.execute(sql)
    cursor.close()

def db_execute(db_conn, table, insert_data):
    create_table(db_conn, table)

    sql = "INSERT INTO `{}`(`timestamp`, `open`, `high`, `low`, `close`, `volume`, `deal`, `market`, `t`)VALUES".format(table)
    count = 0
    for item in insert_data:
        if count > 0:
            sql = sql + ','
        sql = sql + "({0[0]}, '{0[1]}', '{0[2]}', '{0[3]}', '{0[4]}', '{0[5]}', '{0[6]}', '{0[7]}', {0[8]})".format(item)
        count += 1

    print(table)
    cursor = db_conn.cursor()
    cursor.execute(sql)
    db_conn.commit()
    cursor.close()
    time.sleep(0.2)

def insert_to_db(market, kline_class, db_conn, kline_data):
    global sql_data

    local_time = time.localtime(kline_data[0])
    table_suffix = time.strftime("%Y%m", local_time)
    table = 'kline_history_{}'.format(table_suffix)

    sql_data.setdefault(table, [])
    kline_data.append(market)
    kline_data.append(kline_class)
    sql_data[table].append(kline_data)

    if len(sql_data[table]) > 1000:
        db_execute(db_conn, table, sql_data[table])
        sql_data[table] = []

def get_kline_min(market, db_conn, end_time):
    start = end_time - 60 * 60 * 24 * 30
    count = 0
    while True:
        end = start + 1000 * 60 - 60
        if end > end_time:
            end = end_time

        params = {
            "symbol": market,
            "interval": "1m",
            "startTime": start * 1000,
            "endTime": end * 1000,
            "limit": 1000  # max: 1000 default: 500
        }

        klines = rpc(api_binance_kline, params)
        count += len(klines)
        for item in klines:
            #print(item)
            insert_to_db(market, 1, db_conn, [int(item[0] / 1000), item[1], item[2], item[3], item[4], item[5], item[7]])

        start = end + 60
        if start > end_time:
            break

    global sql_data
    for table, kline_data in sql_data.items():
        if len(kline_data) > 0:
            db_execute(db_conn, table, kline_data)
            sql_data[table] = []

    print("min kline, market: {}, count: {}".format(market, count))

def get_kline_hour(market, db_conn, end_time):
    start = end_time - 60 * 60 * 24 * 365 * 3
    if start < start_time:
        start = start_time

    count = 0
    while True:
        end = start + 1000 * 3600 - 3600
        if end > end_time:
            end = end_time

        params = {
            "symbol": market,
            "interval": "1h",
            "startTime": start * 1000,
            "endTime": end * 1000,
            "limit": 1000  # max: 1000 default: 500
        }

        klines = rpc(api_binance_kline, params)
        count += len(klines)
        for item in klines:
            #print(item)
            insert_to_db(market, 2, db_conn, [int(item[0] / 1000), item[1], item[2], item[3], item[4], item[5], item[7]])

        start = end + 3600
        if start > end_time:
            break

    global sql_data
    for table, kline_data in sql_data.items():
        if len(kline_data) > 0:
            db_execute(db_conn, table, kline_data)
            sql_data[table] = []

    print("hour kline, market: {}, count: {}".format(market, count))

def get_kline_day(market, db_conn, end_time):
    start = start_time
    count = 0
    while True:
        end = start + 1000 * 86400 - 86400
        if end > end_time:
            end = end_time

        params = {
            "symbol": market,
            "interval": "1d",
            "startTime": start * 1000,
            "endTime": end * 1000,
            "limit": 1000  # max: 1000 default: 500
        }

        print(params)
        klines = rpc(api_binance_kline, params)
        #print(klines)
        #print(len(klines))

        count += len(klines)
        for item in klines:
            insert_to_db(market, 3, db_conn, [int(item[0] / 1000), item[1], item[2], item[3], item[4], item[5], item[7]])

        start = end + 86400
        if start > end_time:
            break

    global sql_data
    for table, kline_data in sql_data.items():
        if len(kline_data) > 0:
            db_execute(db_conn, table, kline_data)
            sql_data[table] = []

    print("day kline, market: {}, count: {}".format(market, count))

def get_kline(market, db_conn):
    now = int(time.time())
    get_kline_min(market, db_conn, now)
    get_kline_hour(market, db_conn, now)
    get_kline_day(market, db_conn, now)

def get_binance_klines(markets, process_id):
    print("process_id: {}, market list: {}, len: {}".format(process_id, markets, len(markets)))
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)
    for market in markets:
        get_kline(market, db_conn)

def main():
    markets_binance = get_binance_markets()
    markets_binance_list = markets_binance.keys()
    markets_coinex = get_coinex_markets()

    #get_binance_klines(['BTCUSDT'], 1)
    
    markets_list = get_market_common(markets_coinex, markets_binance_list)
    print(len(markets_list))
    print("market_list:", markets_list)
    start = 0;
    step = int(len(markets_list) / process_count + 1)
    processlist = []
    for i in range(process_count):
        end = start + step
        if end > len(markets_list):
            end = len(markets_list)
        #process = Process(target=get_binance_klines, args=(markets_list[start:end], i))
        #markets_list = ['BTCUSDT']
        process = Process(target=get_binance_klines, args=(markets_list[start:end], i))
        start += step
        processlist.append(process)

    for process in processlist:
        process.start()
    for process in processlist:
        process.join()

    print("over")

if __name__ == '__main__':
    main()





'''
K线间隔:

mysqldump kline_coinex -u root -pshit --add-drop-table | mysql kline_coinex2 -u root -pshit

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月
1m
3m
5m
15m
30m
1h
2h
4h
6h
8h
12h
1d
3d
1w
1M
'''
