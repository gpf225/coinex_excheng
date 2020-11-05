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

MYSQL_HOST = "192.168.0.95"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"

MYSQL_COINEX = "kline_coinex"
MYSQL_COINEX2 = "kline_coinex2"
MYSQL_BINANCE = "kline_binance"
MYSQL_HUOBI = "kline_huobi"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 6

api_coinex_markets = "http://127.0.0.1:8000/internal/exchange/market/list"
api_huobi_markets = "https://api.huobi.pro/v1/common/symbols"
api_binance_markets = "https://api.binance.com/api/v3/exchangeInfo"

insert_data = {}

def rpc(url, params):
    headers = {"Content-Type": "application/json"};
    r = requests.get(url, params=params, headers=headers).json()
    return r

def get_binance_markets():
    marketlist_binace = {}
    exchang_info = rpc(api_binance_markets, {})
    for market in exchang_info["symbols"]:
        if market['status'] == 'TRADING':
            marketlist_binace[market['symbol'].upper()] = market
    return marketlist_binace

def get_huobi_markets():
    markets_info = rpc(api_huobi_markets, {})
    if markets_info['status'] != 'ok':
        print("get market list error")
        return

    marketlist_huobi = {}
    for market in markets_info["data"]:
        if market['state'] == 'online':
            marketlist_huobi[market['symbol'].upper()] = market
    return marketlist_huobi

def get_coinex_markets():
    market_list = {}
    res = rpc(api_coinex_markets, {})
    for market_info in res['data']:
        market_list[market_info['name']] = market_info
    return market_list

def get_market_common(market_list1, market_list2):
    market_list = list(set(market_list1).intersection(set(market_list2)))
    return market_list

def get_day_kline(db_name, market_list, table_list):
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=db_name)
    cursor = db_conn.cursor()

    volume_list = {}
    for table in table_list:
        query_sql_str = "select `market`, `timestamp`, `volume` from {} where `t`=3".format(table)
        print(query_sql_str)
        try:
            cursor.execute(query_sql_str)
            res = cursor.fetchall()
        except Exception:
            print("exception: ",Exception)

        for item in res:
            market_name = item[0]
            if market_name not in market_list:
                continue

            timestamp = int(item[1])
            volume_list.setdefault(market_name, {})
            volume_list[market_name][timestamp] = decimal.Decimal(item[2])

    db_conn.close()
    return volume_list

def get_month(date, n):
    month = date.month
    year = date.year
    for i in range(n):
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
    return datetime.date(year, month, 1).strftime('%Y%m')


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

def laod_table(db_conn, redis_conn, table_name, market_list_coinex, market_list_common, now, proportion_avg, proportion_avg_all):
    limit = 100000
    offset = 0
    minute_start = now - 30 * 86400

    while True:
        query_sql_str = "select `market`, `timestamp`, `t`, `open`, `close`, `high`, `low`, `volume`, `deal` from {} where `t`!=1 order by id asc limit {}, {}".format(table_name, offset, limit)
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
            if market_name not in market_list_common:
                continue

            if int(item[2]) == 1 and (now - 30 * 86400) > int(item[2]):
                continue

            price_format = "%.{}f".format(int(market_list_coinex[item[0]]['money']['prec']))
            volume_format = "%.{}f".format(int(market_list_coinex[item[0]]['stock']['prec']))
            volume = decimal.Decimal(item[7])
            if market_name in proportion_avg:
                volume = volume / proportion_avg[market_name]
            else:
                volume = volume / proportion_avg_all

            kline.append(price_format % decimal.Decimal(item[3]))
            kline.append(price_format % item[4])
            kline.append(price_format % item[5])
            kline.append(price_format % item[6])
            #kline.append(volume_format % item[7])
            kline.append(volume_format % volume)
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

def kline_load(db_conn, redis_conn, market_list_coinex, market_list_common, proportion_avg, proportion_avg_all, table_list):
    query_table_str = "show tables like 'kline_history_2%'"
    cursor = db_conn.cursor()
    cursor.execute(query_table_str)
    res = cursor.fetchall()

    now = int(time.time())
    for item in res:
        table = item[0]
        if table in table_list:
            continue

        print(table)
        laod_table(db_conn, redis_conn, table, market_list_coinex, market_list_common, now, proportion_avg, proportion_avg_all)

    cursor.close()

    global insert_data
    for key, value in insert_data.items():
        if len(value) > 0:
            redis_conn.hmset(key, value)
            print(key)
            insert_data[key] = {}

def main():
    market_list_coinex = get_coinex_markets()
    market_list_huobi = get_huobi_markets()

    market_list_common = get_market_common(market_list_coinex.keys(), market_list_huobi.keys())

    date = datetime.datetime.today()
    table_list = []
    table_list.append("kline_history_{}".format(get_month(date, 2)))

    print("market list len: {}".format(len(market_list_common)))
    print(MYSQL_COINEX)
    coinex_volume_list = get_day_kline(MYSQL_COINEX, market_list_common, table_list)
    print(len(coinex_volume_list))

    print(MYSQL_HUOBI)
    huobi_volume_list = get_day_kline(MYSQL_HUOBI, market_list_common, table_list)
    print(len(huobi_volume_list))

    proportion = {}
    for market, volumes in coinex_volume_list.items():
        for day, volume in volumes.items():
            if (market not in huobi_volume_list):
                continue

            if day in huobi_volume_list[market]:
                proportion.setdefault(market, {})
                proportion[market][day] = huobi_volume_list[market][day] / volume
                print("market: {}, timestamp: {}, coinex: {}, huobi: {}, pro: {}".format(market, day, volume, huobi_volume_list[market][day], proportion[market][day]))

    proportion_avg = {}
    for market, props in proportion.items():
        proportion_avg.setdefault(market, 0)
        for day, prop in props.items():
            proportion_avg[market] += prop
        proportion_avg[market] = proportion_avg[market] / len(props)

    print(proportion_avg)

    proportion_avg_all = 0
    for market, prop in proportion_avg.items():
        proportion_avg_all += prop
    proportion_avg_all = proportion_avg_all / len(proportion_avg)
    print(proportion_avg_all)

    redis_conn = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_COINEX2)

    table_list = []
    table_list.append("kline_history_{}".format(get_month(date, 0)))
    table_list.append("kline_history_{}".format(get_month(date, 1)))
    kline_load(db_conn, redis_conn, market_list_coinex, market_list_common, proportion_avg, proportion_avg_all, table_list)

    db_conn.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())