#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import json
import requests
import websocket
import traceback
import ssl
import StringIO, gzip
import pymysql
from multiprocessing import Process

api_coinex_markets = "https://api.coinex.com/v1/market/list"
api_huobi_markets = "https://api.huobi.pro/v1/common/symbols"
ws_huobi = "wss://api.huobi.pro/ws"

MYSQL_HOST = "192.168.0.95"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "kline_huobi"

process_market = ''
process_kline_type = 1
process_end_time = 0
process_start_time = 1513900800 #2017-12-22 08:00:00
process_curr_time = 0
db_conn = None

sql_data = {}
curr_table = ''

def rpc(url, params):
    headers = {"Content-Type": "application/json"};
    r = requests.get(url, params=params, headers=headers).json()
    return r

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
    markets = rpc(api_coinex_markets, {})
    if markets['code'] != 0:
        return
    return markets['data']

def get_market_common(markets_coinex, markets_huobi):
    market_list = list(set(markets_coinex).intersection(set(markets_huobi)))
    return market_list

def get_market_diff(markets_coinex, markets_huobi):
    market_list = list(set(markets_coinex).difference(set(markets_huobi)))
    return market_list

def on_error(ws, error):
    print("on_error:{}".format(error))
 
def on_close(ws):
    global db_conn
    db_conn.close()
    print("on_close:", time.time())

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

def process_kline(klines):
    global process_kline_type
    global process_market
    global db_conn

    for kline in klines:
        timestamp = int(kline['id'])
        if process_kline_type == 1:
            timestamp = timestamp / 60 * 60
        elif process_kline_type == 2:
            timestamp = timestamp / 3600 * 3600
        elif process_kline_type == 3:
            timestamp = timestamp / 86400 * 86400
        insert_to_db(process_market, process_kline_type, db_conn, [timestamp, kline['open'], kline['high'], kline['low'], kline['close'], kline['amount'], kline['vol']])

def flush_db():
    global sql_data
    for table, kline_data in sql_data.items():
        if len(kline_data) > 0:
            db_execute(db_conn, table, kline_data)
            sql_data[table] = []

def kline_query(ws):
    global process_curr_time
    global process_start_time
    global process_end_time
    global process_kline_type
    global process_market

    if process_curr_time == 0:
        process_curr_time = process_end_time

    if process_curr_time <= process_start_time:
        flush_db()
        print("kline_query over")
        ws.close()
        return

    start = 0
    kline_type = '1min'
    if process_kline_type == 1:
        kline_type = '1min'
        start = process_curr_time - 290 * 60

    if process_kline_type == 2:
        kline_type = '60min'
        start = process_curr_time - 290 * 3600

    if process_kline_type == 3:
        kline_type = '1day'
        start = process_curr_time - 290 * 86400

    if start < process_start_time:
        start = process_start_time

    params = {
        "id": int(time.time()),
        "req": "market.{}.kline.{}".format(process_market.lower(), kline_type),
        "from": start,
        "to": process_curr_time #max 300
    }
    print("send params:", params)
    ws.send(json.dumps(params))
    process_curr_time = start
 
def on_message(ws, message):
    print(time.time())
    compressedstream = StringIO.StringIO(message)
    gziper = gzip.GzipFile(fileobj=compressedstream)
    response = gziper.read()
    response = json.loads(response)

    if response.has_key("pong"):
        kline_query(ws)
        return
    
    print("on_message:::")
    print(response)
    time.sleep(0.5)
    if response.has_key("status") and response['status'] == 'ok':
        if (len(response['data']) == 0):
            flush_db()
            print("kline_query over")
            ws.close()
            return

        process_kline(response['data'])
        kline_query(ws)
    else:
        time.sleep(0.5)
        kline_query(ws)
    

def on_open(ws):
    params = {"ping": 1492420473027}
    ws.send(json.dumps(params))

def get_huobi_kline(market, kline_type, process_id):
    global process_market
    process_market = market

    print("process_id: {}, market: {}".format(process_id, market))

    global db_conn
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)

    global process_end_time
    process_end_time = int(time.time()) / 86400 * 86400
    
    global process_kline_type
    process_kline_type = kline_type

    global process_start_time
    if process_kline_type == 1:
        process_start_time = process_end_time - 30 * 86400

    try:
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            ws_huobi,
            header={"X-Forwarded-For": "159.65.88.191"},
            on_message = on_message,
            on_error = on_error,
            on_close = on_close,
            on_open = on_open
        )
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False})

    except Exception as ex:
        print("Exception:{}".format(ex))
        traceback.print_exc()


def main():
    markets_huobi = get_huobi_markets()
    markets_huobi_list = markets_huobi.keys()
    markets_coinex = get_coinex_markets()

    markets_list = get_market_common(markets_coinex, markets_huobi_list)
    print(len(markets_list))
    print("market_list:", markets_list)

    start = 0
    step = 2
    while start < len(markets_list):
        end = start + step
        if end > len(markets_list):
            end = len(markets_list)

        processlist = []
        process_id = 0
        for market in markets_list[start:end]:
            process = Process(target=get_huobi_kline, args=(market, 1, process_id))
            processlist.append(process)
            process_id += 1

            process = Process(target=get_huobi_kline, args=(market, 2, process_id))
            processlist.append(process)
            process_id += 1

            process = Process(target=get_huobi_kline, args=(market, 3, process_id))
            processlist.append(process)
            process_id += 1
            

        for process in processlist:
            process.start()
        for process in processlist:
            process.join()

        start = end

    print("over")

if __name__ == '__main__':
    main()