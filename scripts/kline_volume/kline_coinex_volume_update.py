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
from multiprocessing import Process

MYSQL_HOST = "192.168.0.95"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"

MYSQL_COINEX = "kline_coinex"
MYSQL_HUOBI = "kline_huobi"

process_count = 20

def rpc(url, params):
    headers = {"Content-Type": "application/json"};
    r = requests.get(url, params=params, headers=headers).json()
    return r

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

def laod_table(db_coinex, db_huobi, table):
    limit = 100000
    offset = 0

    cursor_coinex = db_coinex.cursor()
    cursor_huobi = db_huobi.cursor()
    while True:
        query_sql_str = "select `market`, `timestamp`, `t`, `open`, `close`, `high`, `low`, `volume`, `deal` from {} where `t`!=1 order by id asc limit {}, {}".format(table, offset, limit)
        print(query_sql_str)

        res = {}
        try:
            cursor_coinex.execute(query_sql_str)     
            res = cursor_coinex.fetchall()
        except Exception:
            print("exception: ",Exception)
        
        if len(res) > 0:              
            for item in res:
                kline = []
                market = item[0]
                timestamp = int(item[1])
                t = int(item[2])

                update_sql_str = "update {} set `open`='{}', `close`='{}', `high`='{}', `low`='{}' where `market`='{}' and `t`={} and `timestamp`={}".format(table, item[3], item[4], item[5], item[6], market, t, timestamp)
                #print(update_sql_str)
                cursor_huobi.execute(update_sql_str)
        db_huobi.commit()

        if len(res) < limit:
            break
        else:
            offset = offset + limit

    cursor_coinex.close()
    cursor_huobi.close()


def kline_update_volume(table_list, process_id):
    print("process_id: {}, table_list: {}".format(process_id, table_list))
    

    db_coinex = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_COINEX)
    db_huobi = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_HUOBI)

    now = int(time.time())
    for table in table_list:
        laod_table(db_coinex, db_huobi, table)


def main():
    date = datetime.datetime.today()
    table_list = []
    for i in range(3, 36):
        table_list.append("kline_history_{}".format(get_month(date, i)))

    print(table_list)
    start = 0;
    step = int(int(len(table_list)) / process_count + 1)
    processlist = []
    for i in range(process_count):
        end = start + step
        if end > len(table_list):
            end = len(table_list)
        process = Process(target=kline_update_volume, args=(table_list[start:end], i))
        start += step
        processlist.append(process)

        if end == len(table_list):
            break

    for process in processlist:
        process.start()

    for process in processlist:
        process.join()

    print("over")

if __name__ == '__main__':
    main()