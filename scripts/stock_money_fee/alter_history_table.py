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

MYSQL_HOST = ["192.168.0.95", "192.168.0.95", "192.168.0.95", "192.168.0.95", "192.168.0.95"]
MYSQL_PORT = [3306, 3306, 3306, 3306, 3306]
MYSQL_USER = ["root", "root", "root", "root", "root"]
MYSQL_PASS = ["shit", "shit", "shit", "shit", "shit"]
MYSQL_DB = ["trade_history_0", "trade_history_1", "trade_history_2", "trade_history_3", "trade_history_4"]

'''
MYSQL_HOST= ["coinextradehistory0.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory1.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory2.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory3.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory4.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"]
MYSQL_PORT = [3306, 3306, 3306, 3306, 3306]
MYSQL_USER = ["root", "root", "root", "root", "root"]
MYSQL_PASS = ["6jh7QCaj4gX8QVx4T7j6", "7BG5CWFvPAOOdx99Gytn", "lsD9idvDE0b26W6V474M", "60yQcHSNB76PQtl7HvQA", "Hs7NMTIdG58Zk7sP68vD"]
MYSQL_DB = ["trade_history_0", "trade_history_1", "trade_history_2", "trade_history_3", "trade_history_4"]
'''

def add_cloumn(db_conn):
    cursor = db_conn.cursor()
    for i in range(100):
        alter_sql = "alter table order_history_{} ADD money_fee DECIMAL(40,20) NOT NULL DEFAULT 0;".format(i)
        print(alter_sql)
        cursor.execute(alter_sql)
        db_conn.commit()

        alter_sql = "alter table order_history_{} ADD stock_fee DECIMAL(40,20) NOT NULL DEFAULT 0;".format(i)
        print(alter_sql)
        cursor.execute(alter_sql)
        db_conn.commit()

    cursor.close()

def main():
    for i in range(5):
        print("history db: {}".format(i))
        db_conn = pymysql.connect(host=MYSQL_HOST[i], port=MYSQL_PORT[i], user=MYSQL_USER[i], passwd=MYSQL_PASS[i], db=MYSQL_DB[i])
        add_cloumn(db_conn)
        db_conn.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())

