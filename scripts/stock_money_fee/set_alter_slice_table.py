#encoding: utf-8
import os
import sys
import pymysql
import traceback
import decimal
import json
import time
import datetime

MYSQL_HOST = "192.168.0.95"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "trade_log"

'''
MYSQL_HOST = "coinexlog.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_USER = "coinex"
MYSQL_PASS = "hp1sXMJftZWPO5bQ2snu"
MYSQL_DB = "trade_log"
MYSQL_PORT = 3306
'''

def get_tables(db_conn):
    query_table_str = "show tables like 'slice_order_%'"
    cursor = db_conn.cursor()
    cursor.execute(query_table_str)
    res = cursor.fetchall()
    return res

def add_cloumn(db_conn, tables):
    cursor = db_conn.cursor()
    for item in tables:
        table = item[0]
        print(table)

        alter_sql = "alter table {} ADD money_fee DECIMAL(40,24) NOT NULL DEFAULT 0;".format(table)
        print(alter_sql)
        cursor.execute(alter_sql)
        db_conn.commit()

        alter_sql = "alter table {} ADD stock_fee DECIMAL(40,24) NOT NULL DEFAULT 0;".format(table)
        print(alter_sql)
        cursor.execute(alter_sql)
        db_conn.commit()
    cursor.close()

def update_table(db_conn, table):
    query_limit = 10000
    query_offset = 0
    cursor = db_conn.cursor()
    while True:
        query_sql = "select id, option, side, deal_fee from {} order id asc limit {}, {}".format(table, query_offset, query_limit)
    cursor.close()

def update_tables(db_conn, tables):
    for item in tables:
        table = item[0]
        update_table(db_conn, table)

def main():
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)
    tables = get_tables(db_conn)
    add_cloumn(db_conn, tables)
    db_conn.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())
