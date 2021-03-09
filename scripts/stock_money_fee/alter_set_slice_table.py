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

def get_table(db_conn):
    query_table_str = "select time from slice_history order by time desc limit 1"
    cursor = db_conn.cursor()
    cursor.execute(query_table_str)
    res = cursor.fetchall()
    return "slice_order_{}".format(res[0][0])
    #return "slice_order_1614067819"

def add_column(db_conn, table):
    cursor = db_conn.cursor()
    alter_sql = "alter table {} ADD money_fee DECIMAL(40,24) NOT NULL DEFAULT 0;".format(table)
    print(alter_sql)
    cursor.execute(alter_sql)
    db_conn.commit()

    alter_sql = "alter table {} ADD stock_fee DECIMAL(40,24) NOT NULL DEFAULT 0;".format(table)
    print(alter_sql)
    cursor.execute(alter_sql)
    db_conn.commit()

    alter_sql = "alter table slice_order_example ADD money_fee DECIMAL(40,24) NOT NULL DEFAULT 0;"
    print(alter_sql)
    cursor.execute(alter_sql)
    db_conn.commit()

    alter_sql = "alter table slice_order_example ADD stock_fee DECIMAL(40,24) NOT NULL DEFAULT 0;"
    print(alter_sql)
    cursor.execute(alter_sql)
    db_conn.commit()
    cursor.close()

def del_column(db_conn):
    cursor = db_conn.cursor()
    alter_sql = "alter table slice_order_example drop column deal_fee;"
    print(alter_sql)
    cursor.execute(alter_sql)
    db_conn.commit()
    cursor.close()

def update_table(db_conn, table):
    cursor = db_conn.cursor()
    update_sql = "update {} set money_fee=deal_fee where side=1 and deal_fee>0;".format(table)
    print(update_sql)
    cursor.execute(update_sql)
    db_conn.commit()

    update_sql = "update {} set stock_fee=deal_fee where side=2 and deal_fee>0;".format(table)
    print(update_sql)
    cursor.execute(update_sql)
    db_conn.commit()
    
    cursor.close()

def update_tables(db_conn, tables):
    for item in tables:
        table = item[0]
        update_table(db_conn, table)

def main():
    db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)
    table = get_table(db_conn)
    add_column(db_conn, table)
    update_table(db_conn, table)
    del_column(db_conn)
    db_conn.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())
