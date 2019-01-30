#!/usr/bin/python
# -*- coding: UTF-8 -*-

import datetime
import os
import sys
import time
import MySQLdb

'''
删除trade_log过期表文件分两步执行：
步骤1：执行./trim_trade_log_tables.py check。这一步检查需要删除的数据库表，并在data目录保存需要删除的数据库表名以及对应的删除sql语句。
  执行完步骤1以后可以仔细检查下需要删除的表是否正确。

步骤2: 执行./trim_trade_log_tables.py drop [table suffix]。执行步骤1生成的sql语句。支持批量删除和选择性删除。
执行./trim_trade_log_tables.py drop删除步骤1检查出的所有表。
执行./trim_trade_log_tables.py drop slice_balance 删除步骤1检查出的所有以slice_balance为前缀的表。
'''

last_time = 0
# lice_keeptime_seconds = 259200
# 7表示保留7最近7天的表
slice_keeptime_seconds = int(86400 * 7)

def connect_db():
    db_host = "localhost"
    db_user = "root"
    db_pwd = "shit"
    db_name = "trade_log"

    return MySQLdb.connect(db_host, db_user, db_pwd, db_name, charset='utf8')

def export_drop_list_to_file(suffix, drop_list):
    file_name = "data/{}.dat".format(suffix)
    file = open(file_name, 'w+')
    for line in drop_list:
        file.write(line + '\n')

    file.close()

def generate_drop_sql(suffix, drop_list):
    drop_sql = "drop table "
    for line in drop_list:
        drop_sql = drop_sql + line + ", "
    
    drop_sql = drop_sql[0 : len(drop_sql)-2] + ";\n"

    file_name = "data/{}_drop.sql".format(suffix)
    file = open(file_name, 'w+')
    file.write(drop_sql)
    file.close()

def is_expired(suffix, table_name):
    index = table_name.find(suffix)
    if index == -1:
        print "table:%s invalid" % table_name
        return False

    table_time = table_name[index + len(suffix) + 1: ]
    if table_time == "example":
        #print "%s_example exclusive" % suffix
        return False

    if long(table_time) > long(last_time):
        #print "no need to drop table %s, table_time:%s last_time:%s diff:%s" % (table_name, table_time, last_time, long(table_time) - long(last_time))
        return False
    return True

def fetch_expired_tables(suffix):
    db = connect_db()
    cursor = db.cursor()
    
    drop_list = []
    cursor.execute("show tables like '{}_%'".format(suffix))
    results = cursor.fetchall()
    for row in results:
        if is_expired(suffix, row[0]):
            drop_list.append(row[0])
    
    if (len(drop_list) == 0):
        print "%s has no expired tables" % suffix
        return 

    print "%s expired tables:%s first:%s last:%s" % (suffix, len(drop_list), drop_list[0], drop_list[len(drop_list)-1]) 
    drop_list.sort(reverse=False)
    export_drop_list_to_file(suffix, drop_list)
    generate_drop_sql(suffix, drop_list)

    db.close()

def get_trim_last_time():
    cur_seconds = int(time.time())

    sql = "SELECT `id`, `time` FROM `slice_history` WHERE `time` < %s ORDER BY `id` DESC limit 1" % (cur_seconds - slice_keeptime_seconds)
    print "sql:", sql
    
    global last_time
    db = connect_db()
    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()
    if result is None:
        print 'could not get last time'
        last_time = cur_seconds - slice_keeptime_seconds
        print 'last_time:', last_time
        return 

    last_time = int(result[1])
    print 'last_time:', last_time

    db.close()

def check_drop_tables():
    get_trim_last_time()

    if not os.path.exists("data"):
        os.mkdir("data")

    fetch_expired_tables('slice_balance') 
    fetch_expired_tables('slice_order') 
    fetch_expired_tables('slice_stop') 
    fetch_expired_tables('slice_update') 

def drop_table(suffix):
    file_name = "data/{}_drop.sql".format(suffix)
    file = None
    try:
        file = open(file_name, 'r')
    except IOError, error:
        print 'IOError:', error
        return 

    sql = file.read();
    file.close()
    print "drop %s sql:", (suffix, sql)

    print "drop..., please wait a moment"
    db = connect_db()
    cursor = db.cursor()

    try:
        cursor.execute(sql)
        print "drop completed!!!"
    except MySQLdb.OperationalError, error:
        print 'OperationalError:', error
    db.close()

def execute_drop(suffix):
    print "drop_table drop:", suffix

    if suffix == "all":
        drop_table('slice_balance') 
        drop_table('slice_order') 
        drop_table('slice_stop') 
        drop_table('slice_update') 
    else:
        drop_table(suffix)

def main_fun():
    if __name__ != "__main__":
        print "not execute as main"
        return 

    if len(sys.argv) < 2:
        print "invalid params"
        return 

    command = sys.argv[1]
    print "command:", command

    if command == "check":
        print '---check---'
        check_drop_tables()
        return ;
    elif command == "drop":
        print "---drop---" 
        suffix = "all"
        if  len(sys.argv) == 3:
            suffix = sys.argv[2]
        execute_drop(suffix)
    else :
        print "invalid command:", command 

main_fun()