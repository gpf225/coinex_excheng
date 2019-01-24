#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import urllib2
import urllib
import json
import os
import sys

db_host = "localhost"
db_user = "root"
db_pwd = "shit"
db_name = "trade_log"

asset = "CET"
access_http_url = "http://127.0.0.1:8080/"

def parse_asset(json_str):
    json_obj = json.loads(json_str)
    if json_str is None:
        print "json_str:%s invalid" % json_str
        return 

    result = json_obj["result"]
    result_asset = result[asset]
    return result_asset["frozen"]

def get_frozen_asset(user_id):
    params = [int(user_id), asset]
    request = {'id': 1, 'method': "asset.query",'params':params}
    print 'POST:', access_http_url, "?", request

    req = urllib2.Request(access_http_url, json.dumps(request))
    res_data = ""
    try :
        res_data = urllib2.urlopen(req)
        res_data = res_data.read()
    except urllib2.HTTPError, error:
        print 'Http error:', error
        return "0"
    #print res_data
    return parse_asset(res_data)

def connect_db():
    db_host = "localhost"
    db_user = "root"
    db_pwd = "shit"
    db_name = "trade_log"

    return MySQLdb.connect(db_host, db_user, db_pwd, db_name, charset='utf8')

def get_last_asset_table():
    db = connect_db()
    cursor = db.cursor()
    
    cursor.execute("show tables like 'slice_balance_1%'")
    results = cursor.fetchall()
    last_table = ""
    for row in results:
        last_table = row[0]

    cursor.close()
    db.close()
    return last_table;

def export_users_to_file(user_list):
    file_name = "data/user_{}.dat".format(asset)
    file = open(file_name, 'w+')
    for item in user_list:
        file.write(str(item) + ' ')
    file.write('\n')
    file.close()

def check_frozen_users():
    if not os.path.exists("data"):
        os.mkdir("data")
    
    table = get_last_asset_table()
    sql = "SELECT `user_id` from {} WHERE `asset`='{}' AND `t`='2'".format(table, asset)
    print "sql:%s" % sql
    
    users = []
    db = connect_db()
    cursor = db.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    if (len(results) == 0):
        print "could read asset records"
        return 
    for row in results:
        users.append(row[0])

    cursor.close()
    db.close()
     
    export_users_to_file(users)
    print "%d users has asset:%s" % (len(users), asset)

def read_users():
    file_name = "data/user_{}.dat".format(asset)
    file = None
    try:
        file = open(file_name, 'r')
    except IOError, error:
        print 'IOError:', error
        return 

    users = file.readline().split()
    file.close()
    print "Has read %d users" % len(users)
    return users

def export_frozen_to_file(fonzens):
    file_name = "data/frozen_{}.dat".format(asset)
    file = open(file_name, 'w+')

    for item in fonzens:
        file.write(str(item) + ' ')
    file.write('\n')

    file.close()

def calculate_frozens(user_fonzens):
    total = float(0.0)
    for key, value in user_fonzens.items():
        total += float(value)
    return total

def check_frozen_assets():
    users = read_users()
    fonzens = []
    user_fonzens = {}
    for user in users:
        if user not in user_fonzens.keys():
            user_fonzens[user] = get_frozen_asset(user)
            fonzens.append(get_frozen_asset(user))
        else:
            print "user:%s repeat" % user
    
    print "fonzen users:%s fonzen total:%s" % (len(user_fonzens), calculate_frozens(user_fonzens))
    export_frozen_to_file(fonzens)
    print user_fonzens

def read_frozon():
    file_name = "data/frozen_{}.dat".format(asset)
    file = None
    try:
        file = open(file_name, 'r')
    except IOError, error:
        print 'IOError:', error
        return 

    user_fonzens = {}
    while True:
        line = file.readline()
        if line is None or len(line) == 0:
            break
        strs = line.split()
        user_fonzens[strs[0]] = strs[1]

    file.close()
    print "Has read %d frozens" % len(user_fonzens)
    print user_fonzens
    return user_fonzens

def main_fun():
    if __name__ != "__main__":
        print "not execute as main"
        return 

    if len(sys.argv) < 2:
        print "invalid params"
        print "please use ./user_asset_manager.py [user, frozen]"
        return 

    command = sys.argv[1]
    print "command:", command

    if command == "user":
        print '---check user---'
        check_frozen_users()
        return ;
    elif command == "frozen":
        print "---check frozen---" 
        check_frozen_assets()
    else :
        print "invalid command:", command 
        print "please use ./user_asset_manager.py [user, frozen]"

main_fun()