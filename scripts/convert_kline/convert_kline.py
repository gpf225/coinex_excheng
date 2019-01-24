#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import redis
import json
import ast

'''
拷贝K线数据分3步:
步骤1: 配置需要拷贝的K线数据对应的市场
步骤2: 执行./convert_kline.py export从redis导出指定的k线数据，在./kline文件夹下面有导出的K线数据，可以人工检查是否正常。
步骤3: 执行./convert_kline.py import将k线数据导入到redis。

redis配置:
本文件只支持连接到redis的master结点，需要先查到master结点的IP和端口信息。

市场配置:
在market_convert中配置需要拷贝的市场，例如：LFT改名为LFC，那么请如下配置：
   market_convert = {"BCHSVBCH": "BSVBCH", "BCHSVBTC": "BSVBTC", "BCHSVUSDT": "BSVUSDT"} 
need_convert标志，一般情况下配置为False。当市场定价货币和交易货币反转时将该参数设置为True。例如：
从BTCBCH市场拷贝k线数据到BCHBTC市场，此时将need_convert设置为True
'''

redis_host = "127.0.0.1"
redis_port = 6383

market_convert = {"LFTBCH": "LFCBCH", "LFTBTC": "LFCBTC", "LFTETH": "LFCETH", "LFTUSDT": "LFCUSDT"}
need_convert = False
data_dir = "data"

def get_kline_key_second(market):
    return "k:%s:1s" % market

def get_kline_key_minute(market):
    return "k:%s:1m" % market

def get_kline_key_hour(market):
    return "k:%s:1h" % market

def get_kline_key_day(market):
    return "k:%s:1d" % market

def get_redis_client():
    return redis.StrictRedis(redis_host, redis_port)

def get_kline_from_redis(key):
    print "get kline from redis, key:", key
    redis_client = get_redis_client()
    dcit_value = redis_client.hgetall(key)

    print "key:%s size:%d:" % (key, len(dcit_value))
    return dcit_value

def get_convert_kline(kline_str):
    kline = ast.literal_eval(kline_str)
    kline_open    = str(round(1.0 / float(kline[0]), 6))
    kline_close   = str(round(1.0 / float(kline[1]), 6))
    kline_high    = str(round(1.0 / float(kline[3]), 6))
    kline_low     = str(round(1.0 / float(kline[2]), 6))
    kline_amount  = kline[5]
    kline_deal    = kline[4]
    
    return '["%s", "%s", "%s", "%s", "%s", "%s"]' % (kline_open, kline_close, kline_high, kline_low, kline_amount, kline_deal)

def convert_kline_dict(kline_dict):
    for key in kline_dict:
        kline_list = get_convert_kline(kline_dict[key])
        kline_dict[key] = kline_list


def serialize_kline_into_file(market, key, kline_dict):
    kline_json = json.dumps(kline_dict)

    file_name = ("%s/%s_%s.json" % (data_dir, market, key[len(key)-2 :]))
    file = open(file_name, 'w+')
    
    file.write(kline_json + '\n')
    file.close()

def fetch_kline(market, key):
    dict_kline = get_kline_from_redis(key)
    if len(dict_kline) == 0:
        print "key:%s kline is empty" % key
        return 
    serialize_kline_into_file(market, key, dict_kline)

    reverted_market_nane = market_convert[market]
    if need_convert:
        convert_kline_dict(dict_kline)
    serialize_kline_into_file(reverted_market_nane, key, dict_kline)


def fetch_kline_second(market):
    fetch_kline(market, get_kline_key_second(market))

def fetch_kline_minute(market):
    fetch_kline(market, get_kline_key_minute(market))

def fetch_kline_hour(market):
    fetch_kline(market, get_kline_key_hour(market))

def fetch_kline_day(market):
    fetch_kline(market, get_kline_key_day(market))

def export_kline():
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    
    for market in market_convert.keys():
        print "export_kline market:", market
        fetch_kline_second(market)
        fetch_kline_minute(market)
        fetch_kline_hour(market)
        fetch_kline_day(market)

def get_kline_from_cache(market, key):
    file_name = ("%s/%s_%s.json" % (data_dir, market, key[len(key)-2 :]))
    if not os.path.exists(file_name):
        print "file_name:%s does not exist" % file_name
        return None

    file = None
    try:
        file = open(file_name, 'r')
    except IOError, error:
        print 'IOError:', error
        return 

    file_content = file.readline();
    #print "file_content:", file_content
    dict_kline = json.loads(file_content)
    file.close()
    print "read kline:%s size:%d" % (key, len(dict_kline))
    return dict_kline

def import_cached_kline_to_redis(market, key):
    dict_kline = get_kline_from_cache(market, key)
    if dict_kline is None:
        print "key:%s does not have kline data, no need to import" % key
        return 

    print "import, key:%s size:%d" % (key, len(dict_kline))
    #print "dict_kline:", dict_kline
    redis_client = get_redis_client()
    redis_client.hmset(key, dict_kline)

def import_kline_second(market):
    import_cached_kline_to_redis(market, get_kline_key_second(market))

def import_kline_minute(market):
    import_cached_kline_to_redis(market, get_kline_key_minute(market))

def import_kline_hour(market):
    import_cached_kline_to_redis(market, get_kline_key_hour(market))

def import_kline_day(market):
    import_cached_kline_to_redis(market, get_kline_key_day(market))

def import_kline():
    for key in market_convert.keys():
        market_dest = market_convert[key]
        print "import market:", market_dest
        import_kline_second(market_dest)
        import_kline_minute(market_dest)
        import_kline_hour(market_dest)
        import_kline_day(market_dest)

def clear_all_data():
    ls = os.listdir(data_dir)
    for i in ls:
        c_path = os.path.join(data_dir, i)
        if os.path.isdir(c_path):
            del_file(c_path)
        else:
            os.remove(c_path)

def print_help():
    print "please use ./convert_kline.py [export, import, clear]"

def main_fun():
    if __name__ != "__main__":
        print "not execute as main"
        return 

    if len(sys.argv) < 2:
        print "invalid params"
        print_help()
        return 

    command = sys.argv[1]
    print "command:", command

    if command == "export":
        print '---export kline---'
        export_kline()
        return 

    elif command == "import":
        print "---import kline---" 
        import_kline();
        return 
    
    elif command == "clear":
        print "---clear all cached data---" 
        clear_all_data()
        return 
    else :
        print "invalid command:", command 
        print_help()

main_fun()