#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import json
import decimal
import requests
import datetime

#coin = {'host': '127.0.0.1', 'port': 8080}
httpsvr = {'host': '127.0.0.1', 'port': 8080}

def rpc(method, params):
    data = {'method': method, 'params': params, 'id': int(time.time() * 1000)}
    #print(data)
    r = requests.post('http://%s:%d/' % (httpsvr['host'], httpsvr['port']), data=json.dumps(data))
    result = r.json()

    return result

    '''
    if result['error'] is not None:
        print "error code: {code}, error msg: {msg}".format(code = result['error']['code'], msg = result['error']['message'])
    else :
        return result['result']
    '''

def asset_query(user_id):
    r = rpc('asset.query', [user_id, 0])
    return r

def asset_query_intime(user_id):
    r = rpc('asset.query_intime', [user_id, 0])
    return r

def asset_query_all(user_id):
    r = rpc('asset.query_all', [user_id])
    return r

def asset_query_all_intime(user_id):
    r = rpc('asset.query_all_intime', [user_id])
    return r

def asset_query_lock(user_id):
    r = rpc('asset.query_lock', [user_id, 0])
    return r

def asset_query_lock_intime(user_id):
    r = rpc('asset.query_lock_intime', [user_id, 0])
    return r

def asset_query_users(user_ids):
    r = rpc('asset.query_users', [1, user_ids])
    return r

def asset_query_users_intime(user_ids):
    r = rpc('asset.query_users_intime', [1, user_ids])
    return r

def order_pending(user_id, market, side, offset, limit):
    r = rpc('order.pending', [user_id, market, side, offset, limit])
    return r

def order_pending_intime(user_id, market, side, offset, limit):
    r = rpc('order.pending_intime', [user_id, market, side, offset, limit])
    return r

def pending_stop(user_id, market, side, offset, limit):
    r = rpc('order.pending_stop_intime', [user_id, market, side, offset, limit])
    return r

def pending_stop_intime(user_id, market, side, offset, limit):
    r = rpc('order.pending_stop_intime', [user_id, market, side, offset, limit])
    return r

def pending_detail(market, order_id):
    r = rpc('order.pending_detail', [market, order_id])
    return r

def order_book(market, side, offset, limit):
    r = rpc('order.book', [market, side, offset, limit])
    return r

def order_stop_book(market, side, offset, limit):
    r = rpc('order.stop_book', [market, side, offset, limit])
    return r

def market_detail(market):
    r = rpc('market.detail', [market])
    return r

def count_time(func):
    def int_time(*args, **kwargs):
        start_time = datetime.datetime.now()  # 程序开始时间
        func()
        over_time = datetime.datetime.now()   # 程序结束时间
        total_time = (over_time - start_time).total_seconds()
        print('count_time: %s' % total_time)
    return int_time

if __name__ == '__main__':

    asset_amount = '1000'
    asset_amount2 = '1000000'

    user_id = 476
    asset = 'BTC'
    asset2 = 'USDT'
    amount = '10'
    market = 'BTCUSDT'
    order_side = 1;
    price = '7300'
    taker_fee_rate = '0.001'
    maker_fee_rate = '0.001'
    stop_price = '7500'
    fee_asset = 'USDT'

    '''
    stop_id = 
    r = cancel_stop(user_id, market, order_id)
    print r
    '''


    print "asset_query:"
    r = asset_query(user_id)
    print r

    print "asset_query intime:"
    r = asset_query_intime(user_id)
    print r

    print "asset_query all:"
    r = asset_query_all(user_id)
    print r

    print "asset_query all intime:"
    r = asset_query_all_intime(user_id)
    print r

    print "asset_query_lock:"
    r = asset_query_lock(user_id)
    print r

    print "asset_query_lock intime:"
    r = asset_query_lock_intime(user_id)
    print r

    print "asset_query_users:"
    r = asset_query_users([476, 477])
    print r

    print "asset_query_lock intime:"
    r = asset_query_users_intime([476, 477])
    print r
    
