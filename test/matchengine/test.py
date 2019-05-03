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
    r = requests.post('http://%s:%d/' % (httpsvr['host'], httpsvr['port']), data=json.dumps(data))
    result = r.json()

    return result

    '''
    if result['error'] is not None:
        print "error code: {code}, error msg: {msg}".format(code = result['error']['code'], msg = result['error']['message'])
    else :
        return result['result']
    '''

## write
def asset_update(user_id, asset, amount):
    detail = {'test': 'test'}
    if not hasattr(asset_update, 'business_id'):
        asset_update.business_id = 34

    asset_update.business_id += 1

    r = rpc('asset.update', [user_id, asset, 'test', asset_update.business_id, amount, detail])
    return r

def asset_lock(user_id, asset, amount):
    if not hasattr(asset_update, 'business_id'):
        asset_update.business_id = 0

    asset_update.business_id += 1

    r = rpc('asset.lock', [user_id, asset, 'test', asset_update.business_id, amount])
    return r

def asset_unlock(user_id, asset, amount):
    detail = {'locktest': 'locktest'}
    if not hasattr(asset_update, 'business_id'):
        asset_update.business_id = 0

    asset_update.business_id += 1

    r = rpc('asset.unlock', [user_id, asset, 'test', asset_update.business_id, amount])
    return r

def asset_backup():
    detail = {'locktest': 'locktest'}
    if not hasattr(asset_update, 'business_id'):
        asset_update.business_id = 0

    asset_update.business_id += 1

    r = rpc('asset.backup', [])
    return r

def put_limit_order(user_id, market, order_side, amount, price, taker_fee_rate, maker_fee_rate):
    r = rpc('order.put_limit', [user_id, market, order_side, amount, price, taker_fee_rate, maker_fee_rate, 'test'])
    return r

def put_market_order(user_id, market, order_side, amount, taker_fee_rate):
    r = rpc('order.put_market', [user_id, market, order_side, amount, taker_fee_rate, 'test'])
    return r

def cancel_order(user_id, market, order_id):
    r = rpc('order.cancel', [user_id, market, order_id])
    return r

def put_stop_limit(user_id, market, order_side, amount, stop_price, price, taker_fee_rate, maker_fee_rate, fee_asset):
    r = rpc('order.put_stop_limit', [user_id, market, order_side, amount, stop_price, price, taker_fee_rate, maker_fee_rate, 'test', fee_asset, ''])
    return r

def put_stop_market(user_id, market, order_side, amount, stop_price, taker_fee_rate, fee_asset):
    r = rpc('order.put_stop_market', [user_id, market, order_side, amount, stop_price, taker_fee_rate, 'test', fee_asset, ''])
    return r

def cancel_stop(user_id, market, order_id):
    r = rpc('order.cancel_stop', [user_id, market, order_id])
    return r

def self_deal(market, amount, price, side, ):
    r = rpc('market.self_deal', [market, amount, price, side])
    return r

## read
def asset_list():
    r = rpc('asset.list', [])
    return r

def asset_query(user_id):
    r = rpc('asset.query', [user_id])
    return r

def asset_query_lock(user_id):
    r = rpc('asset.query_lock', [user_id])
    return r

def order_pending(user_id, market, side, offset, limit):
    r = rpc('order.pending', [user_id, market, side, offset, limit])
    return r

def order_book(market, side, offset, limit):
    r = rpc('order.book', [market, side, offset, limit])
    return r

def stop_book(market, side, offset, limit):
    r = rpc('order.stop_book', [market, side, offset, limit])
    return r

def order_depth(market, limit, interval):
    r = rpc('order.depth', [market, limit, interval])
    return r

def order_detail(market, order_id):
    r = rpc('order.pending_detail', [market, order_id])
    return r

def pending_stop(user_id, market, side, offset, limit):
    r = rpc('order.pending_stop', [user_id, market, side, offset, limit])
    return r

def market_list():
    r = rpc('market.list', [])
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

    ### write
    print "asset_update: \n"
    r = asset_update(user_id, asset, asset_amount)
    print r

    print "asset_update2: \n"
    r = asset_update(user_id, asset2, asset_amount2)
    print r

    print "asset_lock: \n"
    r = asset_lock(user_id, asset, amount)
    print r

    print "asset_unlock: \n"
    r = asset_unlock(user_id, asset, amount)
    print r

    print "asset_backup: \n"
    r = asset_backup()
    print r

    print "put_limit_order: \n"
    r = put_limit_order(user_id, market, order_side, amount, price, taker_fee_rate, maker_fee_rate)
    print r

    print "put_market_order: \n"
    r = put_market_order(user_id, market, order_side, amount, taker_fee_rate)
    print r

    print "put_stop_limit: \n"
    r = put_stop_limit(user_id, market, order_side, amount, stop_price, price, taker_fee_rate, maker_fee_rate, fee_asset)
    print r

    print "put_stop_market: \n"
    r = put_stop_market(user_id, market, order_side, amount, stop_price, taker_fee_rate, fee_asset)
    print r

    print "self_deal: \n"
    r = self_deal(market, amount, price, order_side)
    print r


    order_id = 8149102
    r = cancel_order(user_id, market, order_id)
    print r

    '''
    stop_id = 
    r = cancel_stop(user_id, market, order_id)
    print r
    '''


    ## read
    print "asset_list: \n"
    r = asset_list()
    print r

    print "asset_query: \n"
    r = asset_query(user_id)
    print r

    print "asset_query_lock: \n"
    r = asset_query_lock(user_id)
    print r

    offset = 0
    limit = 20
    interval = '0.001'

    print "order_pending: \n"
    r = order_pending(user_id, market, order_side, offset, limit)
    print r

    print "order_book: \n"
    r = order_book(market, order_side, offset, limit)
    print r

    print "stop_book: \n"
    r = stop_book(market, order_side, offset, limit)
    print r

    print "order_depth: \n"
    r = order_depth(market, limit, interval)
    print r

    print "order_detail: \n"
    r = order_detail(market, order_id)
    print r

    print "pending_stop: \n"
    r = pending_stop(user_id, market, order_side, offset, limit)
    print r

    print "market_list: \n"
    r = market_list()
    print r
