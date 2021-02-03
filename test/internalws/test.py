#encoding: utf-8
import sys
import websocket
import thread
import threading
import time
import json
import traceback
import ssl
import threading
import multiprocessing
import random
import hashlib
from decimal import Decimal

start_time = time.time()

WS_URL = "ws://127.0.0.1:443"

def server_ping(ws):
    params = {
        "id": 1,
        "method": "server.ping",
        "params": ['lzwtest_{}'.format(time.time())]
    }
    ws.send(json.dumps(params))

def server_time(ws):
    params = {
        "id": 1,
        "method": "server.time",
        "params": []
    }
    ws.send(json.dumps(params))

# kline
def kline_query(ws):
    end_time = int(time.time())
    begin_time = end_time - 86400

    params = {
        "id": 201,
        "method": "kline.query",
        "params": ["BTCUSDT", begin_time, end_time, 1]
    }
    ws.send(json.dumps(params))

def kline_subscribe(ws):
    params = {
        "id": 202,
        "method": "kline.subscribe",
        "params": [["BTCUSDT", 1], ["ETHUSDT", 1]]
    }
    ws.send(json.dumps(params))

def kline_unsubscribe(ws):
    params = {
        "id": 203,
        "method": "kline.unsubscribe",
        "params": []
    }
    ws.send(json.dumps(params))

# depth
def depth_query(ws):
    params = {
        "id": 1,
        "method": "depth.query",
        "params": ["BTCUSDT", 20, "1"]
    }
    ws.send(json.dumps(params))

def depth_subscribe(ws):
    params = {
        "id": 2,
        "method": "depth.subscribe",
        "params": [["BTCUSDT", 20, "1"], ["ETHUSDT", 20, "1"]]
    }
    ws.send(json.dumps(params))


def depth_unsubcribe(ws):
    print("######## depth_unsubcribe")
    params = {
        "id": 3,#int(time.time()),
        "method": "depth.unsubscribe",
        "params": []
    }
    ws.send(json.dumps(params))

#deals
def deals_query(ws):
    print("######## deals_subscribe")
    params = {
        "id": 1,#int(time.time()),
        "method": "deals.query",
        "params": ["BTCUSDT", 10, 0]
    }
    ws.send(json.dumps(params))

def deals_subscribe(ws):
    print("######## deals_subscribe")
    params = {
        "id": 1,#int(time.time()),
        "method": "deals.subscribe",
        "params": ["BTCUSDT", "ETHUSDT"]
    }
    ws.send(json.dumps(params))

def deals_unsubscribe(ws):
    print("######## deals_unsubscribe")
    params = {
        "id": 1,#int(time.time()),
        "method": "deals.unsubscribe",
        "params": []
    }
    ws.send(json.dumps(params))

def deals_query_user(ws):
    params = {
        "id": 1,#int(time.time()),
        "method": "deals.query_user",
        "params": [476, 0, "BTCUSDT", 0, 0, 0, 0, 10]
    }
    ws.send(json.dumps(params))
    return

def deals_subscribe_user(ws):
    params = {
        "id": 2,#int(time.time()),
        "method": "deals.subscribe_user",
        "params": [[476, "BTCUSDT", "ETHUSDT"], [800, "BTCUSDT", "ETHUSDT"]]
    }
    ws.send(json.dumps(params))
    return

def deals_unsubscribe_user(ws):
    print("######## deals_unsubscribe_user")
    params = {
        "id": 1,#int(time.time()),
        "method": "deals.unsubscribe_user",
        "params": []
    }
    ws.send(json.dumps(params))

def order_query(ws):
    params = {
        "id": 1,
        "method": "order.query",
        "params": [476, 0, "BTCUSDT", 0, 0, 10]
    }
    ws.send(json.dumps(params))

def order_query_stop(ws):
    params = {
        "id": 2,
        "method": "order.query_stop",
        "params": [476, 0, "BTCUSDT", 0, 0, 10]
    }
    ws.send(json.dumps(params))

def order_subscribe(ws):
    params = {
        "id": 3,
        "method": "order.subscribe",
        "params": [[476, "BTCUSDT", "ETHUSDT"], [800, "BTCUSDT", "ETHUSDT"]]
    }
    ws.send(json.dumps(params))

def order_unsubscribe(ws):
    params = {
        "id": 4,
        "method": "order.unsubscribe",
        "params": []
    }
    ws.send(json.dumps(params))


#asset
def asset_query(ws):
    params = {
        "id": 1,
        "method": "asset.query",
        "params": [476, 1, 'BTC', 'ETH', 'USDT']
    }
    ws.send(json.dumps(params))

def asset_query_all(ws):
    params = {
        "id": 2,
        "method": "asset.query_all",
        "params": [476]
    }
    ws.send(json.dumps(params))

def asset_subscribe(ws):
    print("asset_subscribe")
    params = {
        "id": 3,
        "method": "asset.subscribe",
        "params": [[476, "BTC", "ETH"], [800, "BTC", "ETH"]]
    }
    ws.send(json.dumps(params))

def asset_unsubscribe(ws):
    print("asset_unsubscribe")
    params = {
        "id": 4,
        "method": "asset.unsubscribe",
        "params": []
    }
    ws.send(json.dumps(params))

def run(ws):
    asset_query(ws)

def on_error(ws, error):
    print("on_error:{}".format(error))
 
def on_close(ws):
    print "### closed ###"

def ws_close(ws):
    print("#########ws close")
    ws.close()

message_count = 0
def on_message(ws, message):
    global message_count
    message_count += 1
    #print("on_message:{}".format(message))
    message = json.loads(message)
    print(message)
    if message_count == 15:
        deals_unsubscribe_user(ws)
        asset_unsubscribe(ws)
    
def on_open(ws):
    #server_ping(ws)
    #server_time(ws)s
    #kline_query(ws)
    #kline_subscribe(ws)
    #deals_subscribe(ws)
    #deals_query_user(ws)
    #deals_subscribe_user(ws)
    #depth_query(ws)s
    #depth_subscribe(ws)
    #order_query(ws)
    #order_query_stop(ws)
    order_subscribe(ws)
    #asset_query(ws)
    #asset_query_all(ws)
    asset_subscribe(ws)

def main(argv):
    try:
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            WS_URL,
            header={"X-Forwarded-For": "192.168.0.99"},
            on_message = on_message,
            on_error = on_error,
            on_close = on_close,
        )
        ws.on_open = on_open
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False})

    except Exception as ex:
        print("Exception:{}".format(ex))
        traceback.print_exc()


def web_direct():
    print("web direct")
    ws = websocket.create_connection(WS_URL)
    print()
    server_sign(ws)
    time.sleep(100)

if __name__ == '__main__':
    #web_direct()
    main(sys.argv[1:])

