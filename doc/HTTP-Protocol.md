## API protocol
The API is based on HTTP protocol [JSON RPC](http://json-rpc.org/), whose request method must be POST. Its URL is：/ and the Content-Type is：application/json

**Request**
* method: method, String
* params: parameters, Array
* id: Request id, Integer

**Response**
* result: Json object, null for failure
* error: Json object, null for success, non-null for failure
1. code: error code
2. message: error message
* id: Request id, Integer

General error code:
* 1: invalid argument
* 2: internal error
* 3: service unavailable
* 4: method not found
* 5: service timeout

## Asset API

**Asset list**
* method: `asset.list`
* params: none
* result:
```
{
	'id': 1, 
	'result': {
		'1': {
			'BCH': {
				'prec_show': 20,
				'prec_save': 20
			}, 
			'BTC': {
				'prec_show': 20, 
				'prec_save': 20
			}
		}, 
		'0': {
			'NEO': {
				'prec_show': 20, 
				'prec_save': 20
			}, 
			'BCH': {
				'prec_show': 20, 
				'prec_save': 20
			}, 
			'SEER': {
				'prec_show': 20, 
				'prec_save': 20
			}
			...
		}
	}, 
	'error': null
}
```

**Asset change**
* method: `asset.update`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. asset: asset name, String
4. business: business type, String
5. business_id: business ID, Integer, but it will only succeed once with multiple operations of the same user_id, asset, business or business_id
6. change: balance change, String, negative numbers for deduction
7. detail: Json object, attached information
* result: "success"
* error code:
10. repeat update
11. balance not enough
* example:

```
"params": [1, 1, "BTC", "deposit", 100, "1.2345"]
"result": "success"
```

**Asset lock**
* method: `asset.lock`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. asset: asset name, String
4. business: business type, String
5. business_id: business ID, Integer, but it will only succeed once with multiple operations of the same user_id, asset, business or business_id
6. amount: amount, String
* result: "success"
* error code:
10. repeat update
11. balance not enough

**Asset unlock**
* method: `asset.unlock`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. asset: asset name, String
4. business: business type, String
5. business_id: business ID, Integer, but it will only succeed once with multiple operations of the same user_id, asset, business or business_id
6. amount: amount, String
* result: "success"
* error code:
10. repeat update
11. balance not enough

**Asset query**
* method: `asset.query`
* params:
1. user_id
2. account: account ID, Integer, 0 is compatible with the original
asset list(optional, if no asset special, return all asset)
* result:
```
{
    "error": null,
    "result": {
        "BTC": {
            "available": "250",
            "frozen": "10"
        }
    },
    "id": 1530261478813
}
```

**Asset query all**
* method: `asset.query_all`
* params:
1. user_id
* result:
```
{
	'id': 1, 
	'result': {
		'1': {
			'BCH': {
				'available': '990554.89473682', 
				'frozen': '9445.10526318'
			}, 
			'BTC': {
				'available': '92.919825', 'frozen': '7.08000000'
			}
		}, 
		'0': { 
		    'CET': {
		        'available': '999900473.1500094689', 
		        'frozen': '0'
		    }, 
		    'BCH': {
		        'available': '1002000200.00000000', 
		        'frozen': '0'
		    },
		    'BTC': {
		        'available': '100.00000000', 
		        'frozen': '0'
		    }
		}
	}, 
	'error': null
}
```

**Asset query lock**
* method: `asset.query_lock`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
asset list(optional, if no asset special, return all asset)
* result:
```
{
    "error": null,
    "result": {
        "BTC": "250"
    },
    "id": 1530261478813
}
```

**Asset backup**
* method: `asset.backup`
* params: none

* result
```
{
    "error":null,
    "id": 1,
    "result": {
        "table":"backup_asset_123213",
        "time":123213
    }
}
```

**Asset history**
* method: `asset.history`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. asset: asset name, which can be empty
4. business: business, which can be empty
5. start_time: start time, 0 for unlimited, Integer
6. end_time: end time, 0 for unlimited, Integer
7. offset: offset position, Integer
8. limit: count limit, Integer
* result:
```
{
	'id': 1, 
	'result': {
		'records': [
			{
				'account': 1, 'business': 'trade', 'detail': {}, 'asset': 'BTC', 'time': 1558680350.414049, 'balance': '99.999825', 'change': 'trade', 'user': 553
			}, 
			{
				'account': 1, 'business': 'trade', 'detail': {}, 'asset': 'BTC', 'time': 1558680350.414049, 'balance': '99.619825', 'change': 'trade', 'user': 553
			}
		], 
		'limit': 100,
		'offset': 0
	},
	'error': null
}
```

## Trade API

**Place limit order**
* method: `order.put_limit`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. market: market name, String
4. side: 1: sell, 2: buy, Integer
5. amount: count, String
6. price: price, String
7. taker_fee_rate: String, taker fee
8. maker_fee_rate: String, maker fee
9. source: String, source, up to 30 bytes
10. fee_asset: String, asset to use as fee
11. fee_discount: String, 0~1
* result: order detail
* error:
10. balance not enough
* example:

```
params: [1, 0, "BTCCNY", 1, "10", "8000", "0.002", "0.001"]
```

**Place market order**
* method: `order.put_market`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. market: market name, String
4. side: 1: sell, 2: buy, Integer
5. amount: count or amount, String
6. taker_fee_rate: taker fee
7. source: String, source, up to 30 bytes
8. fee_asset: String, asset to use as fee
9. fee_discount: String, 0~1
* result: order detail
* error:
10. balance not enough
* example:

```
params: '[1, "BTCCNY", 1, "10","0.002"]'
```

**Cancel order**
* method: `order.cancel`
* params:
1. user_id: user ID
2. market：market
3. order_id： order ID
* result: order detail
* error:
10. order not found
11. user not match

**Place limit stop order**
* method: `order.put_stop_limit`
* params:
1. user_id: user ID, Integer
2. market: market name, String
3. side: 1: sell, 2: buy, Integer
4. amount: count, String
5. stop_price: price, String
6. price: price, String
7. taker_fee_rate: String, taker fee
8. maker_fee_rate: String, maker fee
9. source: String, source, up to 30 bytes
10. fee_asset: String, asset to use as fee
11. fee_discount: String, 0~1
* result: order detail
* error:
10. balance not enough
11. invalid stop price
12. amount too small

**Place stop market order**
* method: `order.put_stop_market`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. market: market name, String
4. side: 1: sell, 2: buy, Integer
5. amount: count or amount, String
6. stop_price: stop price
7. taker_fee_rate: taker fee
8. source: String, source, up to 30 bytes
9. fee_asset: String, asset to use as fee
10. fee_discount: String, 0~1
* result: order detail
* error:
10. balance not enough
11. invalid stop price
12. amount too small

**Place self market order**
* method: `market.self_deal`
* params:
1. market: market name, String
2. amount: count or amount, String
3. price: price, String
4. side: 1: sell, 2: buy, Integer
* result: "success"
* error:
10. no reasonable price

**Cancel stop order**
* method: `order.cancel_stop`
* params:
1. user_id: user ID
2. market：market
3. order_id： order ID
* result: order detail
* error:
10. order not found
11. user not match

**Order details**
* method: `order.deals`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
3. order_id: order ID, Integer
4. offset
5. limit
* result:
* example:

```
{
	'id': 1, 
	'result': {
		'records': [{
			'deal_user': 553,
			'account': 1,
			'fee': '2.25',
			'deal': '0.05',
			'price': '0.05',
			'amount': '1',
			'role': 1,
			'user': 553,
			'time': 1558610344.312309,
			 'fee_asset': 'CET',
			'deal_order_id': 198,
			'id': 99
		}],
		'limit': 100,
		'offset': 0
	},
	'error': null
}
```

**Order Book**
* method: `order.book`
* params:
1. market:
2. side: side, 1：sell, 2：buy
3. offset:
4. limit:
* result:
```
{
	'id': 1, 
	'result': {
	 	'total': 1, 
	 	'limit': 100, 
	 	'orders': [
	 		{
	 			'asset_fee': '741.59999998110000000000', 
	 			'account': 1, 
	 			'deal_fee': '0', 
	 			'ctime': 1558680092.364266, 
	 			'maker_fee': '0.0001', 
	 			'price': '0.01900000', 
	 			'deal_stock': '557.89473682', 
	 			'fee_discount': '0.9000', 
	 			'side': 1, 
	 			'source': 'api', 
	 			'amount': '10000.00000000', 
	 			'user': 553, 
	 			'mtime': 1558680350.414576, 
	 			'fee_asset': 'CET', 
	 			'deal_money': '16.4799999995800000', 
	 			'left': '9442.10526318', 
	 			'type': 1, 
	 			'id': 292, 
	 			'market': 'BCHBTC', 
	 			'taker_fee': '0.0001'
	 		}
	 	], 
	 	'offset': 0
	}, 
	'error': null
}
```

**Stop Book**
* method: `order.stop_book`
* params:
1. market:
2. side: side, 1：sell, 2：buy
3. offset:
4. limit:
* result:
```
{
 "result": {
  "total": 1,
  "offset": 0,
  "limit": 10,
  "orders": [{
   "side": 2,
   "amount": "5",
   "taker_fee": "0.001",
   "source": "MoonTest",
   "type": 1,
   "mtime": 1.535383338231633E9,
   "fee_asset": "CET",
   "market": "CETBCH",
   "fee_discount": "1",
   "stop_price": "0.0002",
   "price": "0.00021",
   "maker_fee": "0.001",
   "ctime": 1.535383338231633E9,
   "id": 5,
   "user": 102,
   "account": 1 
  }]
 },
 "id": 1,
 "error": null
}
```

**Order depth**
* method: `order.depth`
* params:
1. market：market name
2. limit: count limit, Integer
3. interval: interval, String, e.g. "1" for 1 unit interval, "0" for no interval
* result:

```
"result": {
    "asks": [
        [
            "8000.00",
            "9.6250"
        ]
    ],
    "bids": [
        [
            "7000.00",
            "0.1000"
        ]
    ],
    "last":"99.50001",
    "time":1153809861000
}
```

**Inquire unexecuted orders**
* method: `order.pending`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
3. market: market name, String, Null for all market
4. side: 0 no limit, 1 sell, 2 buy
5. offset: offset, Integer
6. limit: limit, Integer
* result:
* example:

```
"params": [1, "BTCCNY", 0, 100]"
"result": {
    'id': 1, 
    'result': {
    	'records': [
    		{
    	    	'asset_fee': '0',
    			'account': 1,
    			'deal_fee': '0',
    			'ctime': 1558680350.41878,
    			'maker_fee': '0.0001',
    			'price': '0.01900000',
    			'deal_stock': '0',
    			'fee_discount': '0.9000',
    			'side': 1, // 1：sell, 2：buy
    			'source': 'api',
    			'amount': '1.00000000',
    			'user': 553,
    			'mtime': 1558680350.41878,
    			'fee_asset': 'CET',
    			'deal_money': '0',
    			'left': '1.00000000',
    			'type': 1, // 1: limit order, 2：market order
    			'id': 303,
    			'market': 'BCHBTC',
    			'taker_fee': '0.0001'
    		}
    	], 
    	'total': 1,
    	'limit': 100,
    	'offset': 0
    },
    'error': null
}
```

**Inquire unexecuted stop orders**
* method: `order.pending_stop`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
3. market: market name, String, Null for all market
4. side: 0 no limit, 1 sell, 2 buy
5. offset: offset, Integer
6. limit: limit, Integer
* result:
```
"result": {
	'id': 1,
	'result': {
		'records': [
			{
				'account': 1,
				'fee_discount': '0.9000',
				'stop_price': '0.01800000',
				'ctime': 1558684047.573049,
				'maker_fee': '0',
				'price': '0',
				'side': 1,
				'source': 'api',
				'amount': '1.00000000',
				'user': 553,
				'mtime': 1558684047.573049,
				'fee_asset': 'CET',
				'type': 2,
				'id': 305,
				'market': 'BCHBTC',
				'taker_fee': '0.0010'
			}
		],
		'total': 1,
		'limit': 100,
		'offset': 0
	},
	'error': null
}
```

**Unexecuted order details**
* method: `order.pending_detail`
* params:
1. market:
2. order_id: order ID, Integer
* result:
```
{
	'id': 1,
	'result': {
		'asset_fee': '741.59999998110000000000',
		'account': 1,
		'deal_fee': '0','c
		'time': 1558680092.36317,
		'maker_fee': '0.0001',
		'price': '0.01900000',
		'deal_stock': '557.89473682',
		'fee_discount': '0.9000',
		'side': 1,
		'source': 'api',
		'amount': '10000.00000000',
		'user': 553,
		'mtime': 1558680350.414576,
		'fee_asset': 'CET',
		'deal_money': '16.4799999995800000',
		'left': '9442.10526318',
		'type': 1,
		'id': 292,
		'market': 'BCHBTC',
		'taker_fee': '0.0001'
	},
	'error': null
}
```

**Inquire executed orders**
* method: `order.finished`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
3. market: market name, String
4. side: side, 0 for no limit, 1 for sell, 2 for buy
5. start_time: start time, 0 for unlimited, Integer
6. end_time: end time, 0 for unlimited, Integer
7. offset: offset, Integer
8. limit: limit, Integer
* result:
```
{
	'id': 1,
	'result': {
		'records': [
			{
				'asset_fee': '1.8',
				'account': 1,
				'fee_discount': '0.9',
				'ctime': 1558680071.197081,
				'maker_fee': '0.0001',
				'price': '0.019',
				'deal_fee': '0',
				'side': 1,
				'source': 'api',
				'amount': '1',
				'ftime': 1558680071.197093,
				'user': 553,
				'deal_stock': '1',
				'fee_asset': 'CET',
				'deal_money': '0.04',
				'type': 1,
				'id': 290,
				'market': 'BCHBTC',
				'taker_fee': '0.0001'
			}
		],
		'limit': 100,
		'offset': 0
	},
	'error': null
}
```

**Inquire executed stop orders**
* method: `order.finished_stop`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
3. market: market name, String
4. side: side, 0 for no limit, 1 for sell, 2 for buy
5. start_time: start time, 0 for unlimited, Integer
6. end_time: end time, 0 for unlimited, Integer
7. offset: offset, Integer
8. limit: limit, Integer
* result:
```
{
	'id': 1,
	'result': {
		'records': [
			{
				'account': 1,
				'fee_discount': '0.9000',
				'stop_price': '0.01800000',
				'ctime': 1558684047.573049,
				'maker_fee': '0',
				'price': '0',
				'side': 1,
				'source': 'api',
				'amount': '1.00000000',
				'user': 553,
				'mtime': 1558684047.573049,
				'fee_asset': 'CET',
				'type': 2,
				'id': 305,
				'market': 'BCHBTC',
				'taker_fee': '0.0010'
			}
		],
		'total': 1,
		'limit': 100,
		'offset': 0
	},
	'error': null
}
```

**Executed order details**
* method: `order.finished_detail`
* params:
1. user_id: user ID, Integer
2. order_id: order ID, Integer
* result:
```
{
	'id': 1,
	'result': {
		'asset_fee': '0',
		'account': 0,
		'fee_discount': '0',
		'ctime': 1558600893.818328,
		'maker_fee': '0.0001',
		'price': '0.05',
		'deal_fee': '0.0001',
		'side': 2,
		'source': 'api',
		'amount': '1',
		'ftime': 1558600893.823128,
		'user': 553,
		'deal_stock': '1',
		'fee_asset': '',
		'deal_money': '0.05',
		'type': 1,
		'id': 1,
		'market': 'BCHBTC',
		'taker_fee': '0.0001'
	},
	'error': null
}
```

## Market API

**Market price**
* method: `market.last`
* params:
1. market
* result: "price"

**Executed history**

**Market list**
* method: `market.list`
* params: none

* method: `market.deals`
* params:
1. market:
2. limit: count, no more than 10000
3. last_id: id limit
* result:

```
"result": [
    {
        "id": 5,
        "time": 1492697636.238869,
        "type": "sell",
        "amount": "0.1000",
        "price": "7000.00"
    }
    ...
]
```

**Executed history extend**
* method: `market.deals_ext`
* params:
1. market:
2. limit: count, no more than 10000
3. last_id: id limit
* result:

```
"result": [
    {
        "id": 5,
        "time": 1492697636.238869,
        "type": "sell",
        "price": "7000.00"
        "amount": "0.1000"
        "ask_user_id": 1,
        "bid_user_id": 2,
    }
    ...
]
```

**User Executed history**
* method: `market.user_deals`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
3. market: market name, String
4. side: side, 0 for no limit, 1 for sell, 2 for buy
5. start_time: start time, 0 for unlimited, Integer
6. end_time: end time, 0 for unlimited, Integer
7. offset: offset, Integer
8. limit: limit, Integer
* result:

```
"result": [
    "offset":
    "limit":
    "records": [
        {
            "id": Executed ID
            "order_id": Order ID
            "time": timestamp
            "user": user ID
            "account": account ID,
            "side": side, 1：sell, 2：buy
            "role": role, 1：Maker, 2: Taker
            "amount": count
            "price": price
            "deal": amount
            "fee": fee
            "fee_asset": fee asset
        }
    ...
    ]
}
```

**KLine**
* method: `market.kline`
* params:
1. market: market
2. start: start, Integer
3. end: end, Integer
4. interval: interval, Integer
* result:

```
"result": [
    [
        1492358400, time
        "7000.00",  open
        "8000.0",   close
        "8100.00",  highest
        "6800.00",  lowest
        "1000.00"   volume
        "123456.78" amount
        "BTCCNY"    market name
    ]
]
```

**Market status**
* method: `market.status`
* params:
1. market: market name
2. period: cycle period, Integer, e.g. 86400 for last 24 hours
* result:

```
"result": {
    "period": 86400,
    "last": "7000.00",
    "open": "0",
    "close": "0",
    "high": "0",
    "low": "0",
    "volume": "0"
}
```

## Monitor API

**Increase**
* method: `monitor.inc`
* params:
1. scope: business type, eg: web
2. key: event key
3. host: server name, not empty
4. value: event aggregate count, integer

* example: monitor.inc("web", "job1", "sms_send_success", 1)

**Set**
* method: `monitor.set`
* params:
1. scope: business type, eg: web
2. key: event key
3. host: server name, not empty
4. value: event aggregate count, integer

* example: monitor.set("web", "job1", "sms_send_pending", 10)

**List scope**
* methdod: `monitor.list_scope`
* params: None
* result: scope list

**List key**
* method: `monitor.list_key`
* params:
1. scope
* result: key list

**List host**
* method: `monitor.list_host`
* params:
1. scope
2. key
* result: host list

**Query Minute**
* method: `monitor.query_minute`
* params:
1. scope
2. key
3. host: Can be empty
4. points: total past minutes
* result:
```
[
  [timestamp, value]
  ...
]
```

**Query Daily**
* method: `monitor.query_daily`
* params:
1. scope
2. key
3. host: Can be empty
4. points: total past minutes
* result:
```
[
  [timestamp, value]
  ...
]
```

## TradeRank API

**TradeRank**
* method: `trade.rank`
* params:
1. market list: array
2. start_time: start time  Integer
3. end_time: start time  Integer

* example: trade.rank("[BTCUSDT]", 1557471355, 1557476355)

## Index API

**market index list**

* method: `index.list`
* params: None

* result:

```
"result": {
	'id': 1,
	'result': [
		{
			'index': '265.40',
			'name': 'ETHUSDT',
			'timestamp': 1558945100
		},
		{
			'index': '112.40',
			'name': 'LTCUSDT',
			'timestamp': 1558945100
		},
		{
			'index': '8652.86',
			'name': 'BTCUSDT',
			'timestamp': 1558945100
		}
	],
	'error': null
}
```

**query market index**

* method: `index.query`
* params:
	1. market name

* result:

```
request: ['BTCUSDT']

respose:
{
	'id': 1,
	'result': {
		'index': '112.28',
		'name': 'LTCUSDT',
		'timestamp': 1558945160
	},
	'error': null
}
```

## Config API

**Update asset config**
* method: `config.update_asset`

**Update market config**
* method: `config.update_market`

**Update market index config**

* method: `config.update_index`
* result:

```
 {
     'id': 1,
     'result': {
     		'status': 'success'
     },
     'error': null
  
```