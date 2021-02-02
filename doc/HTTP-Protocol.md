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

**Asset query in time**
* method: `asset.query_intime`
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

**Asset query users**
* method: `asset.query_users`
* params:
1. account: account ID, Integer, greater than 0
2. user list: array
* result:
```
{
    "error": null,
    "result": {
        "553": {
            "BTC": {
                "available": "1.106000000",
                "frozen": "2.01000000"
            },
            "USDT": {
                "available": "0.00000000",
                "frozen": "0.00000000"
            }
        },
        "554": {
            "BTC": {
                "available": "1.00000000",
                "frozen": "0.01000000"
            },
            "USDT": {
                "available": "0.00000000",
                "frozen": "0.00000000"
            }
        }
    },
    "id": 11111
}
```

**Asset query users in time**
* method: `asset.query_users_intime`
* params:
1. account: account ID, Integer, greater than 0
2. user list: array
* result:
```
{
    "error": null,
    "result": {
        "553": {
            "BTC": {
                "available": "1.106000000",
                "frozen": "2.01000000"
            },
            "USDT": {
                "available": "0.00000000",
                "frozen": "0.00000000"
            }
        },
        "554": {
            "BTC": {
                "available": "1.00000000",
                "frozen": "0.01000000"
            },
            "USDT": {
                "available": "0.00000000",
                "frozen": "0.00000000"
            }
        }
    },
    "id": 11111
}
```

**Asset summary**
* method: `asset.summary`
* params:
1. asset
2. account: account ID (optional)
* result:
```
{
	'id': 1, 
	'result': {
		'total_users': 10,
		'available_users': 10,
		'lock_users': 10,
		'frozen_users': 10,
		'total': '100',
		'available': '100',
		'frozen': '100',
		'lock': 11
	}, 
	'error': null
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

**Asset query all in time**
* method: `asset.query_all_intime`
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

**Asset query lock in time**
* method: `asset.query_lock_intime`
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
12. option: Integer, 0 is default
    * bit 1: use stock fee only;
    * bit 2: use money fee only;
    * bit 3: unlimited min amount
    * bit 4: immediate or cancel order
    * bit 5: fill or kill order
13. client_id: user self-define order id

* result: order detail
* error:
10. balance not enough
11. amount too small
12. can't be completely executed, kill the order
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
10. option: Integer, 0 is default
    * bit 1: use stock fee only;
    * bit 2: use money fee only;
    * bit 3: unlimited min amount;
    * bit 5: fill or kill order;
11. client_id: user self-define order id

* result: order detail
* error:
10. balance not enough
11. amount too small
12. no enough trader
13. can't be completely executed, kill the order
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

**Cancel order batch**
* method: `order.cancel_batch`
* params:
1. user_id: user ID
2. market：market
3. order_id list： array of order ID list 

* result:

```
{
    'id': 1, 
    'result': [
        {
            "code": 10, //10:order not found; 11:user not match
            "message": "order not found",
            "order": {
                //order detail
            }
        }
    ],
    'error': null
}
```



**Cancel all order**
* method: `order.cancel_all`
* params:
1. user_id: user ID
2. account: cancel all orders belong to account, -1 for cancel all orders
3. market：market
4. side: side , optional
* result: "success"


**Place limit stop order**
* method: `order.put_stop_limit`
* params:
1. user_id: user ID, Integer
2. account: account ID, Integer, 0 is compatible with the original
3. market: market name, String
4. side: 1: sell, 2: buy, Integer
5. amount: count, String
6. stop_price: price, String
7. price: price, String
8. taker_fee_rate: String, taker fee
9. maker_fee_rate: String, maker fee
10. source: String, source, up to 30 bytes
11. fee_asset: String, asset to use as fee
12. fee_discount: String, 0~1
13. option: Integer, 0 is default
    * bit 1: use stock fee only
    * bit 2: use money fee only
    * bit 3: unlimited min amount
14. client_id: user self-define order id

* result: order detail
* error:
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
11. option: Integer, 0 is default
    * bit 1: use stock fee only
    * bit 2: use money fee only
    * bit 4: immediate or cancel order
    * bit 5: fill or kill order
12. client_id: user self-define order id

* result: order detail
* error:
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

**Cancel all stop order**
* method: `order.cancel_stop_all`
* params:
1. user_id: user ID
2. account: cancel the all stop orders belong to account, -1 for cancel all stop orders
3. market：market
4. side: side , optional
* result: "success"
  
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
	 			'taker_fee': '0.0001',
				'client_id': 'test_123'
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
2. state:  1：low, 2: high
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
   "account": 1,
   "client_id": "test_123"
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
    			'taker_fee': '0.0001',
				'client_id': 'test_123'
    		}
    	], 
    	'total': 1,
    	'limit': 100,
    	'offset': 0
    },
    'error': null
}
```

**Inquire unexecuted orders in time**
* method: `order.pending_intime`
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
                'taker_fee': '0.0001',
                'client_id': 'test_123'
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
				'taker_fee': '0.0010',
				'client_id': 'test_123'
			}
		],
		'total': 1,
		'limit': 100,
		'offset': 0
	},
	'error': null
}
```

**Inquire unexecuted stop orders in time**
* method: `order.pending_stop_intime`
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
                'taker_fee': '0.0010',
                'client_id': 'test_123'
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
		'taker_fee': '0.0001',
		'client_id': 'test_123'
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
				'taker_fee': '0.0001',
				'client_id': 'test_123'
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
				'taker_fee': '0.0010',
				'client_id': 'test_123'
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
		'taker_fee': '0.0001',
		'client_id': 'test_123'
	},
	'error': null
}
```

**call auction start**
* method: `call.start`
* params:
1. market: market name, String
* result:

```
request: ["BTCUSDT"]
response:
	{
		"id": 1562296842,
		"result": {
			"status": "success"
		},
		"error": null
	}
```

**call auction execute**
* method: `call.execute`
* params:
1. market: market name, String
* result: 
1. price: string, if price == '0' need to self deal
2. volume: string deal amount
```
request: ["BTCUSDT"]
response:
	{
		"id": 1562296842,
		"result": {
			"price": "3.6500000",
			"volume": "12.00"
		},
		"error": null
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

**Market detail**
* method: `market.detail`
* params:
1. market: market name

```
{
    "id": 1565367444,
    "result": {
        "account": -1,
        "fee_prec": 4,
        "money": "USDC",
        "money_prec": 8,
        "min_amount": "0.001",
        "stock_prec": 8,
        "stock": "USDT",
        "name": "USDTUSDC"
    },
    "error": null
}
```

**Market deals**
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

**Market summary**
* method: `market.summary`
* params:
1. market
* result:
```
{
	'id': 1,
	'result': {
		'order_users': 10,
		'order_ask_users': 10,
		'order_bid_users': 10,
		'stop_users': 10,
		'stop_ask_users': 10,
		'stop_bid_users': 10,
		'orders': 10,
		'stops': 10,
		'order_asks': 10,
		'order_ask_amount': '10.10',
		'order_ask_left': '100.10',
		'order_bids': '100.10',
		'order_bid_amount': '100.10',
		'order_bid_left': '100.10',
		'stop_asks': '1000.10',
		'stop_ask_amount': '100.10',
		'stop_bids': '100.10',
		'stop_bid_amount': '100.10'
	},
	'error': null
}
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
        "1000.00"   amount
        "123456.78" volume
        "BTCCNY"    market name
    ]
]
```

**Market status**
* method: `market.status`
* params:
1. market: market name and get index status by market name with suffix _INDEX like: BTCUSDT_INDEX
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

**TradeNetRank**
* method: `trade.net_rank`
* params:
1. market list: array
2. start_time: start time  Integer
3. end_time: start time  Integer

* example: trade.net_rank("[BTCUSDT]", 1557471355, 1557476355)
* result:

```
{
    "id": 1572834684,
    "result": {
        "sell": [
            {
                "net": "1803.44444445",
                "total": "1823.44444445",
                "user_id": 305
            },
            {
                "net": "10.00000000",
                "total": "10.00000000",
                "user_id": 63
            }
        ],
        "buy": [
            {
                "net": "1813.44444445",
                "total": "1813.44444445",
                "user_id": 814
            }
        ],
        "total_amount": "1823.44444445",
        "total_net": "1813.44444445",
        "total_sell_users": 2,
        "total_buy_users": 1
    },
    "error": null
}
```

**TradeAmountRank**
* method: `trade.amount_rank`
* params:
1. market list: array
2. start_time: start time  Integer
3. end_time: start time  Integer

* example: trade.amount_rank("[BTCUSDT]", 1557471355, 1557476355)
* result:

```
{
    "id": 1572834698,
    "result": {
        "sell": [
            {
                "amount": "1813.44444445",
                "user_id": 305,
                "total_amount": "1823.44444445"
            },
            {
                "amount": "10.00000000",
                "user_id": 63,
                "total_amount": "10.00000000"
            }
        ],
        "buy": [
            {
                "amount": "1813.44444445",
                "user_id": 814,
                "total_amount": "1813.44444445"
            },
            {
                "amount": "10.00000000",
                "user_id": 305,
                "total_amount": "1823.44444445"
            },
            {
                "amount": "0",
                "user_id": 63,
                "total_amount": "10.00000000"
            }
        ],
        "total_sell_users": 2,
        "total_buy_users": 3,
        "total_sell_amount": "2.0",
        "total_buy_amount": "2.0",
        "total_users": 2
    },
    "error": null
}
```

**Users Volume**
* method: `trade.users_volume`
* params:
1. market list: array
2. user list: array
2. start_time: start time  Integer
3. end_time: start time  Integer

* example: trade.users_volume [["BTCUSDT"], [476], 1598357836, 1598929182]
* result:

```
{
    "id": 1572834684,
    "result": {
        "BTCUSDT": {
            "476":     //user_id
            [
                "3400.1020000000000000",  //buy volume
                "3400.1020000000000000"   //sell volume
            ],
            "477":     //user_id
            [
                "3400.1020000000000000",  //buy volume
                "3400.1020000000000000"   //sell volume
            ]
        }
    },
    "error": null
}
```

## Index API

**market index list**

* method: `index.list`
* params: None

* result:

```
"result": {
	'id': 1,
	'ttl': 1000,
	'result': {
		'LTCUSDT': {
			'index': '114.23',
			'time': 1559042800000
		},
		'BTCUSDT': {
			'index': '8711.09',
			'time': 1559042800000
		},
		'ETHUSDT': {
			'index': '269.37',
			'time': 1559042800000
		}
	},
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
	'ttl': 1000,
	'result': {
		'index': '8705.37',
		'name': 'BTCUSDT',
		'time': 1559042980000
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

## Push message API

**Push user message**
* method: `push.user_message`
* params:
1. message: json format, must need user_id param

* result:

```
 {
     'id': 1,
     'result': {
            'status': 'success'
     },
     'error': null
  
```