# 修改接口

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