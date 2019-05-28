#Websocket接口变更

##新增接口

**Unexecuted account order inquiry**

* method: `order.account_query`
* params:
	1. user_id: user ID, Integer
	2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
	3. market: market name，String
	4. side: 0 no limit, 1 sell, 2 buy
	5. offset: offset，Integer
	6. limit: limit，Integer
* result: see HTTP protocol(order.pending)

**Unexecuted stop order inquiry**

* method: `order.account_query_stop`
* params:
	1. user_id: user ID, Integer
	2. account: account ID, Integer, 0 is compatible with the original, -1 for query all
	3. market: market name，String
	4. side: 0 no limit, 1 sell, 2 buy
	5. offset: offset，Integer
	6. limit: limit，Integer
* result: see HTTP protocol(order.pending_stop)

**All Account Asset inquiry**

* method: `asset.account_query_all`
* params: 
	1. user_id: user ID, Integer
	2. asset list, null for inquire all
* result: see HTTP protocol(asset.query_all)

```
request: [553]

response:
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
		    ...
		}
	}, 
	'error': null
}
```

**Account Asset inquiry**

* method: `asset.account_query`
* params:
    1. user_id: user ID, Integer
    2. account: account ID, Integer, 0 is compatible with the original
    3. asset list, null for inquire all
* result: see HTTP protocol(asset.query)

```
request:
[469, 0, ["CET", "BTC"]]

response:
{"BTC": {"available": "1.10000000","freeze": "9.90000000"}}
```

**index subscription**

* method: `index.subscribe`
* params:

**cancel index subscription**

* method: `index.unsubscribe`
* params:

**index notification**

* method: `index.update`
* params:

```
	{
		"method": "index.update",
		"params": ["BTCUSDT", "8730.91"],
		"id": null
	}
```

##修改接口
### Deal API

**Acquire user latest executed list**

* method: `deals.query_user`
* params:
	1. user_id: user ID, Integer
	2. <span style='color:red'>**account: account ID, Integer, 0 is compatible with the original, -1 for query all**</style>
	3. market: market name, String
	4. side: side, 0 for no limit, 1 for sell, 2 for buy
	5. start_time: start time, 0 for unlimited, Integer
	6. end_time: end time, 0 for unlimited, Integer
	7. offset: offset, Integer
	8. limit: limit, Integer