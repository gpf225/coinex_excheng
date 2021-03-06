## API protocol

The API is based on [JSON RPC](http://json-rpc.org/wiki/specification) of Websocket protocol. Repeated subscription will be cancelled for the same data type.

**Request**
* method: method，String
* params: parameters，Array
* id: Request id, Integer

**Response**
* result: Json object，null for failure
* error: Json object，null for success, non-null for failure
1. code: error code
2. message: error message
* id: Request id, Integer

**Notify**
* method: method，String
* params: parameters，Array
* id: Null

General error code:
* 1: invalid argument
* 2: internal error
* 3: service unavailable
* 4: method not found
* 5: service timeout
* 6: require authentication

## System API

**PING**
* method: `server.ping`
* params: None
* result: "pong"
* example: `{"error": null, "result": "pong", "id": 1000}`

**System time**
* method: `server.time`
* params: none
* result: timestamp，Integer
* example: `{"error": null, "result": 1493285895, "id": 1000}`

**ID verification Web**
* method: `server.auth`
* params:
1. token: String
2. source: String, source，e.g. "web", version number up to 30 bytes is required for applications

**Sub user ID verification Web**
* method: `server.auth_sub`
* params: user id list, null for all.

**ID verification Api**
* method: `server.sign`
* params:
1. access_id: String
2. authorization: String, sign data
3. tonce: timestamp，for milliseconds spent from Unix epoch to current time，and error between tonce and server time can not exceed plus or minus 60s

## Market API

**Market inquiry**
* method: `kline.query`
* params: 
1. market: market name
2. start: start time，Integer
3. end: end time, Integer
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
        "123456.00" amount
        "BTCCNY"    market name
    ]
    ...
]
```

**Market subscription**
* method: `kline.subscribe`
* params:
1. market
2. interval

**Market notification**
* method: `kline.update`
* params:

```
[
    [
        1492358400, time
        "7000.00",  open
        "8000.0",   close
        "8100.00",  highest
        "6800.00",  lowest
        "1000.00"   volume
        "123456.00" amount
        "BTCCNY"    market name
    ]
    ...
]
```

**Cancel subscription**
* method: `kline.unsubscribe`
* params: none


## Market state API

**Acquire market status**
* method: `state.query`
* params:
1. market: market
2. period: cycle period，Integer, e.g. 86400 for last 24 hours
* result: 

```
{
    "open": open price
    "last": latest price
    "high": highest price
    "low":  lowest price
    "deal": value
    "volume": volume
}
```

**Market 24H status subscription**
* method: `state.subscribe`
* params: market list, null for subscribe all

**Market 24H status notification**
* method: `state.update`
* params:
1. market state and get index status by market name with suffix _INDEX like: BTCUSDT_INDEX

```
{
    "BCHBTC": {
        "open": open price
        "last": latest price
        "high": highest price
        "low":  lowest price
        "deal": value
        "volume": volume
    }
    ...
}
``` 

**Cancel subscription**
* method: `state.unsubscribe`
* params: none

## Deal API

**Acquire latest executed list**
* method: `deals.query`
* params:
1. market：market
2. limit：amount limit
3. last_id: largest ID of last returned result

**Acquire user latest executed list**
* method: `deals.query_user`
* params:
1. account: account ID, Integer, 0 is compatible with the original, -1 for query all
2. market: market name, String
3. side: side, 0 for no limit, 1 for sell, 2 for buy
4. start_time: start time, 0 for unlimited, Integer
5. end_time: end time, 0 for unlimited, Integer
6. offset: offset, Integer
7. limit: limit, Integer

**Latest order list subscription, if call after auth, will push user's deal**
* method: `deals.subscribe`
* params: market list

**Latest order list update**
* method: `deals.update`
* params: 
1. market name
2. order list
3. user's deal, boolean, optional

**Cancel subscription**
* method: `deals.unsubscribe`
* params: none

## Depth API

**Acquire depth**
* method: `depth.query`
* params:
1. market：market name
2. limit: amount limit，Integer
3. interval: interval，String, e.g. "1" for 1 unit interval, "0" for no interval

* result: checksum is a signed integer (32 bit) and is for verify the accuracy of depth data, 
the checksum string will be bid1_price:bid1_amount:bid2_price:bid2_amount:ask1_price:ask1_amount:...
if there is no bids, the checksum string will be ask1_price:ask1_amount:ask2_price:ask2_amount...
```
"result": {
    "last": "7500.00",
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
    "checksum": 216578179
}
```

**Depth subscription**
* method: `depth.subscribe`
* params:
1. market
2. limit
3. interval

**Depth notification**
* method: `depth.update`
* params:
1. clean: Boolean, true: complete result，false: last returned updated result
2. Same as depth.query，only return what's different from last result, asks 或 bids may not exist.
amount == 0 for delete
3. market name

```
"result": {
    "asks": [
        ["3.70000000", "6.00000000"]
    ],
    "bids": [
        ["3.64000000", "4.00000000"],
        ["3.62000000", "7.00000000"],
        ["3.54000000", "6.00000000"]
    ],
    "last": "3.66000000",
    "time": 1562662542872,
    "checksum": 216578179
}
```

**Depth Full subscription**
* method: `depth.subscribe_full`
* params:
1. market
2. limit
3. interval

* result:
1. clean
2. depth: same as the depth.query
3. market name

```
[
    true,
    {
        "asks": [
            ["10000.00", "10.00000000"], ["10001.00", "10.00000000"]
        ],
        "bids": [
            ["9999.00", "1.50000000"], ["9998.00", "1.50000000"], ["9997.00", "8.00000000"], ["9996.00", "135.00000000"]
        ],
        "last": "0.10",
        "time": 1574838026364,
        "checksum": 216578179
    },
    "BTCUSDT"],
    "id": null
    },
    "BTCUSDT"
]
```

**Cancel depth full subscription**
* method: `depth.unsubscribe_full`
* params: none


## Order API (Authentication required before connection)

**Unexecuted order inquiry**
* method: `order.query`
* params:
1. market: market name，String
2. side: 0 no limit, 1 sell, 2 buy
3. offset: offset，Integer
4. limit: limit，Integer
* result: see HTTP protocol

**Unexecuted stop order inquiry**
* method: `order.query_stop`
* params:
1. market: market name，String
2. side: 0 no limit, 1 sell, 2 buy
3. offset: offset，Integer
4. limit: limit，Integer
* result: see HTTP protocol

**Unexecuted account order inquiry**
* method: `order.account_query`
* params:
1. account: account ID, Integer, 0 is compatible with the original, -1 for query all
2. market: market name，String
3. side: 0 no limit, 1 sell, 2 buy
4. offset: offset，Integer
5. limit: limit，Integer
* result: see HTTP protocol

**Unexecuted stop order inquiry**
* method: `order.account_query_stop`
* params:
1. account: account ID, Integer, 0 is compatible with the original, -1 for query all
2. market: market name，String
3. side: 0 no limit, 1 sell, 2 buy
4. offset: offset，Integer
5. limit: limit，Integer
* result: see HTTP protocol

**Order subscription**
* method: `order.subscribe`
* params: market list

**Order notification**
* method: `order.update`
* params:
1. event: event type，Integer, 1: PUT, 2: UPDATE, 3: FINISH
2. order: order detail，Object, last_deal_amount is the amount of the last deal, last_deal_price is the price of the last deal
* result:
```
{
    "method": "order.update",
    "params": [
        1,
        {
            "id": 12750,
            "type": 1,
            "side": 2,
            "user": 1,
            "account": 0,
            "option": 0,
            "ctime": 1571971282.015829,
            "mtime": 1571971282.015829,
            "market": "BTCUSDT",
            "source": "test",
            "client_id": "buy1_1234",
            "price": "5999.00",
            "amount": "1.50000000",
            "taker_fee": "0.0001",
            "maker_fee": "0.0001",
            "left": "1.50000000",
            "deal_stock": "0",
            "deal_money": "0",
            "deal_fee": "0",
            "asset_fee": "0",
            "fee_discount": "1",
            "last_deal_amount": "0",
            "last_deal_price": "0",
            "fee_asset": null
        }
    ],
    "id": null
}
```

* method: `order.update_stop`
* params:
1. event: event type，Integer, 1: PUT, 2: ACTIVE, 3: CALCEL
2. order: order detail，Object，see HTTP protocol

**Cancel subscription**
* method: `order.unsubscribe`
* params: none

## Asset API (Authentication required before connection)

**Asset inquiry**
* method: `asset.query`
* params: asset list, null for inquire all
* result:

```
{"BTC": {"available": "1.10000000","freeze": "9.90000000"}}
```

**Sub-account asset inquiry**
* method: `asset.query_sub`
* params: 
  1. sub user id,
  2. asset list, null for inquire all
* result:

```
request:
[469, ["CET", "BTC"]]

response:
{
	"CET": {
		"available": "1.10000000",
		"freeze": "9.90000000"
	},
	"BTC": {
		"available": "1.10000000",
		"freeze": "9.90000000"
	}
}
```

**Account Asset inquiry**
* method: `asset.account_query`
* params: 
1. account: account ID, Integer, 0 is compatible with the original
2. asset list, null for inquire all
* result:

```
request:
[469, 0, ["CET", "BTC"]]

response:
{"BTC": {"available": "1.10000000","freeze": "9.90000000"}}
```

**All Account Asset inquiry**
* method: `asset.account_query_all`
* params: 
    1. asset list, null for inquire all
* result:

```
request:
[553]

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

**Asset subscription**
* method: `asset.subscribe`
* params: asset list, null for subscribe all

**Asset notification**
* method: `asset.update`
* params: 
```
[{"BTC: {"available": "1.10000000","freeze": "9.90000000"}, "CNY": {}}]
```

**Cancel subscription**
* method: `asset.unsubscribe`
* params: none

**Asset subscription delay(delay time can be change, 1s default)**
* method: `asset.subscribe_delay`
* params: asset list, null for subscribe all

**Cancel subscription delay**
* method: `asset.unsubscribe_delay`
* params: none

**Sub-account asset subscription**
* method: `asset.subscribe_sub`
* params: user list, null for subscribe all

**Sub-account asset notification**
* method: `asset.update_sub`
* params: 
```
[469, {"BTC: {"available": "1.10000000","freeze": "9.90000000"}, "CNY": {}}]
469 is the sub-account id
```

**Cancel sub-account subscription**
* method: `asset.unsubscribe_sub`
* params: none

## Iindex API

**index query**

* method: `index.query`
* params: 
1. market: market name

**index query list**

* method: `index.query_list`
* params: 

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