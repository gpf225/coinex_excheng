## 新增接口

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

## 修改接口

### Trade API

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
12. option: optional field, Integer
    * bit 1: use stock fee only;
    * bit 2: use money fee only;
    * bit 3: unlimited min amount
    * bit 4: immediate or cancel order
    * bit 5: fill or kill order

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
10. option: optional field, Integer
    * bit 1: use stock fee only;
    * bit 2: use money fee only;
    * bit 3: unlimited min amount;
    * bit 5: fill or kill order;
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
11. option: optional field, Integer, 
	* bit 1: use stock fee only
	* bit 2: use money fee only
	* bit 4: immediate or cancel order
    * bit 5: fill or kill order
* result: order detail
* error:
1.  invalid stop price
2.  amount too small

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