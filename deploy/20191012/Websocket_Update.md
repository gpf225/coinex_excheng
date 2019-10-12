# 修改接口


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


**Order notification**
* method: `order.update`
* params:
1. event: event type，Integer, 1: PUT, 2: UPDATE, 3: FINISH
2. order: order detail，Object，see HTTP protocol

* method: `order.update_stop`
* params:
1. event: event type，Integer, 1: PUT, 2: ACTIVE, 3: CALCEL
2. order: order detail，Object，see HTTP protocol
