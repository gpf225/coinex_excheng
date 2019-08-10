## 修改的接口

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