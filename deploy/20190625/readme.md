#一. 更新的模块：
* 1 matchengine： 添加option_fee，stop，call_auction 3个功能
* 2 historywrite: 增加option字段
* 3 readhistory:  增加读取option字段
* 4 accessws:     asset与market长度修改成30
* 5 cachecenter:  market长度修改成30
* 6 tradesummary: asset长度修改成30
* 7 accesshttp:   更新错误码与文档保存一致
* 8 marketindex:  增加相关交易所指数价格与价格保护机制

#二. 配置文件
* marketindex新增配置：
```
    "protect_interval": 300,
    "protect_rate": "0.3"
```

#三. 升级操作：先升级数据，再重启服务
* 1 在scripts/option\_fee下: 执行alter\_option.sh脚本: 
  1.1 修改slice\_example表: 1.增加option字段; 2.修改精度20改成24(balance表:balance, order表:frozen, deal\_fee, asset\_fee)  
  1.2 修改history表: 增加option字段

* 2 暂停交易与充值提现，数据切片后: 执行在scripts/call\_aution下的market\_price\_data\_format.py，用于修改slice_history最后一个切片的数据格式
* 3 停止matchengine
* 4 web修改asset_url对应的 "prec\_save":20 改成24; 修改market\_url添加account
* 5 重启 matchengine
* 6 重启 historywrite
* 7 重启 readhistory
* 8 重启 accesshttp
* 9: marketindex: 后台增加：gate.io，kucoin，bitfinex，max, bittrex, poloniex这几个交易所的配置,然后重启marketindex
    
    注意：
    1. 配置bittrex与bittrex时，例如USDT-BTC：money为USDT， stock为BTC，<span style="color:red">**与普通表示是相反的**</span>
    2. 每配置一个url，最好手动使用curl命令进行访问是否正常，<span style="color:red">**curl访问正常后再配置到后台**</span>
    
    配置url参考：
    
    ```
    1. gate.io
    curl https://data.gateio.co/api2/1/tradeHistory/btc_usdt
    
    2. kucoin
    curl https://api.kucoin.com/api/v1/market/histories?symbol=BTC-USDT
    
    3. bitfinex
    curl https://api.bitfinex.com/v1/trades/btcusd?limit_trades=1
    
    4. mxc
    curl https://www.mxc.com/open/api/v1/data/history?market=BTC_USDT
    
    5. bittrex
    curl https://api.bittrex.com/api/v1.1/public/getmarkethistory?market=USDT-BTC  注意USDT_BTC: money为USDT，stock为BTC
    
    6. poloniex
    https://poloniex.com/public?command=returnTradeHistory&currencyPair=USDT_BTC  注意USDT_BTC: money为USDT，stock为BTC
    
    ```
