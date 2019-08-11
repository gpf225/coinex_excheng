## 一、更新的模块

1、accesshttp:

* 增加根据市场名换回市场信息接口
* fatal log增加加入时间戳

2、accessrest

* 增加根据市场名获取市场信息接口
* 能够获取指数的ticker

3、accessws

* 支持指数推送
* fatal log增加加入时间戳

4、cachecenter

* 增加指数信息推送

5、marketindex

* fatal log增加加入时间戳

6、marketprice

* fatal log增加加入时间戳

7、matchengine

* 限价单、计划委托单支持FillOrCancel、ImmediateOrCancel
* 市价单支持FillOrCancel
* reader内存泄漏
* fatal log增加时间戳
* 自成交开启CET抵扣交易流水不对修复
* http_request请求修改为异步
* 计划委托不检测当前用户余额及增加最小交易金额检测
* stop订单打上stop标签及unlimited_min标签

8、readhistory

* fatal log增加加入时间戳

9、tradesummary

* fatal log增加加入时间戳

## 二、修改配置文件

1、cachecenter配置文件增加

```
    "marketindex": {
        "name": "marketindex",
        "addr": [
            "tcp@127.0.0.1:7901"
        ],
        "max_pkg_size": 2000000
    }
```

三、升级操作

1、暂停交易与充值提现
2、数据切片(makeslice)
3、更新程序
4、重启，重启顺序：

  * 1、marketindex
  * 2、accesshttp
  * 3、accessws
  * 4、accessrest
  * 5、matchengine
  * 6、marketprice
  * 7、readhistory
  * 8、cachecenter
  * 9、tradesummary
