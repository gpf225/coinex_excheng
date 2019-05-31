#一. 更新的模块：
* 1 accesshttp：1.增加index，2.相关接口增加account字段
* 2 accessrest：增加index
* 3 accessws：1.增加index，2.相关接口增加account字段
* 4 historywrite: 增加account字段
* 5 marketindex：新增指数价格模块
* 6 marketprice：增加指数价格处理
* 7 matchengine：新增多账号功能
* 8 readhistory：增加读取account字段
* 9 tradesummary: 增加交易量amount排名接口

#二. 修改配置文件
* 1 部署marketindex， 配置文件为全新

* 2 部署accesshttp， 配置文件添加：

```
	 "marketindex": {
        "name": "marketindex",
        "addr": [
            "tcp@127.0.0.1:7901"
        ],
        "max_pkg_size": 2000000
    }
```

* 3 部署accessrest， 配置文件添加：

```
	 "marketindex": {
        "name": "marketindex",
        "addr": [
            "tcp@127.0.0.1:7901"
        ],
        "max_pkg_size": 2000000
    }
```

* 4 部署accessws， 配置文件添加：

```
	 "indexs": {
        "brokers": "127.0.0.1:9092",
        "topic": "indexs",
        "partition": 0
    }
```

* 5 部署historywrite，配置文件无需更改

* 6 部署marketprice， 配置文件添加：

 ```
	 "indexs": {
        "brokers": "127.0.0.1:9092",
        "topic": "indexs"
     }
 ```
 
* 7 部署matchengine，
  >. 配置文件json.config的reader_num 在原有基础上+1

  >. 确认asset_url沿用原来的还是新的。
 
* 8 部署readhistory，配置文件无需修改

* 9 部署tradesummary，配置文件无需修改

#三. 数据切片，升级数据库
* 1 升级history数据库表：去掉balance_history旧的索引，可能比较耗时放到该第一步
>. 在mulitaccount/alter_table.sh 只调用alter_history_table_drop_index

* 2 暂停交易，数据切片。
* 3 停止matchengine。
* 4 升级trade_log数据库(不会耗时)：增加account字段默认为0(包括slice example表与 最后一个时间的slice相关表)
	 > . 修改alter_table.sh的slice_time为最后切片的时间
	
    > . 修改alter_table.sh只调用alter_log_table_add_account函数
    
    
# 四：重启顺序：
* 1 marketindex
* 2 accesshttp
* 3 accessws
* 4	 accessrest
* 5 matchengine
* 6 marketprice
* 7 readhistory
* 8 historywrite
* 9	 tradesummary

