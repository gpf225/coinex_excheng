<!-- TOC -->

- [引言](#引言)
    - [目的](#目的)
    - [面向的读者](#面向的读者)
    - [范围](#范围)
- [概述](#概述)
    - [作用](#作用)
    - [服务关系](#服务关系)
- [系统结构](#系统结构)
    - [进程](#进程)
        - [access进程](#access进程)
        - [reader进程](#reader进程)
        - [writer进程](#writer进程)
    - [进程通信图](#进程通信图)
    - [程序文件](#程序文件)
    - [主要对象](#主要对象)
- [主要机制](#主要机制)
    - [启动过程](#启动过程)
    - [请求路由](#请求路由)
    - [请求处理](#请求处理)
    - [交易处理示例](#交易处理示例)
    - [reader进程同步](#reader进程同步)
- [配置](#配置)
- [运行维护](#运行维护)
    - [启动失败](#启动失败)
    - [服务不可用](#服务不可用)
        - [阻塞导致的"service unavailable"](#阻塞导致的service-unavailable)
        - [load operlog failed导致的"service unavailable"](#load-operlog-failed导致的service-unavailable)

<!-- /TOC -->

# 引言
## 目的
本文的目的是简要描述matchengine服务的实现。

## 面向的读者
面向的读者是撮合系统的开发与维护人员,使他们能够快速了解matchengine的技术实现要点。

## 范围
本文描述的范围包括：
- 服务的作用
- 服务的外部环境
- 服务的系统结构
- 主要机制
- 配置
- 运行维护事项


# 概述
## 作用
matchengine中文名称是"撮合引擎",是撮合系统的功能核心，其功能围绕交易组织。
功能包括以下请求的处理：
- 交易委托： 支持限价,市价委托;计划限价,计划市价委托
- 委托撤销：
- 委托查询：
- 账户资产变更: 转入,转出账户资产
- 查询市场订单簿
- 市场深度查询

matchengine运行期处理和产生的订单，成交,资产变化信息通过消息队列输出，由其它服务进行处理，如订单,成交统计汇总,生成K线数据。

撮合算法采用价格优先,时间优先策略.

## 服务关系

matchengine相关服务关系如下图:
``` dot
digraph {
    rankdir=TB
    ah[shape="record" label="accesshttp"]
    me[shape="box3d" label="matchengine" color=blue]
    ah->me
    q[shape="Mrecord" label="<orders> orders|<deals> deals|<his_balances> his_balances|<his_deals> his_deals" xlabel="kafka"]

    me->q

    ts->q:orders[dir="back"]
    ts->q:deals[dir="back"]
    mp->q:deals[dir="back"]
    hw->q:his_balances[dir="back"]
    hw->q:his_deals[dir="back"]

    ts[shape="record" label="tradesummary"]
    mp[shape="record" label="marketprice"]
    hw[shape="record" label="historywriter"]

    w[shape="record" label="{外部服务|asset_url\nmarket_url}"]
    
    me->w
}
```



matchengine作为消息生产者生产的消息以及消费者关系如下:

|主题|主题名称|消费者|
|-|-|-|
|TOPIC_ORDER|orders|tradesummary|
|TOPIC_DEAL|deals|marketprice,tradesummary|
|TOPIC_HIS_BALANCE|his_balances|historywriter: 保存|
|TOPIC_HIS_DEAL|his_deals|historywriter:保存|


外部服务: asset_url,market_ur,启动时需要访问，以获取币种信息,市场信息。

# 系统结构

## 进程
  进程关系图如下：
``` dot
digraph {
    M [label="主进程"]

    P1,P2,P3 [label=""]
    R1[label = "reader_0"]
    M->R1[style="dashed"]
    A [label="access"]
    M->A[label="1"]
    R2[label="reader_1"]
    M->P1[label="2"]
    P1->R2[style="dashed"]
    P1->P2
    R3[label="reader_2"]
    P2->R3[style="dashed"]
    P2->P3
    W[label="writer"]
    P3->W
    R4[label="reader_3"]
    P3->R4[style="dashed"]
}
```

图中实线表示fork的子进程,虚线表示父进程.
主进程第一次fork出access子进程,之后父进程第2次fork出一个子进程，父进程为reader_0,子进程继续fork其它reader进程.
最后一次fork的子进程是writer进程.

<span id="processes"></span>
### access进程

启动：
。创建连接到writer,reader的rpc client,主动连接
。打开服务端口(matchengine的服务端口)


### reader进程
可以有多个reader进程,数量由配置文件的reader_num决定.

启动：
。创建共享内存: writer写,reader读
。writer,reader之间通过管道通信,writer写入数据后,通过管道通知reader

。打开rpc server： 端口号=matchengine端口+reader_id+2

对象：
。dict_cache： 缓存市场深度.缓存超时时间可配置(cache_timeout),并且每分钟全部clear
。dict_fini_orders： 缓存已完成订单. 已完成订单最多保留时间可配置(order_fini_keeptime)，定时清理

。job： 单线程任务对象.接收和处理config.update_asset,config.update_market任务,更新币种和市场信息



### writer进程
启动：
。创建operlog定时器和job
。创建消息主题：
|主题对象|主题名称|对象列表|
|-|-|-|
|rkt_balances|balances|list_balances|
|rkt_orders|orders|list_orders|
|rkt_deals|deals|list_deals|
|rkt_stops|stops|list_stops|
|rkt_his_deals|his_deals|list_his_deals|
|rkt_his_stops|his_stops|list_his_stops|
|rkt_his_orders|his_orders|list_his_orders|
|rkt_his_balances|his_balances|list_his_balances|
。启动消息生产定时器，启动消息生产线程池
。启动快照定时器：

。创建共享内存：与reader共享
。打开端口，启动rpc servre： 端口号=matchengine端口+1
。创建me_rquest.job: 该任务负责处理config.update_asset,config.update_market请求

对象：
。me_operlog.job: 执行operlog写入sql的单线程任务

处理逻辑：
。operlog写入逻辑：
  定时生成sql(0.1秒执行一次),把sql加入到job,由job执行

。消息生产：
共8个主题，每个主题由一个线程执行写入。
定时执行，间隔0.1秒。

。快照：
每指定的间隔创建快照(slice_interval),通常是3600秒.
启动子进程创建快照。
快照数据包括:
|数据类型|快照表tag|处理|
|-|-|-|
|订单|slice_order|遍历市场,保存每个市场的asks,bids|
|计划委托|slice_stop|遍历市场,保存每个市场的stop_high,stop_low|
|账户资产|slice_balance|转储dict_balance|
|资产变动|slice_update|转储dict_update|

实际的快照表名中包含时间戳,如slice_order_1625715743
最后要增加新的快照记录(到slice_history表)。
清除历史快照，快照保存时间由配置slice_keeptime指定.



## 进程通信图

``` dot
digraph {
    rankdir=TB
    client[label="accesshttp"]
    client->access[label="RPC"]
    access[shape="record" label="access进程"]

    subgraph cluster{
        #rankdir=LR
        style = "invis";
        readers[shape="record" label="{reader进程|{0|1|2|3}}"]
        writer[shape="record" label="writer进程"]

        shms[shape="Mrecord" label="{共享内存|{0|1|2|3}}"]

        writer->readers[label="pipe通信"]
        writer->shms[label="写入"]
        readers->shms[label="读取" dir="back" ]
    }

    access->writer[label="RPC"]
    access->readers[label="RPC"]
}
```



## 程序文件

|文件名|说明|
|-|-|
|me_main.c|主程序文件|
|me_config.c|提供读取配置文件函数|
|me_access.c|access处理文件,接收来自accesshttp的请求,转发|
|me_writer.c|writer处理文件,包括writer进程初始化操作,请求处理函数|
|me_reader.c|reader处理文件,包括reader进程初始化操作,请求处理函数|
|me_asset.c|币种信息操作,账户资产备份|
|me_balance.c|账户资产模块,提供账户资产的增加,减少,冻结,解冻等函数|
|me_dump.c|快照模块,提供各类数据的快照dump函数|
|me_history.c|提供历史订单和成交消息生成函数|
|me_load.c|operlog处理模块|
|<b>me_market.c</b>|委托处理模块|
|me_message.c|消息推送模块|
|me_operlog.c|operlog输出模块|
|me_persist.c|快照写库模块|
|me_request.c|提供asset,market的初始化,更新函数|
|me_trade.c|提供市场操作函数,如更新market,获取market信息|
|me_update.c|提供资产变动操作函数|



## 主要对象
|对象|数据|说明|
|-|-|-|
|dict_balance|账户资产|
|dict_update|资产变更操作缓存(asset.update接口) |定时处理,间隔60秒. 保留7天|
|dict_asset|币种信息|分account组织|
|dict_market|市场信息|
|dict_user_orders|未完成的用户委托|
|dict_user_stops|未完成的用户计划委托|


# 主要机制

## 启动过程

。读取配置
。初始化币种信息(解析asset_url)
校验数据,如asset的名称长度,币种prec_save是否小于配置的min_save_prec
。初始化市场信息(解析market_url)
。从数据库(trade_log)加载数据：
  从最新的快照中加载(order,stop,balance,update)
  加载operlog: 从最后快照时间戳之后开始
  确定last_oper_id,last_order_id,last_deals_id
。创建子进程(access,writer,reader)
。各进程分别继续初始化：见[上文各进程的启动说明](#processes)
。各进程进入事件循环(nw_loop_run)


## 请求路由

- accesshttp的请求由access进程接收和处理
- access根据请求命令(对应接口的method),分别转发给writer,reader进程.
有修改操作的交易类请求转发给writer进程.
查询请求转发给reader,多个reader分发逻辑如下:
    - 如果请求中指定了unique_id,则reader id按(unique_id%(reader_num-1)  
    - 否则,在reader_num-1个reader中轮流选择
    - 统计查询转发给id最大的reader进程
    -config.update_asset和config.update_market发送给writer和所有reader进程
- 请求的实际处理由writer,reader完成

## 请求处理
基本的请求处理流程如下:
1. 检查参数：
按接口定义检查请求参数.无效则返回"invalid argument",错误码:1
2. 调用功能模块的方法处理请求
3. 释放资源：如mpd_t对象
4. 处理失败返回错误描述和错误码,成功返回响应消息


## 交易处理示例
以writer的限价买入(order.put_limit)为例说明具体的实现。

1. 检查参数(根据[order.put_limit接口说明](https://gitlab.hoogeek.com/jianguo.liu/coinex-doc/-/blob/master/coinex%E4%BA%A4%E6%98%93%E6%9C%8D%E5%8A%A1%E6%8E%A5%E5%8F%A3.md#orderput_limit))
   检查user_id: required,int类型
   检查account: required,int类型
   检查market: required,且存在该市场
   检查side：int类型,1-卖 2-买
   检查amount：买入数量,required,number类型
   检查price: 买入价格,requried,number类型
   检查taker_fee: required,number类型. 且值在有效区间
   检查maker_fee: required,number类型. 且值在有效区间
   检查source: required,string类型.长度不超过31字节.
   检查fee_asset: required,string类型.且必须是交易币对的money
   检查fee_discount: 可选,(0,1)之间
   检查option:可选. 
   检查client_id:可选,长度不超过32字节,由字母,数字,连字符(-),下划线(_)组成

2. 检查可用余额是否足够(买入所需资金+手续费),若余额不足，则返回错误. (error_no=10,error="balance not enough")

3. 检查amount是否小于最小交易数量.若是，则返回错误(error_no=11,error="amount too small")

4. option相关检查

5. 生成订单id，创建订单对象

6. 与市场的卖单(m->asks)匹配：
   以taker身份,从低到高循环与m->asks中的卖单匹配. 
   每个卖单匹配处理如下:
   - 如果left=0,买单已全部成交,则结束.
   - 如果price < 卖单价格,则结束.
   - 以卖单price做为本笔成交价格.
   - 以双方最小的left做为本笔成交数量(amount).
   - 计算成交金额deal
   - 计算卖方手续费(ask_fee): deal\*fee_price\*fee_discount
   卖方(maker)处理:
   - 减少卖方left: 减去amount
   - 减少卖方冻结数量: 减去amount
   - 增加卖方订单成交数量deal_stock: 加上amount
   - 增加卖方订单成交金额deal_money: 加上deal
   - 增加卖方订单手续费asset_fee: 加上ask_fee
   - 减少卖方stock冻结： 减少amount (卖出成交后减少冻结的stock)
   - 记录卖方stock资产变动(his_balances消息)
   - 增加卖方money可用： 增加deal (卖出成交后收到money)
   - 记录卖方money资产变动((his_balances消息)
   如果ask_fee>0,
   - 减少卖方手续费币种的可用数量：减少ask_fee 
   - 记录卖方手续费资产变动((his_balances消息)


   - 计算买方手续费(bid_fee)： deal\*taker_fee\*fee_discount
   - 修改买卖双方订单的update_time为当前时间
   - 生成成交id
   - 生成历史成交(his_deals消息)
   - 生成成交信息(deals消息)
   
   买方(taker)处理:
   - 减少买方left: 减少amount
   - 增加买方订单deal_stock: 增加amount
   - 增加买方订单deal_money: 增加deal
   - 增加买方订单asset_fee:增加bid_fee
   - 减少买方money可用： 减少deal
   - 记录买方money资产变动(his_balances消息)
   - 增加买方stock可用:增加amount
   - 记录买方stock资产变动(his_balances消息)
   如果bid_fee>0,
   - 减少买方手续费币种可用：减少bid_fee
   - 记录买方手续费资产变动(his_balances消息)

   如果买方left=0,则订单状态为已完成,订单结束:
   - 如果订单有冻结(frozen>0),则解冻
   - 删除订单内存对象(m->orders,m->bids,dict_user_orders,m->user_orders)
   - 生成his_orders消息
   - 生成订单完成事件(ORDER_EVENT_FINISH):通过orders消息
   否则,
   - 生成订单更新事件(ORDER_EVENT_UPDATE):通过orders消息

   修改市场最新价:m->last=price 

7. 如果全部成交(left=0),订单状态为完成成交
   - 生成his_orders消息
   - 生成订单完成事件(ORDER_EVENT_FINISH):通过orders消息
   否则，
   - 冻结资产
   - 挂单： 把买单加入m->bids

8. 检查计划委托是否触发

9. 生成operlog
10. 把请求写入共享内存,通知reader,reader接收后再处理


- option说明:
交易目前只支持money做为手续费币种.(窄化了原系统的能力)
OPTION_SUGGEST_STOCK_FEE,OPTION_SUGGEST_MONEY_FEE不起作用.
其它选项目前也未使用. 

## reader进程同步

reader进程同步处理请求的逻辑与writer相似. 差异如reader不会生成消息。
代码中通过real,is_reader控制. 
real的分支只有writer执行.
is_reader的分支只有reader执行.


# 配置

|属性|说明|类型|是否必须|默认|示例|
|-|-|-|-|-|-|
|cli|cli服务入口|string|是||
|asset_url|币种信息的访问url|string|是|
|market_url|市场信息的访问url|string|是|
|min_fee|最低手续费系数|number|否|0|
|max_fee|最高手续费系数|number|否|1.0|
|brokers|kafka服务地址|string,格式:host:port|是|||127.0.0.1:9092|
|slice_interval|快照间隔,单位:秒|int|否|86400|
|slice_keeptime|快照保留时间,单位:秒|int|否|86400*3|
|depth_merge_max|最大合并深度|int|否|1000|
|min_save_prec|最小保存精度|int|否|18|
|discount_prec|折扣精度|int|否|2|
|reader_num|读进程个数|int|否|2|
|cache_timeout|缓存超时时间,单位:秒|number|否|0.1|
|order_fini_keeptime|已完成订单缓存时间,单位:秒|numer|否|300.0||
|worker_timeout|worker进程请求超时时间(http,rpc),单位:秒|number|否|1.0||


完整的配置说明见[配置说明](
https://gitlab.hoogeek.com/jianguo.liu/coinex-doc/-/blob/master/%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E.md#matchengine)



# 运行维护
包括启动失败的情况，运行期错误与处理


## 启动失败
启动失败的情况包括(但不限于):
|错误|错误提示关键字|判定|解决|
|-|-|-|-|
|asset_url,market_url不可访问|load config fail|根据返回的行号判定|确保外部服务可用,重启matchengine|
|加载operlog失败|load_operlog/fail|根据返回的行号判定|见下文|

- 加载operlog失败：
  该问题属于异常情况，<b>通常情况是可以避免的</b>。产生的原因有:
    - 数据损坏(如多服务实例操作相同的数据库)
    - 业务规则改变而升级时未考虑(如手续费变化导致资金余额不足)
    - 程序bug

    解决方法：定位具体出错的operlog记录,修改method为nop,跳过此记录. 事后再对问题进行定性和人工处理。
    <b>目前需要手动处理,可能比较耗时</b>.


## 服务不可用

### 阻塞导致的"service unavailable"
检查日志判断此问题.

日志特征关键字: "fatal" "service unavailable"
日志内容示例:
```
service unavailable, queue: 0, operlog: 0, message: 1
```

当出现以下情形时，会出现此错误：
- 共享队列阻塞：表示reader处理不过来
- operlog阻塞：待写入operlog查过10000条时
- 消息队列阻塞：任一待写入消息队列的数据项超过50000条时

这种错误可能出现在负载增大的情况下，可以自动恢复.需要干预.

### load operlog failed导致的"service unavailable"
运行期间operlog同步处理错误会导致服务不可用.
与启动时加载operlog失败有相同的逻辑. <b>通常是可以避免的</b>。


检查日志判断此问题.

日志特征关键字: "fatal" "service unavailable"
日志示例:
```
[2021-06-16 16:58:10.579970] [16567] [trace]ut_json_rpc.c:126(rpc_reply_json): connection: 127.0.0.1:56584 size: 81, send: {"error": {"code": 3, "message": "service unavailable"}, "result": null, "id": 0}

{"code": 3, "message": "service unavailable"}, "result": null, "id": 0}
[2021-06-16 16:54:41.481802] [16570] [fatal]me_market.c:2710(market_close_order_all): cancel order: 408 fail: -498
[2021-06-16 16:54:41.481844] [16570] [error]me_load.c:1266(load_close_order_all): market_close_order_all market: ETH-USDT, fail: -498
[2021-06-16 16:54:41.481846] [16570] [fatal]me_reader.c:1350(on_message): load operlog failed, ret: -1267, data: {"method": "close_order_all", "params": ["ETH-USDT"]}, queue: 1
```

