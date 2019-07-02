#一. 更新的模块：
* 1 matchengine： 支持option字段指定使用money或stock扣费
* 2 historywrite: 增加option字段
* 3 readhistory:  增加读取option字段
* 4 accessws:     asset与market长度修改成30
* 5 cachecenter:  market长度修改成30
* 6 tradesummary: asset长度修改成30

#二. 配置文件
* 无需修改

#三. 升级操作：先升级数据，再重启服务
* 1 在scripts/option\_fee下: 执行alter\_option.sh脚本: 
  1.1 修改slice\_example表: 1.增加option字段; 2.修改精度20改成24(balance表:balance, order表:frozen, deal\_fee, asset\_fee)  
  1.2 修改history表: 增加option字段

* 2 暂停交易与充值提现，数据切片。
* 3 停止matchengine
* 4 web修改asset_url对应的 "prec_save":20 改成24
* 5 重启 matchengine
* 6 重启 historywrite
* 7 重启 readhistory
