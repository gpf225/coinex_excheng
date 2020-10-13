# 升级说明

## 更新的模块

matchengine
accesshttp

### accesshttp

* 增加实时接口

### matchengine

* 增加实时接口
* 根据不同接口

## 升级操作

* 执行脚本./script/add_client_id/add_client_id.py 增加对order_history,stop_history,slice_stop,slice_order增加字段client_id(执行前修改add_client_id.py DB_HOST及DB_PASSWD为需要修改的DB信息，然后执行命令：python add_client_id.py modify)
* 暂停交易与充值提现
* 数据切片(makeslice)
* 更新程序
* 重启，重启顺序：
  1、matchengine
  2、readhistory
  3、historywriter
