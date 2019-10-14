# 升级说明

## 更新的模块

### readhistory

* 限价单、stop订单返回新增字段client_id

### historywriter

* 限价单、stop订单插入增加新client_id

### matchengine

* 限价单、stop限价单、stop市价单、市价单支持用户自定义client_id
* 限价单、stop限价单、stop市价单、市价单pending、detail、book返回client_id

## 升级操作

* 执行脚本./script/add_client_id/add_client_id.py 增加对order_history,stop_history,slice_stop,slice_order增加字段client_id(执行前修改add_client_id.py DB_HOST及DB_PASSWD为需要修改的DB信息，然后执行命令：python add_client_id.py modify)
* 暂停交易与充值提现
* 数据切片(makeslice)
* 更新程序
* 重启，重启顺序：
  1、matchengine
  2、readhistory
  3、historywriter
