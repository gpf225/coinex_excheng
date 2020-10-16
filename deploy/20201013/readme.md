# 升级说明

## 更新的模块

matchengine
cachecenter
accesshttp
accessws

### accesshttp

* 增加实时接口

### matchengine

* 增加实时接口
* 根据不同接口采取不同的方式映射到不同读进程

## 升级操作

* 暂停交易与充值提现
* 更新程序
* 对matchengine进行数据切片(makeslice)
* 重启 matchengine
* 重启其他模块
