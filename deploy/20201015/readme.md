# 升级说明

## 更新的模块
tradesummary
1. 使用redis定时快照
2. 增加taker_fee maker_fee统计

## 升级操作

1. 修改scripts/update_ts_offset/update.py中redis的配置,执行
2. 修改scripts/alter_summary/alter_user_fee.py中mysql的配置,执行
2. 替换tradesummary执行文件, 重启
