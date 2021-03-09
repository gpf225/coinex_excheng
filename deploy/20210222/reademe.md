# 更新点
1. 修改手续费规则
2. order相关接口返回的数据里，删除deal_fee, 增加money_fee stock_fee
3. slice_order表删除deal_fee 增加money_fee stock_fee
4. order_history表增加money_fee stock_fee

# 升级步骤
1. 停止币币交易 转账
1. 更新代码，编译
1. 替换matchengine readhistory historywriter的二进制文件
2. matchengine makeslice
3. 执行alter_set_slice_table.py，抽查数据
4. 执行alter_history_table.py， 检查表结构
5. 重启matchengine readhistory historywriter
6. 开发交易 转账
7. 执行set_history_table.py
   