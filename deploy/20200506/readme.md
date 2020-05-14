描述：
1. k线写历史
2. 修复marketprice的offset问题,改成各个work进程根据work_id写对应offset
3. 新增交易区实际交易量的统计
4. 修复marketindex内存泄漏


部署步骤：
1. 创建kline_history_example表
2. 配置marketprice的config.json的db_history：用于写历史k线
3. 停止marketprce，配置add_work_offset.py参数并执行：用于配置各个work进程对应的offset
4. 更新重启marketprice
5. 更新重启cachecentor
6. 更新重启marketindex
7. 等新版本marketprice运行大概一天后，配置kline_dump.py参数，执行脚本：把k线数据dump到数据库中  原因: 最新的kline_dump.py去掉了start_dump_time参数即dump所有redis的数据,为了避免漏数据，先运行新marketprice一天
