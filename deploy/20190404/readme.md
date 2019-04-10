该脚本部署分三个阶段：

第一个阶段部署datamigrator.

第二个阶段部署2个服务，matchengine和historywriter(新增服务)

第三个阶段部署readhistory服务。

# 阶段一部署,部署datamigrator
```
1 拉取master分支代码，执行build_all.sh脚本。
2 datamigrator为新增服务，需要提前创建好相关目录。
3 上传可执行文件以及配置文件。
4 修改配置文件，配置文件有如下需要注意：
"last_user_id": 0,   # 最后迁移成功的user id，主要是为了阶段性迁移。
"least_day_per_user": 7,  # 用户至少保存交易数据天数，默认为7
"max_order_per_user": 50000,  # 用户至少保存交易数据条数，默认5W条
"migrate_start_time": 1554112135.0,  # 本次迁移起始时间，必须受手动配置。
"migrate_end_time": 1.0,     # 本次迁移截止时间，第一次迁移时一般设置为1.0，后续的增量迁移时必须手动配置，配置为上一次迁移设置的migrate_start_time时间。
"migrate_mode": 1   # 迁移模式，1表示全量迁移，全量迁移会采用迁移规则过滤数据，2位增量迁移，增量迁移时不会采用过滤规则。、

例如：
今天第一次开始迁移，当前时间戳为1554343339，那么配置migrate_start_time为1554343339，migrate_end_time为1.0. migrate_mode为1.此时启动数据迁移服务，此时数据迁移服务会采用过滤规则，将[1554343339, 1.0)时间段符合要求的数据迁移过去。

下周一更新服务后需要迁移这几天的数据，此时采用增量迁移策略，此时migrate_start_time为下周一的某个时间戳，migrate_end_time配置为1554343339，即上一次的开始时间，设置migrate_mode的值为2，此时开始迁移。
```



# 阶段2部署，部署matchengine和history_writer
```
1 修改matchengine的配置文件，增加字段"history_mode": 3。3表示双写，1表示只写老的DB，2表示只写kafka，不写老的DB，此参数主要用与平滑断开双写。为了操作安全，实现了一个脚本用于断开双写，参考scripts/history_control.sh脚本。
sh history_control.sh direct控制matchengine的交易历史只写旧库。
sh history_control.sh kafka控制matchengine的交易历史只推kafka。
sh history_control.sh couble控制matchengine的交易历史即写旧库也推kafka，即双写。
2 更新matchengine.exe文件。
本次matchengine.exe更新需要暂停交易，但是不用切片。

3 部署history_writer。注意修改history_writer服务的配置文件，kafka, redis, mysql的配置要注意。
```


# 阶段3部署，部署readhistory
```
1 修改readhistory的配置文件。
2 上传readhistory.exe可执行文件，然后重启服务
```


