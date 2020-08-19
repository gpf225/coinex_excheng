描述：
1. 优化深度缓存逻
2. 更新cachecenter matchengine


部署步骤：
1. 暂停交易 资产更新
2. 替换cachecenter.exe matchengine.exe
3. 在matchengine目录执行./shell/makeslice.sh
4. 重启matchengine ./shell/restart.sh
5. 重启cachecenter ./shell/restart.sh