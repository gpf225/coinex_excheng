描述：
1 修改matchengine,有些稳定币市场的CET折扣根据CETUSDC市场来计算。

部署步骤：
1 进入deploy/20190308目录。
2 执行build.sh脚本(sh build.sh)。
3 修改matchenginet/config.json文件，必须更新，添加字段如下：。
4 更新matchengine，只更新matchengine.exe文件即可。


config.json添加字段如下
```
"usdc_assets": ["TUSD", "PAX", "GUSD"]
```