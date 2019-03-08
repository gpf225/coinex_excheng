描述：
1 修改accessrest获取deal信息最大限制和默认值。
2 修改accessrest获取kline信息最大限制和默认值。
3 顺便将depth limit默认值也做了优化。

部署步骤：
1 进入deploy/20190307目录。
2 执行build.sh脚本(sh build.sh)。
3 修改accessrest/config.json文件，可以不添加字段，不添加将提供默认值，添加字段如下。
4 更新accessrest，只更新accessrest.exe文件即可。


```
"depth_limit_default": 20,
"kline_max": 1000,
"kline_default": 100,
"deal_max": 1000,
"deal_default": 100
```