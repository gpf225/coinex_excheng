描述：
1 更新accesshttp服务，主要是为了深度缓存优化，因为需要暂停交易，所以在此版本和matchengine一同发布。
2 修改matchengine服务，
--2.1 去掉深度缓存.
--2.2 清除‘最小可展示精度可用余额’.

部署步骤：
1 执行代码目录下build_all.sh脚本(sh build_all.sh)。
2 部署accesshttp和matchengine.exe

部署1：更新accesshttp服务
1.1 更新config.json配置文件。
```
"cache": {
    "name": "cache",
    "addr": [
        "tcp@127.0.0.1:7801"
    ],
    "max_pkg_size": 2000000
}
```
1.2 上传accesshttp.exe文件，并启动。


部署2：更新matchengine
1 关闭交易
2 生成切片
3 上传matchengine.exe,然后重启启动
4 开放交易

