描述：
1 增加longpoll服务。
2 增加cache服务。
3 修改accessrest服务。
4 修改accessws服务。
5 修改accesshttp服务。

部署步骤：
1 进入deploy/20190312目录。
2 执行build.sh脚本(sh build.sh)。

步骤1：部署cache服务
1.1 cache是新服务，需要先在服务器上创建对应的文件夹。
1.2 复制cache.exe，config.json, restart.sh这3个文件至指定文件夹。
1.3 然后启动cache.exe。


步骤2：部署longpoll服务
2.1 longpoll是新服务，需要先在服务器上创建对应的文件夹。
2.2 复制longpoll.exe，config.json, restart.sh这3个文件至指定文件夹。
2.3 然后启动longpoll.exe。


步骤3：更新accessrest服务
3.1 先备份accessrest.exe和config.json文件。
3.2 更新accessrest的配置文件config.json。
```
"cache": {
    "name": "cache",
    "addr": [
        "tcp@127.0.0.1:7801"
    ],
    "max_pkg_size": 2000000
},
"longpoll": {
    "name": "longpoll",
    "addr": [
        "tcp@127.0.0.1:10001"
    ],
    "max_pkg_size": 2000000
},



"depth_limit_max": 50,
```
注意,longpoll和cache只能在某一台机器上部署，因此两台机器的accessrest配置文件的IP地址是不一样的。
3.3 上传accessrest.exe文件，并启动。

---accessws和accesshttp暂缓部署，等待accessrest稳定以后再部署---
步骤4：更新accessws服务
4.1 更新config.json配置文件。
```
"cache": {
    "name": "cache",
    "addr": [
        "tcp@127.0.0.1:7801"
    ],
    "max_pkg_size": 2000000
},
"longpoll": {
    "name": "longpoll",
    "addr": [
        "tcp@127.0.0.1:10001"
    ],
    "max_pkg_size": 2000000
}
```
4.2 上传accessws.exe文件，并启动。


步骤5：更新accesshttp服务
5.1 更新config.json配置文件。
```
"cache": {
    "name": "cache",
    "addr": [
        "tcp@127.0.0.1:7801"
    ],
    "max_pkg_size": 2000000
}
```
5.2 上传accesshttp.exe文件，并启动。
