# 升级说明

## 更新的模块
internalws

## 升级操作
配置文件
```
{
    "debug": false,
    "process": {
        "file_limit": 1000000,
        "core_limit": 1000000000
    },
    "log": {
        "path": "log/internalws",
        "flag": "fatal,error,warn,info,debug",
        "num": 40
    },
    "alert": {
        "host": "internalws",
        "addr": "172.31.25.127:4444"
    },
    "svr": {
        "bind": "tcp@0.0.0.0:9000",
        "max_pkg_size": 10240,
        "protocol": "chat",
	    "compress": false
    },
    "worker_num": 2,
    "timeout": 1.0,
    "matchengine": {
        "name": "matchengine",
        "addr": [
            "tcp@172.31.25.127:7316"
        ],
        "max_pkg_size": 1000000
    },
    "marketprice": {
        "name": "marketprice",
        "addr": [
            "tcp@172.31.17.154:7416"
        ],
        "max_pkg_size": 1000000
    },
    "readhistory": {
        "name": "readhistory",
        "addr": [
            "tcp@172.31.19.232:7516"
        ],
        "max_pkg_size": 1000000
    },
    "cache_deals": {
        "name": "cache_deals",
        "addr": [
            "tcp@172.31.25.127:7802"
        ],
        "max_pkg_size": 2000000,
        "heartbeat_timeout": 30
    },
    "brokers": "172.31.25.174:9092,172.31.17.195:9092,172.31.18.190:9092",
    "cachecenter_host": "172.31.25.127",
    "cachecenter_port": 7810,
    "cachecenter_worker_num": 4,
    "cache_timeout": 0.5,
    "backend_timeout": 2.0,
    "depth_limit": [1, 5, 10, 20, 30, 50],
    "depth_merge": ["0", "0.0000000001", "0.000000001", "0.00000001", "0.0000001", "0.000001", "0.00001", "0.0001", "0.001", "0.01", "0.1", "1", "10", "100"]
}
```
