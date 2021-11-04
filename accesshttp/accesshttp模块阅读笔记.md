- [服务关系](#服务关系)
- [程序结构](#程序结构)
    - [进程](#进程)
        - [worker进程](#worker进程)
        - [listener进程](#listener进程)
    - [程序逻辑](#程序逻辑)


# 服务关系

``` dot
digraph {
    rankdir=TB
    me[shape="record" label="amatchengine"]
    ah[shape="box3d" label="accesshttp"color=blue]
    ah->me

    q[shape="Mrecord" label="<orders> orders|<deals> deals|<his_balances> his_balances|<his_deals> his_deals" xlabel="kafka"]

    me->q

    ts->q:orders[dir="back"]
    ts->q:orders[dir="back"]
    ts->q:deals[dir="back"]
    mp->q:deals[dir="back"]
    hw->q:his_balances[dir="back"]
    hw->q:his_deals[dir="back"]

    ts[shape="record" label="tradesummary"]
    mp[shape="record" label="marketprice"]
    hw[shape="record" label="historywriter"]

    w[shape="record" label="{外部服务|asset_url\nmarket_url}"]
    
    me->w
}
```
```
accesshttp模块对外接收客户端指令.
```

# 程序结构

## 进程
  进程关系图如下：
``` dot
digraph {
    M [label="主进程"]

    P1 [label="accesshttp_worker_0"]
    P2 [label="accesshttp_worker_1"]
    P3 [label="accesshttp_worker_2"]
    A [label="accesshttp_listener"]
    M->A[label="1",style="dashed"]
    M->P1[label="2"]
    P1->P2
    P2->P3
}
```
```
图中实线表示fork的子进程,虚线表示父进程.
主进程根据配置在main函数中fork出指定数目的工作进程,如
图中是三个进程分别是:accesshttp_worker_0,accesshttp_worker_1,accesshttp_worker_2.
accesshttp_listener是该程序的父进程.
```

<span id="processes"></span>
### worker进程
    创建httpsvr用于与建立连接的客户端通信.
    创建各种与其他服务通信的rpc,如:matchengine,marketprice,marketindex,tradesummary
    创建与listener进程中worker_svr通信的rpc.

<span id="processes"></span>
### listener进程
    创建listener_svr,用于监听客户端消息.
    创建worker_svr,用于监听worker子进程的连接.


## 程序逻辑 
    1.lintener进程中执行 init_worker_svr() 函数,初始化worker_svr;
    2.worker进程中执行 init_listener_clt() 函数,创建listener(rpc)连接到linteneer进程中的worker_svr,
    并设置回调函数 on_listener_recv_fd(),等客户端与lintener进程中的listener_svr建立连接后,
    listener_svr会把客户端的通信socket发到worker进程中的listener中(此时listener已经与listener_svr建立连接),
    然后worker进程执行 on_listener_recv_fd() 函数, 把客户端的socket加入httpsvr(ah_server.c中13行)的会话链接中.
    3.接着客户端与httpsvr通信,worker进程会执行 on_http_request() 函数,
    解析消息包后消息会执行 rpc_request_jsonunique() 函数与其他模块通信,如matchengine模块.
    这些处理逻辑的注册是ah_server.c文件中init_methods_handler()函数实现的.
    4.最后执行ah_server.c文件中send_http_response_simple()函数给客户的回复消息.

    