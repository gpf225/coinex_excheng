 - [程序结构](#程序结构)
 - [业务逻辑](#业务逻辑)



# 程序结构
  进程关系图如下：
``` dot
digraph {
    M [label="主进程"]

    P1 [label="marketprice_worker_0"]
    P2 [label="marketprice_worker_1"]
    P3 [label="marketprice_worker_2"]
    A [label="marketprice_access"]
    M->A[label="1",style="dashed"]
    M->P1[label="2"]
    P1->P2
    P2->P3
}
```
```
marketprice_worker_%d 表示子进程,内部执行:init_message(),init_server(),init_hisotry();
marketprice_access 表示父进程,内部执行: init_access();

init_server(worker_id)中生成端口为(setting.port + worker_id + 1)的业务处理进程,每个进程中有rpc_svr记作 svr_worker;
init_access()中生成类型为rpc_clt*的数组 worker_arr,worker_arr数组成员个数和业务处理子进程数相等并分别与子进程的 svr_worker 建立连接;
init_access()初始化一个用于被 accesshttp 服务连接的rpc_svr记作 svr_access, 当svr_access收到accesshttp请求到时候通过
    worker_arr成员把请求发送给svr_worker,svr_worker处理完后发回给worker_arr的成员,然后由worker_arr返回给客户的处理结果.

init_message()函数执行 
    init_request()函数,init_request()函数用于向accesshttp服务请求"market.list"和“index.list"业务,
        由mp_message.c文件的on_market_timer()定时执行.
    init_market()函数,请求“market.list"业务,初始化市场信息,后执行init_single_market(),在执行load_market()进而初始化k线;
    kafka_consumer_create()函数,注册on_deals_message()当从kafka消息收到成交信息后执行deal_process()函数,这里更新k线;
        每一笔成交都会更新k线,即在原来的k线基础上更新 open,close,high,low,volume,deal等.    
init_history()函数,on_deals_message()函数会执行kline_history_process(),再更新mysql数据库append_kline_history();
```




# 业务逻辑
    mp_server.c文件初始化业务路由:svr_on_recv_pkg;
|接口|含义|
|-|-|
|market.deals_ext|获取市场最新成交详情|
|market.kline|获取市场k线|
|market.status|获取市场状态|
|market.list|获取某个市场的最新成交价|
|market.deals|获取当前市场的最新成交|
    mp_request.c,用于对accesshttp创建http请求,业务有:"market.list","index.list";
    mp_kline.c用于根据价格处理k线;
    mp_message.c用于接收kafka消息;
    mp_access.c用于接收accesshttp请求和给客户端回复结果.

