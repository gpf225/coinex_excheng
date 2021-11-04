# historyreader程序逻辑

```
hr_config.c文件:init_config()->read_config_from_json()

数据库配置: load_db_histories(root,"db_history"),
根据配置文件设置的“db_history"读取连接的数据库个数和相关配置.
        每个数据库中有100个表,user_id与数据库的对应关系是 
        db_index = user_id/(db_history_count * 100)/100,
        user_id与表的对应关系是 table_index = user_id%100;
工作线程配置: read_cfg_int(root,"worker_num"),根据配置文件的"worker_num"配置数据库工作线程数目.


hr_server.c文件:
init_server()->nw_job_create(&jt,settings.worker_num)创建读取数据库的工作线程.
svr_on_recv_pkg()收到数据包后,执行nw_job_add()加入任务队列. 然后在线程池允许的情况下执行on_job()处理业务逻辑:
        hr_reader.c 文件,处理业务的相关函数
```

|api|cmd|db|
|-|-|-|
|asset.history|CMD_ASSET_HISTORY|balance_history_%u|
|order.finished|CMD_ORDER_FINISHED|order_history_%u|
|order.finished_stop|CMD_ORDER_FINISHED_STOP|stop_history_%u|
|order.finished_detail|CMD_ORDER_FINISHED_DETAIL|order_history_%u|
|order.deals|CMD_ORDER_DEALS|user_deal_history_%u|
|market.user_deals|CMD_MARKET_USER_DEALS|user_deal_history_%u|

```      
处理完之后:(多线程中thread_routine()函数执行on_job(),然后通过管道发送消息给另一端,
           在主线程中对管道的另一端接收空格字符,执行on_can_read()函数)
        执行on_job_finish(),在rsp->code==0的情况下执行 rpc_reply_result()给客户端发送结果(accesshttp模块);
```        

# historywriter程序逻辑
```
hw_writer.c文件:
init_writer()函数中建立线程池对象,on_job执行数据库写操作. nw_job_add()在后面执行.

hw_dispatcher.c文件:
init_dispatcher()函数建立dict_sql对象,key(db_index,table_index,business_type) val(sql),
db_index和table_index与user_id的对应关系同上;
hw_dispatcher.h声明的四个函数用于对dict_sql进行填充赋值, 
get_sql(&key)建立map对象,set_sql(&key,sql)对map对象key的val进行赋值,
set_sql()函数中会调用submit_job(),进而执行nw_job_add()提交任务.

hw_message.c文件
init_message()函数建立kafka_consumer_t对象,接收来自kafka的消息,
kafka_consumer_create()函数创建管道,对管道的读端监听读操作,
同时生成一个线程执行rd_kafka_consume()来消费kafka的消息,
如果有消息被接收向管道写入“ ”,触发主线程on_can_read()函数,
进而执行 on_deals_message()等,对hw_dispatcher.h的 dispatch_deal()等回调.

```
