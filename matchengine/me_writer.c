/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2019/04/25, create
 */

# include "me_config.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_operlog.h"
# include "me_history.h"
# include "me_message.h"
# include "me_asset_backup.h"
# include "ut_queue.h"
# include "me_writer.h"

static rpc_svr *svr;
static queue_t *queue_writers;

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{

}

static int init_server()
{
    if (settings.svr.bind_count != 1)
        return -__LINE__;
    nw_svr_bind *bind_arr = settings.svr.bind_arr;
    if (bind_arr->addr.family != AF_INET)
        return -__LINE__;
    bind_arr->addr.in.sin_port = htons(ntohs(bind_arr->addr.in.sin_port) + 1);

    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    return 0;
}

static int init_queue()
{
    queue_writers = (queue_t *)malloc(sizeof(queue_t) * settings.reader_num);
    memset(queue_writers, 0, sizeof(queue_t) * settings.reader_num);

    for (int i = 0; i < settings.reader_num; ++i) {
        sds queue_name = sdsempty();
        queue_name = sdscatprintf(queue_name, "%s_%d", QUEUE_NAME, i);

        sds queue_pipe_path = sdsempty();
        queue_pipe_path = sdscatprintf(queue_pipe_path, "%s_%d", QUEUE_PIPE_PATH, i);

        key_t queue_shm_key = QUEUE_SHMKEY_START + i;

        int ret = queue_writer_init(&queue_writers[i], NULL, queue_name, queue_pipe_path, queue_shm_key, QUEUE_MEM_SIZE);

        sdsfree(queue_name);
        sdsfree(queue_pipe_path);

        if (ret < 0) {
            log_error("init_queue %d failed", i);
            return ret;
        }
    }
    return 0;
}

int init_writer()
{
    int ret;
    ret = init_queue();
    if (ret < 0) {
        return -__LINE__;
    }

    ret = init_server();
    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}