# include <stdio.h>
# include <time.h>
# include <jansson.h>
# include "nw_ses.h"
# include "ut_ws_clt.h"
# include "ut_misc.h"
# include "ut_comm_dict.h"

struct ws_clt *w_clt;
void client_depth_subscribe(nw_ses* ses, char *market)
{
    json_t *result = json_object();
    time_t t = time(NULL);
    json_object_set_new(result, "id", json_integer(t));
    json_object_set_new(result, "method", json_string("depth.subscribe"));
    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_integer(10));
    json_array_append_new(params, json_string("0.00000001"));
    json_object_set_new(result, "params", params);

    char *message_str = json_dumps(result, 0);
    if (message_str == NULL)
        return;
    printf("connection: %s size: %zu, send: %s\n", nw_sock_human_addr(&ses->peer_addr), strlen(message_str), message_str);

    ws_send_clt_text(ses, message_str);
    free(message_str);
    json_decref(result);
}

void on_error(nw_ses *ses, const char *error)
{
    printf("On_error: %s\n", error);
}


void on_open(nw_ses *ses)
{
    printf("On_open\n");
    client_depth_subscribe(ses, "BTCUSDT");
}

int on_message(nw_ses *ses, void *message, size_t size)
{
    json_t *msg = json_loadb(message, size, 0, NULL);
    char *recv_msg = json_dumps(msg, 0);
    printf("On message json:%s\n", recv_msg);
    free(recv_msg);
    json_decref(msg);
    return 0;
}

void on_close(nw_ses *ses)
{
    printf("On_close\n");
}

int main(int argc, char *argv[])
{
    if (set_file_limit(1000000) < 0) {
        printf("set_file_limit error\n");
        return -__LINE__;
    }

    if (set_core_limit(1000000000) < 0) {
        printf("set_core_limit error\n");
        return -__LINE__;
    }


    ws_clt_type type;
    type.on_error = on_error;
    type.on_open = on_open;
    type.on_message = on_message;
    type.on_close = on_close;

    ws_clt_cfg cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.name = strdup("coinex");
    cfg.url = strdup("wss://test2socket.coinex.com/");
    dict_types dt;
    dt.hash_function = str_dict_hash_function;
    dt.key_dup = str_dict_key_dup;
    dt.key_destructor = str_dict_key_free;
    dt.key_compare = str_dict_key_compare;
    dt.val_dup = str_dict_key_dup;
    dt.val_destructor = str_dict_key_free;

    cfg.header = dict_create(&dt, 8);
    dict_add(cfg.header, "Host", "test2socket.coinex.com");
    dict_add(cfg.header, "Origin", "http://test3.coinex.com");
    cfg.max_pkg_size = 2048000;
    cfg.heartbeat_timeout = 1;
    cfg.reconnect_timeout = 1;

    w_clt = ws_clt_create(&cfg, &type);
    if (w_clt == NULL) {
        printf("create clt error\n");
        return -1;
    }
    ws_clt_start(w_clt);
    printf("client start\n");
    nw_loop_run();
    return 0;
}