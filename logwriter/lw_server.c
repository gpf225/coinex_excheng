/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "lw_config.h"
# include "lw_server.h"

struct instance {
    nw_svr *svr;
    dlog_t *log;
};

static struct instance *instances;

static int decode_pkg(nw_ses *ses, void *data, size_t max)
{
    return max;
}

static void on_recv_pkg(nw_ses *ses, void *data, size_t size)
{
    nw_svr *svr = ses->svr;
    struct instance *ins = svr->privdata;
    sds msg = sdsnewlen(data, size);
    dlog(ins->log, "[%-15s] %s", nw_sock_ip(&ses->peer_addr), msg);
    sdsfree(msg);
}

static int init_instance(struct instance_cfg *cfg, struct instance *ins)
{
    nw_svr_cfg svr_cfg;
    memset(&svr_cfg, 0, sizeof(svr_cfg));
    svr_cfg.bind_count = 1;
    svr_cfg.bind_arr = malloc(sizeof(nw_svr_bind));
    sds bind = sdsempty();
    bind = sdscatprintf(bind, "udp@0.0.0.0:%d", cfg->port);
    nw_sock_cfg_parse(bind, &svr_cfg.bind_arr[0].addr, &svr_cfg.bind_arr[0].sock_type);
    sdsfree(bind);
    svr_cfg.max_pkg_size = 100 * 1000;

    nw_svr_type svr_type;
    memset(&svr_type, 0, sizeof(svr_type));
    svr_type.decode_pkg = decode_pkg;
    svr_type.on_recv_pkg = on_recv_pkg;

    ins->svr = nw_svr_create(&svr_cfg, &svr_type, ins);
    if (ins->svr == NULL)
        return -__LINE__;
    if (nw_svr_start(ins->svr) < 0)
        return -__LINE__;

    ins->log = dlog_init(cfg->log.path, cfg->log.shift | DLOG_NO_TIMESTAMP, cfg->log.max, cfg->log.num, cfg->log.keep);
    if (ins->log == NULL)
        return -__LINE__;

    return 0;
}

int init_server(void)
{
    instances = calloc(settings.instance_count, sizeof(struct instance));
    if (instances == NULL)
        return -__LINE__;

    for (int i = 0; i < settings.instance_count; ++i) {
        int ret = init_instance(&settings.instances[i], &instances[i]);
        if (ret < 0) {
            log_error("init instance %d fail: %d", i, ret);
            return -__LINE__;
        }
    }

    return 0;
}

