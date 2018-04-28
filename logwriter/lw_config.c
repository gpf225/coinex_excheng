/*
 * Description: 
 *     History: yang@haipo.me, 2018/04/24, create
 */

# include "lw_config.h"

struct settings settings;

int load_instances(json_t *root, const char *key)
{
    json_t *node = json_object_get(root, key);
    if (!node || !json_is_array(node))
        return -__LINE__;

    settings.instance_count = json_array_size(node);
    settings.instances = malloc(sizeof(struct instance_cfg) * settings.instance_count);

    for (int i = 0; i < settings.instance_count; ++i) {
        json_t *item = json_array_get(node, i);
        if (!item || !json_is_object(item))
            return -__LINE__;
        ERR_RET_LN(read_cfg_int(item, "port", &settings.instances[i].port, true, 0));
        ERR_RET_LN(load_cfg_log(item, "log",  &settings.instances[i].log));
    }

    return 0;
}

int do_load_config(json_t *root)
{
    int ret;
    ret = load_instances(root, "instances");
    if (ret < 0) {
        printf("load instances fail: %d\n", ret);
        return -__LINE__;
    }

    return 0;
}

int load_config(const char *path)
{
    json_error_t error;
    json_t *root = json_load_file(path, 0, &error);
    if (root == NULL) {
        printf("json_load_file from: %s fail: %s in line: %d\n", path, error.text, error.line);
        return -__LINE__;
    }
    if (!json_is_object(root)) {
        json_decref(root);
        return -__LINE__;
    }
    int ret = do_load_config(root);
    if (ret < 0) {
        json_decref(root);
        return ret;
    }
    json_decref(root);

    return 0;
}

