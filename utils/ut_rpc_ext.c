/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2020/10/15, create
 */

# include <stdlib.h>
# include <assert.h>

# include "ut_rpc_ext.h"
# include "ut_crc32.h"
# include "ut_misc.h"


static void rpc_ext_unique_pack(ext_unique_data *data)
{
    data->unique_id = htole32(data->unique_id);
}

static void rpc_ext_unique_decode(ext_unique_data *data)
{
    data->unique_id = le32toh(data->unique_id);
}

int rpc_ext_pack(rpc_pkg *pkg, uint16_t type, uint16_t length, void *data)
{
    if (pkg->ext_size + RPC_PKG_EXT_HEADER_SIZE + length > RPC_PKG_EXT_MAX_SIZE) {
        return -1;
    }

    uint16_t ext_size = pkg->ext_size + RPC_PKG_EXT_HEADER_SIZE + length;
    void *ext = calloc(1, ext_size);
    if (pkg->ext_size > 0) {
        memcpy(ext, pkg->ext, pkg->ext_size);
        free(pkg->ext);
    }

    uint16_t type_le = htole16(type);
    uint16_t length_le = htole16(length);
    switch (type) {
    case RPC_PKG_EXT_TYPE_UNIQUE:
        rpc_ext_unique_pack(data);
        break;
    default:
        free(ext);
        return -2;
    }

    memcpy(ext + pkg->ext_size, &type_le, sizeof(type_le));
    memcpy(ext + pkg->ext_size + sizeof(type), &length_le, sizeof(length_le));
    memcpy(ext + pkg->ext_size + RPC_PKG_EXT_HEADER_SIZE, data, length);
    pkg->ext = ext;
    pkg->ext_size += RPC_PKG_EXT_HEADER_SIZE + length;
    return 0;
}

static void *list_node_dup(void *value)
{
    rpc_ext_item *obj = malloc(sizeof(rpc_ext_item));
    memcpy(obj, value, sizeof(rpc_ext_item));
    return obj;
}

static void list_node_free(void *value)
{
    free(value);
}

static void rpc_item_decode(rpc_ext_item *item)
{
    switch(item->type) {
        case RPC_PKG_EXT_TYPE_UNIQUE:
            rpc_ext_unique_decode(item->data);
            break;
        default:
            break;
    }
    return;
}

list_t *rpc_ext_decode(rpc_pkg *pkg, int *ext_item_count)
{
    if (pkg->ext_size <= RPC_PKG_EXT_HEADER_SIZE) {
        *ext_item_count = 0;
        return NULL;
    }

    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.dup = list_node_dup;
    lt.free = list_node_free;

    *ext_item_count = 0;
    list_t *list = list_create(&lt);
    if (list == NULL)
        return NULL;

    uint16_t curr = 0;
    while (curr < pkg->ext_size) {
        rpc_ext_item item;
        memset(&item, 0, sizeof(item));

        memcpy(&item.type, pkg->ext + curr, sizeof(item.type));
        item.type = le16toh(item.type);
        curr += sizeof(item.type);

        memcpy(&item.length, pkg->ext + curr, sizeof(item.length));
        item.length = le16toh(item.length);
        curr += sizeof(item.length);

        item.data = pkg->ext + curr;
        rpc_item_decode(&item);
        list_add_node_tail(list, &item);
    }
    return list;
}


