/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2020/10/15, create
 */

# ifndef _UT_RPC_EXT_H_
# define _UT_RPC_EXT_H_

# include <stdint.h>
# include "ut_rpc.h"
# include "ut_list.h"
# include "nw_ses.h"

# define RPC_PKG_EXT_HEADER_SIZE    4
# define RPC_PKG_EXT_MAX_SIZE       65536

# define RPC_PKG_EXT_TYPE_UNIQUE    1

# pragma pack(1)
typedef struct ext_unique_data {
    uint32_t unique_id;
} ext_unique_data;
# pragma pack()

typedef struct rpc_ext_item
{
    uint16_t  type;
    uint16_t  length;
    void      *data;
} rpc_ext_item;

int rpc_ext_pack(rpc_pkg *pkg, uint16_t type, uint16_t length, void *data);
list_t *rpc_ext_decode(rpc_pkg *pkg);

# endif

