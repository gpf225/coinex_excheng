/*
 * Description: 
 *     History: yangxiaoqiang@viabtc.com, 2020/12/25, create
 */

# include <stdlib.h>
# include <stdio.h>
# include <string.h>
# include <zlib.h>
# include "ut_misc.h"

int main(int argc, char* argv[])
{
    //char *src_buf = "{\"id\":2,\"method\":\"state.subscribe\",\"params\":[]}";
    //char *src_buf = "{\"id\":1,\"method\":\"server.ping\",\"params\":[]}";
    char *src_buf = "hello world from xyang";
    unsigned long src_len = strlen(src_buf);

    unsigned long dst_len = compressBound(src_len);
    printf("maked len:  %ld\n", dst_len);
    unsigned char *dst_buf = calloc(1, dst_len);
    int ret = compress(dst_buf, &dst_len, (Bytef *)src_buf, src_len);
    if (ret != Z_OK) {
        printf("compress fail: %d\n", ret);
    }

    printf("compress over, compress len: %ld, compress str: %s\n", dst_len, dst_buf);
    sds hex = bin2hex(dst_buf, dst_len);
    printf("hex: %s\n", hex);

    unsigned char *src_buf_origin = calloc(1, src_len);
    uncompress(src_buf_origin, &src_len, (Bytef *)dst_buf, dst_len);
    if (ret != Z_OK) {
        printf("uncompress fail: %d\n", ret);
    }
    printf("src_buf_origin: %s\n", src_buf_origin);

                      //789cab 56ca4c51b232d451ca4d2dc9c80732958a538bca528bf40a32f3d29574940a128b12738b95aca2636b012f210e3d
    char *decode_buf = "789caa56ca4c51b232d451ca4d2dc9c80732958a538bca528bf40a32f3d29574940a128b12738b95aca2636b01";
    sds decode_buf_bin = hex2bin(decode_buf);

    unsigned long ubuf_len = sdslen(decode_buf_bin) + 1000;
    unsigned char *ubuf = (unsigned char*)calloc(1, ubuf_len);
    ret = uncompress(ubuf, &ubuf_len, (Bytef *)decode_buf_bin, sdslen(decode_buf_bin));
    if (ret != Z_OK) {
        printf("uncompress fail: %d\n", ret);
    }

    printf("uncompress over, uncompress len: %ld, uncompress str: %s\n", ubuf_len, ubuf);

    sds ubuf_hex = bin2hex(ubuf, ubuf_len);
    printf("uncompress hex: %s\n", ubuf_hex);
    return 0;
}

// apt-get install gcc-multilib
// gcc -g test_zlib.c -o test_zlib.exe -Wl,-Bstatic -lz -fPIC -Wl,-Bdynamic -ldl -lm -Wall -Wno-strict-aliasing -Wno-uninitialized -g -rdynamic -std=gnu99
