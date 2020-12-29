#include <stdio.h>
#include <string.h>  // for strlen
#include <assert.h>
#include "ut_misc.h"
#include "zlib.h"

// adapted from: http://stackoverflow.com/questions/7540259/deflate-and-inflate-zlib-h-in-c
int main(int argc, char* argv[])
{   
    // original string len = 36
    //char *a = "Hello Hello Hello Hello Hello Hello!";
    char *a = "0000ffffaa56ca4c51b232d451ca4d2dc9c80732958a538bca528bf40a32f3d29574940a128b12738b95aca2636b0100";
    sds a_bin = hex2bin(a);

    char *b = "0000ffffaa062b3542565a925892aa575c9a549c5c9499948aaa1c00";
    sds b_bin = hex2bin(b);

    int ret;
    unsigned have;
    unsigned char out[100];

    /* allocate inflate state */
    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    //ret = inflateInit(&strm);
    ret = inflateInit2(&strm, -MAX_WBITS);
    if (ret != Z_OK)
        return ret;    

    sds out_bin = sdsempty();
    /* decompress until deflate stream ends or end of file */
    //do {
        strm.avail_in = sdslen(a_bin);
        strm.next_in = a_bin;
        /* run inflate() on input until output buffer not full */
        do {
            strm.avail_out = sizeof(out);
            strm.next_out = out;
            ret = inflate(&strm, Z_NO_FLUSH);
            /*
            switch (ret) {
            case Z_NEED_DICT:
                printf("1\n");
                ret = Z_DATA_ERROR;
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                printf("2\n");
                break;
            }
            */
            have = sizeof(out) - strm.avail_out;
            printf("have: %d\n", have);
            out_bin = sdscatlen(out_bin, out, have);
        } while (strm.avail_out == 0);
    //} while (ret != Z_STREAM_END);

    //do {
        strm.avail_in = sdslen(b_bin);
        strm.next_in = b_bin;
        /* run inflate() on input until output buffer not full */
        do {
            strm.avail_out = sizeof(out);
            strm.next_out = out;
            ret = inflate(&strm, Z_FINISH);

            /*
            switch (ret) {
            case Z_NEED_DICT:
                ret = Z_DATA_ERROR;
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                break;
            }
            */
            have = sizeof(out) - strm.avail_out;

            printf("have: %d\n", have);
            out_bin = sdscatlen(out_bin, out, have);
        } while (strm.avail_out == 0);
    //} while (ret != Z_STREAM_END);

    printf("out_bin: %s\n", out_bin);

    sds hex = bin2hex(out_bin, sdslen(out_bin));
    printf("len: %ld, hex: %s\n", sdslen(out_bin), hex);
    /* clean up and return */
    (void)inflateEnd(&strm);
    have = sizeof(out) - strm.avail_out;
    printf("have: %d\n", have);
    return 0;
}