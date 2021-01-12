#include <stdio.h>
#include <string.h>  // for strlen
#include <assert.h>
#include "ut_misc.h"
#include "zlib.h"

// adapted from: http://stackoverflow.com/questions/7540259/deflate-and-inflate-zlib-h-in-c
int main(int argc, char* argv[])
{   
    
    //char *input = "789caa56ca4c51b232d451ca4d2dc9c80732958a538bca528bf40a32f3d29574940a128b12738b95aca2636b0100";
    //char *input = "789caa062b3542565a925892aa575c9a549c5c9499948aaa1c00";
                 //ab56ca4c51b2323634d151ca4d2dc9c80772948a4b124b52f58a4b938a938b3293529574940a128b12738b95aca2636b01
    char *input = "aa56ca4c51b232d451ca4d2dc9c84f51b2522a4e2d2a4b2dd22bc8cc4b57d2512a482c4acc2d56b28a8ead05000000ffff";
    sds input_bin = hex2bin(input);
    char c[10] = {0};
    // STEP 2.
    // inflate b into c
    // zlib struct
    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;
    // setup "b" as the input and "c" as the compressed output
    infstream.avail_in = (uInt)(sdslen(input_bin)); // size of input
    infstream.next_in = (Bytef *)input_bin; // input char array
    //infstream.avail_out = (uInt)sizeof(c); // size of output
    //infstream.next_out = (Bytef *)c; // output char array
     
    // the actual DE-compression work.
    // inflateInit(&infstream);
    inflateInit2(&infstream, -MAX_WBITS);

    //int ret = inflate(&infstream, Z_NO_FLUSH);
    //inflateEnd(&infstream);

    sds out_bin = sdsempty();
    int have = 0;
    int ret = 0;
    do {
        infstream.avail_out = (uInt)sizeof(c); // size of output
        infstream.next_out = (Bytef *)c; // output char array
        ret = inflate(&infstream, Z_NO_FLUSH);
    
        switch (ret) {
        case Z_NEED_DICT:
            ret = Z_DATA_ERROR;
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            break;
        }

        have = sizeof(c) - infstream.avail_out;
        printf("have: %d\n", have);
        out_bin = sdscatlen(out_bin, c, have);
    } while (infstream.avail_out == 0);
    
    inflateEnd(&infstream);
    printf("inflate ret: %d\n", ret);
    printf("Uncompressed size is: %lu\n", sdslen(out_bin));
    printf("Uncompressed string is: %s\n", out_bin);
    
    return 0;
}
