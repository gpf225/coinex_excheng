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
    //char *a = "{\"id\":1,\"method\":\"server.ping\",\"params\":[]}";
    char *a = "hello";
    //char *a = "{\"id\":314,\"method\":\"state.subscribe\",\"params\":[]}";

    // placeholder for the compressed (deflated) version of "a" 
    char b[100] = {0};

    printf("Uncompressed size is: %lu\n", strlen(a));
    printf("Uncompressed string is: %s\n", a);


    printf("\n----------\n\n");

    // STEP 1.
    // deflate a into b. (that is, compress a into b)
    
    // zlib struct
    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;
    // setup "a" as the input and "b" as the compressed output
    defstream.avail_in = (uInt)strlen(a); // size of input, string + terminator
    defstream.next_in = (Bytef *)a; // input char array    


    sds out_bin = sdsempty();
    deflateInit2(&defstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -MAX_WBITS, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);

    int ret = 0;
    do {
        defstream.avail_out = (uInt)sizeof(b); // size of output
        defstream.next_out = (Bytef *)b; // output char array
        ret = deflate(&defstream, Z_FINISH);    /* no bad return value */
        assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
        int have = sizeof(b) - defstream.avail_out;
        out_bin = sdscatlen(out_bin, b, have);
        printf("1avail out len: %d, have:%d\n", defstream.avail_out, have);
    } while (defstream.avail_out == 0);

    /*
    defstream.avail_in = (uInt)strlen(c); // size of input, string + terminator
    defstream.next_in = (Bytef *)c; // input char array
    do {
        defstream.avail_out = (uInt)sizeof(b); // size of output
        defstream.next_out = (Bytef *)b; // output char array
        ret = deflate(&defstream, Z_FINISH);    
        assert(ret != Z_STREAM_ERROR); 
        int have = sizeof(b) - defstream.avail_out;
        out_bin = sdscatlen(out_bin, b, have);
        printf("2avail out len: %d, have:%d\n", defstream.avail_out, have);
    } while (defstream.avail_out == 0);
    */
    deflateEnd(&defstream);

    sds hex = bin2hex(out_bin, sdslen(out_bin));
    printf("len: %ld, hex: %s\n", sdslen(out_bin), hex);
     
    // This is one way of getting the size of the output
    printf("Compressed size is: %lu\n", strlen(b));
    printf("Compressed string is: %s\n", b);
    //chrome:
    //aa 56ca4c51b232d451ca4d2dc9c80732958a538bca528bf40a32f3d29574940a128b12738b95aca2636b 0100

    //compress
    //ab 56ca4c51b232d451ca4d2dc9c80732958a538bca528bf40a32f3d29574940a128b12738b95aca2636b 01
    //sds hex = bin2hex(b, strlen(b));
    //printf("hex: %s\n", hex);

    printf("\n----------\n\n");

}
