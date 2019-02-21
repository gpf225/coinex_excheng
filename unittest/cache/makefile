SOURCES := test_ca_depth_cache.c ../../cache/ca_depth_cache.c
 
PARAMS_COMMON := -I ../../cache -I ../../utils -L ../../utils -lutils -I ../../network/ -L ../../network/ -lnetwork -lcmockery -lev -lhiredis -ljansson -lmpdec -lcurl -lssl -lcrypto -pthread -Wl,-Bdynamic -lm -std=gnu99 -g -o

TARGET := depth_cache_test.exe
all:
	gcc ${SOURCES} $(PARAMS_COMMON) ${TARGET}
clean:
	rm -rf ${TARGET}