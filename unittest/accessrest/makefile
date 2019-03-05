SOURCES := test_ar_depth_wait_queue.c ../../accessrest/ar_depth_wait_queue.c ../../accessrest/ar_depth_common.c
 
PARAMS_COMMON := -I ../../accessrest -I ../../utils -L ../../utils -lutils -I ../../network/ -L ../../network/ -lnetwork -lcmockery -lev -lhiredis -ljansson -lmpdec -lcurl -lssl -lcrypto -pthread -Wl,-Bdynamic -lm -std=gnu99 -g -o

TARGET := test_depth_wait_queue.exe
all:
	gcc ${SOURCES} $(PARAMS_COMMON) ${TARGET}
clean:
	rm -rf ${TARGET}