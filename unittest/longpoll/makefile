SOURCES := test_lp_depth_sub.c ../../longpoll/lp_depth_sub.c ../../longpoll/lp_common_struct.c 
 
PARAMS_COMMON := -I ../../longpoll -I ../../utils -L ../../utils -lutils -I ../../network/ -L ../../network/ -lnetwork -lcmockery -lev -lhiredis -ljansson -lmpdec -lcurl -lssl -lcrypto -pthread -Wl,-Bdynamic -lm -std=gnu99 -g -o

TARGET := longpoll_test.exe
all:
	gcc ${SOURCES} $(PARAMS_COMMON) ${TARGET}
clean:
	rm -rf ${TARGET}