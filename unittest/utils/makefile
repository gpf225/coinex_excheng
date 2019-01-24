SOURCES_RPC_REPLY := test_ut_rpc_reply.c ../../utils/ut_rpc_reply.c 
 
PARAMS_COMMON := -I ../../utils -I ../../utils -L ../../utils -lutils -I ../../network/ -L ../../network/ -lnetwork -lcmockery -lev -lhiredis -ljansson -lmpdec -lcurl -lssl -lcrypto -pthread -Wl,-Bdynamic -lm -std=gnu99 -g -o

TARGET_RPC_REPLY := test_rpc_reply.exe
all:
	gcc ${SOURCES_RPC_REPLY} $(PARAMS_COMMON) ${TARGET_RPC_REPLY}
clean:
	rm -rf ${TARGET_RPC_REPLY}