TARGET  := internalws.exe
INCS = -I ../network -I ../utils
LIBS = -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lrdkafka -lhiredis -lcurl -lssl -lcrypto -lz -Wl,-Bdynamic -lm -lpthread -ldl
include ../makefile.inc
