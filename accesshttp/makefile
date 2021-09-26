TARGET  := accesshttp.exe
INCS = -I ../network -I ../utils
LIBS = -L ../utils -lutils -L ../network -lnetwork -lev -ljansson -lmpdec -lrdkafka -lz -lssl -lcrypto -lhiredis -lm -lpthread -ldl
include ../makefile.inc



gcc -g -rdynamic -o matchengine.exe  me_access.o  me_asset.o  me_balance.o  me_config.o  me_dump.o  me_history.o  me_load.o  me_main.o  me_market.o  me_message.o  me_operlog.o  me_persist.o  me_reader.o  me_request.o  me_trade.o  me_update.o  me_writer.o -L ../utils -lutils -L ../network -lnetwork -lev -ljansson -lmpdec -lmysqlclient -lrdkafka -lhiredis -lcurl -lssl -lcrypto -lz -lm -lpthread -ldl -lstdc++