TARGET  := init_migration
INCS = -I ../../network -I ../../utils
LIBS = -L ../../utils -lutils -L ../../network -lnetwork -lhoard -lev -ljansson -lmpdec -lmysqlclient -lrdkafka -lhiredis -lcurl -lssl -lcrypto -lz -lm -lpthread -ldl -lstdc++

SOURCE  := $(wildcard *.cpp)
OBJS    := $(patsubst %.cpp, %.o, $(SOURCE))
CC      := g++
CFLAGS  := -Wall -Wno-strict-aliasing -Wno-uninitialized -rdynamic -std=c++11
LFLAGS  := -rdynamic

.PHONY : all clean install

all : $(TARGET)

clean :
	rm -rf *.d *.o $(TARGET)

$(TARGET) : $(OBJS)
	$(CC) $(LFLAGS) -o $@ $(OBJS) $(LIBS)
.cpp.o :
	$(CC) $(CFLAGS) -c -o $@ $< $(INCS)

install :
