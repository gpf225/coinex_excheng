TARGET  := libnetwork.a
INCS = -I ../network -I ../utils
LIBS = -L ../utils -lutils
include ../makefile.inc
$(TARGET) : $(OBJS)
	ar -rs $@ $^
