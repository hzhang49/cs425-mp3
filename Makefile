SERVER_EXE = server
SERVER_OBJS = server.o


COMPILER = g++
COMPILER_OPTS = -c -g -O0 -Wall -lpthread
LINKER = g++
LINKER_OPTS = -lpthread

.PHONY: all clean
all : $(SERVER_EXE)

$(SERVER_EXE) : $(SERVER_OBJS)
	$(LINKER) $(SERVER_OBJS) $(LINKER_OPTS) -o $(SERVER_EXE)


chat.o : server.cpp 
	$(COMPILER) $(COMPILER_OPTS) server.cpp


clean :
	-rm -f *.o $(SERVER_EXE)  LINK
