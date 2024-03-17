.PHONY: clean

CFLAGS := -Wall -O2 -g -ggdb -Werror
LDLIBS := ${LDLIBS} -lrdmacm -libverbs -lpthread
CC := g++

all: pseudo-mm-rdma-server

pseudo-mm-rdma-server: main.cpp pseudo-mm-rdma-server.cpp scm.cpp pseudo-mm-rdma-server.h scm.h
	${CC} ${CFLAGS} -o $@ main.cpp pseudo-mm-rdma-server.cpp scm.cpp ${LDLIBS} 

clean:
	rm -f pseudo-mm-rdma-server
