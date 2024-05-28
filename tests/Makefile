CC       = gcc -O3
SRCS     = speed_test.c

#for macos
MACOSLIB = ../lib.dylib

SHAREDLIB = ./lib.so

PROG	 = speed_test

all: $(PROG)

$(PROG): $(SRCS) $(HDRS)
	ln -fs ../$(SHAREDLIB) $(SHAREDLIB)
	$(CC)  $(SRCS) $(SHAREDLIB) -pthread -o $(PROG)

macos:  $(SRCS) $(HDRS)  $(MACOSLIB)
	$(CC) $(SRCS) $(MACOSLIB) -o $(PROG)

clean: 
	rm -rf *.o *.so *.dylib $(PROG) ENV error.log

