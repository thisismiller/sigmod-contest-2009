CC       = gcc -O3
HDRS     = server.h
SRCS     = unittests.c

.SUFFIXES: .dylib .so

# Under many Linux installations, BDB is installed by default in /usr
# In this case, the following defines should be correct
# BASE = /usr
# BDBINC = -I$(BASE)/include/db4/
#
# Under MacOS with fink, you should use
#BASE = /sw
#BDBINC = -I$(BASE)/include/db4/
#
# Use the following if you installed BDB from Oracle in /usr/local/BerkeleyDB.4.7
BASE	 = /usr/local/BerkeleyDB.4.7
BDBINC = -I$(BASE)/include/

BDBLIBS = -ldb
BDBLIBSMACOS = -L $(BASE)/lib/libdb.a

PROG	 = contest

all: $(PROG)

#default BDB based library -- copy in your own lib.so to test it with
# our test cases
lib.so:	$(SRCS) $(HDRS)
	$(CC) -fPIC -shared $(BDBINC) $(BDBLIBS) ./bdbimpl.c -o lib.so
	
.c.so:
	$(CC) -fPIC -shared $(BDBINC) $(BDBLIBS) $*.c -o $*.so

$(PROG): $(SRCS) $(HDRS) lib.so
	$(CC)  $(SRCS) ./lib.so -pthread -o $(PROG)
	
harness: 
	$(MAKE) -C tests/
#	(for j in tests/*.c; do make tests/`basename $$j .c`.so; done)
	python harness.py

#dynamic library targets for macos testing
lib.dylib: $(SRCS) $(HDRS)
	$(CC) -fPIC -dynamiclib $(BDBINC) $(BDBLIBSMACOS) bdbimpl.c -o lib.dylib

.c.dylib:
	$(CC) -dynamiclib $(BDBINC) $(BDBLIBSMACOS) $*.c -o $*.dylib

dummy:
	$(CC) -fPIC -shared ./dummyimpl.c -o lib.so

dummymacos:
	$(CC) -fPIC -dynamiclib ./dummyimpl.c -o lib.dylib

macos:  $(SRCS) $(HDRS) lib.dylib
	$(CC) $(SRCS) lib.dylib -o $(PROG)
	
macharness: 
	$(MAKE) -C tests/ macos
#	(for j in tests/*.c; do make tests/`basename $$j .c`.dylib; done)
	python harness.py

#generates html and latex documentation files in ../docs
doc:
	doxygen Doxyfile

#runs the unit tests with the Berkeley DB implementation
test: $(PROG)
	rm -rf ENV
	./$(PROG)

clean: 
	rm -rf *.o *.so *.dylib tests/*.dylib tests/*.so tests/speed_test $(PROG) ENV error.log

