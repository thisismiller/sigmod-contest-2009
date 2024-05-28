
In order to run the example implementation which uses Berkeley DB, you must first have BerkeleyDB installed on your machine. Berkeley DB is an open source transactional embedded data manager and can be found here:

http://www.oracle.com/technology/products/berkeley-db/index.html

A Berkeley DB Reference guide can be found here:

http://www.oracle.com/technology/documentation/berkeley-db/db/ref/toc.html

More specifically, Section 27 covers Building Berkeley DB for UNIX/POSIX, and begins here:

http://www.oracle.com/technology/documentation/berkeley-db/db/ref/build_unix/intro.html

The newest version of Berkeley DB when this file (and the example implementation) was written is 4.7, and so the assumed default installation location for Berkeley DB on your machine is /usr/local/BerkeleyDB.4.7/. If you choose to install it somewhere else, then you will need to change the BASE and possible BDBINC variables in the Makefile in order to compile properly (for example, many Linux installations include BerkeleyDB by default;  in these cases, you can set BASE to /lib and BDBINC to $(BASE)/include/db4/

Once you have Berkeley DB installed on your machine, typing:

make test

should build and execute the supplied unit tests in unittests.c and run them on the implementation in bdbimpl.c. The command

make
./contest

will also run the unit tests, but it does not clean up any data from previous runs of the bdbimpl and so the unit tests will fail if they have been run previously without cleanup.

To build on a MacOS machine, use the command:

make macos

this will generate appropriate dynamic libraries under MacOS (for final submission purposes you must build your code under Linux.)


In order to run the benchmark tests using the provided harness, you must have Python 2.5 running on your machine. To build and execute the provided test, use the command:

make harness

which will generate a dynamic library for any *.c files found in the tests directory and execute the harness.py script. At the moment the script only runs the provided speed_test, but other tests can be added to the script to run. To build a specific dynamic library (if you don't want to recompile all of them each time) from a given *.c file, you can use the command:

make tests/speed_test.so

(replacing "speed_test" with the name of the file being built).

To build and run the test(s) on MacOS, use the command:

make macharness

and to build a the dynamic libraries on MacOS, use the command:

make tests/speed_test.dylib

Other tests can be added to the provided harness by placing a *.c file into the tests directory and adding a call to run_test() in the harness.py file. For a test to be runnable it must have run() method which accepts a random seed given to it by the harness.
