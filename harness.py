#
# 
# harness.py
# written by Elizabeth Reid
# ereid@mit.edu
#
# 

import types
import string
import random
import os
from random import Random


def parse_scores(f):
    table={}
    try:
        FILE = open(f,"r")

        for l in FILE.readlines():
            if (len(l) == 0):
                continue
            field,value=l.split(":")
            table[field.strip()] = float(value)
        FILE.close()
    except IOError:
        print "Error opening file.\n"
    except Exception:
        print "Unknown error", sys.exc_info()[0], sys.exc_info()[1]
    return table


class TestHarness:
    def __init__(self):
        """
        """
    
    def run_test(self, test, seed):
        print "running test '" + test + "' with seed ",
        print seed
        if not os.path.exists(test):
            print "The test harness can't find the dynamic library for test '" + test + "'. Did you remember to build it?"
        else:
            random.seed(seed)
            table={}
            table["NUM_DEADLOCK"] = 0
            table["NUM_TXN_FAIL"] = 0
            table["NUM_TXN_COMP"] = 0
            table["TIME"] = 0

            for i in range (0,2):
                # attempts to copy both lib.so and lib.dylib so that script works on both Linux and MacOS machines.
                os.system("mkdir test; cp lib.so test/lib.so; cp lib.dylib test/lib.dylib")
                os.chdir("test")
                newseed = random.random() * 100000
                os.system("../" + test + " " +  repr(newseed) + " " + repr(table["NUM_DEADLOCK"]) + " " + repr(table["NUM_TXN_FAIL"]) + " " + repr(table["NUM_TXN_COMP"]) + " " + repr(table["TIME"]) + " 30 50")
                os.system("cp speed_test.results ../speed_test.results")
                os.chdir("..")
                table = parse_scores("speed_test.results")
                print "time = " + repr(table["TIME"])
                os.system("rm -rf test")

            # run the unit test
            os.system("mkdir test; cp contest test/contest; cp lib.so test/lib.so; cp lib.dylib test/lib.dylib")
            os.chdir("test")
            ret = os.system("./contest");
            os.chdir("..")
            os.system("rm -rf test")
            if (ret != 0):
                print "failed the unit tests!"
                os.system("echo \"UNITTEST: failed\" >> speed_test.results")
            else:
                os.system("echo \"UNITTEST: passed\" >> speed_test.results")

            print "finished running test '" + test + "'"

#the test harness used to call whatever test is necessary
harness = TestHarness()

#run the speed_test (the seed is subject to change)
harness.run_test("tests/speed_test", 234567)
