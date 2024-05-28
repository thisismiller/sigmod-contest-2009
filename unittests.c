/*
Copyright (c) 2008 MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

 unittests.c
 written by Elizabeth Reid
 ereid@mit.edu
 
Version history:

This is version 1.0, released December 12, 2008.

 */

#include "server.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

IdxState *idx;

char *primary_index = "primary_index";
char *secondary_index = "secondary_index";
char *terciary_index = "terciary_index";

char *a_key = "a_key";
char *b_key = "b_key";
char *c_key = "c_key";
char *d_key = "d_key";

const char *value_one = "value one";
const char *value_two = "value two";
const char *small_payload = "z";

int DID_SECONDARY_PASS = 0;
int DID_TRANSACTION_PASS = 0;

int CAN_STOP_TRANS = 0;

int run_unittests(void);

static void wait_thread(void)
{    
    sleep(1);
}

/*
 Ensures that a separate thread can never access any of the entries inserted into the primary
 DB with the key value b_key (which only exist mid-transaction and so should not appear anywhere else).
 This function keeps running until the main function completes, unless one of the tests fail.
 */
static void *test_transaction_func()
{
    int errCode;
    IdxState *idx;
    TxnState *txn;
    Record record;
    memset(&record, 0, sizeof(Record));
    if ((errCode = openIndex(primary_index, &idx)) != SUCCESS) {
        printf("cannot open primary index from transaction tester thread\n");
        DID_TRANSACTION_PASS = -1;
        return NULL;
    }
    int count = 0;
    while (CAN_STOP_TRANS == 0 || count == 0) {
    loop:
        count++;
        txn = NULL;
        if ((errCode = beginTransaction(&txn)) != SUCCESS) {
            printf("could not begin transaction in test_transaction_func\n");
            if (errCode == DEADLOCK) {
                printf("DEADLOCK received\n");
                if ((errCode = abortTransaction(txn)) != SUCCESS) {
                    printf("could not abort deadlocked transaction\n");
                }
                goto loop;
            }
            DID_TRANSACTION_PASS = -1;
            return NULL;
        }
        
        record.key.type = VARCHAR;
        memcpy(record.key.keyval.charkey, b_key, strlen(b_key)+1);
        if ((errCode = get(idx, txn, &record)) != KEY_NOTFOUND) {
            printf("test_transaction_func found entry with key 'b'\n");
            if (errCode == DEADLOCK) {
                printf("DEADLOCK received\n");
                if ((errCode = abortTransaction(txn)) != SUCCESS) {
                    printf("could not abort deadlocked transaction\n");
                }
                goto loop;
            }
            DID_TRANSACTION_PASS = -1;
            return NULL;
        }
        if ((errCode = commitTransaction(txn)) != SUCCESS) {
            printf("could not end transaction in test_transaction_func\n");
            if (errCode == DEADLOCK) {
                printf("DEADLOCK received\n");
                if ((errCode = abortTransaction(txn)) != SUCCESS) {
                    printf("could not abort deadlocked transaction\n");
                }
                goto loop;
            }
            DID_TRANSACTION_PASS = -1;
            return NULL;
        }
        DID_TRANSACTION_PASS = 1;
    }
    printf("successfully passed transaction test! loop count = %d\n", count);
}

/*
 Tests a secondary index while the primary index is also running. This tests to make sure that it
 can't see any of the data from the primary index, while still behaving normally on its own. It also
 pushes some of the edge cases on DB retreival (empty DBs and the last entry in DB, etc)
 */
static void *secondary_index_func()
{
    printf("entered secondary_index_func\n");
    int errCode;
    IdxState *idx;
    TxnState *txn;
    Record record;
    Key k_a, k_b, k_c, k_d;
    k_a.type = VARCHAR;
    memcpy(k_a.keyval.charkey, a_key, strlen(a_key)+1);
    k_b.type = VARCHAR;
    memcpy(k_b.keyval.charkey, b_key, strlen(b_key)+1);
    k_c.type = VARCHAR;
    memcpy(k_c.keyval.charkey, c_key, strlen(c_key)+1);
    k_d.type = VARCHAR;
    memcpy(k_d.keyval.charkey, d_key, strlen(d_key)+1);
    
    if ((errCode = create(VARCHAR, secondary_index)) != SUCCESS) {
        printf("could not create secondary index\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    if ((errCode = openIndex(secondary_index, &idx)) != SUCCESS) {
        printf("can't open secondary index\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    if ((errCode = beginTransaction(&txn)) != SUCCESS) {
        printf("could not begin transaction in secondary index tester\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    //get(a) on empty DB
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, a_key, strlen(a_key)+1);
    if ((errCode = get(idx, txn, &record)) != KEY_NOTFOUND) {
        printf("get on empty DB did not properly report KEY_NOTFOUND\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    //insert (b, 1)
    if ((errCode = insertRecord(idx, txn, &k_b, value_one)) != SUCCESS) {
        printf("could not insert (b,1) into secondary DB\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    //getNext should return (b, 1)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("getNext on single entry in secondary DB failed\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    } else {
        if ((strcmp(b_key, record.key.keyval.charkey) != 0) || 
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (b, 1) from getNext\n");
            DID_SECONDARY_PASS = -1;
            return NULL;
        }
    }
    
    //getNext should hit DB_END
    if ((errCode = getNext(idx, txn, &record)) != DB_END) {
        printf("getNext does not return DB_END properly\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    //insert (c, 1)
    if ((errCode = insertRecord(idx, txn, &k_c, value_one)) != SUCCESS) {
        printf("could not insert (c, 1) into secondary DB\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    //get(b)
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, b_key, strlen(a_key)+1);
    if ((errCode = get(idx, txn, &record)) != SUCCESS) {
        printf("could not get in secondary DB\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    } else {
        if ((strcmp(b_key, record.key.keyval.charkey) != 0) || 
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return payload (b) from get\n");
            DID_SECONDARY_PASS = -1;
            return NULL;
        }
    }
    
    //getNext should return (c, 1)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("could not getNext in secondary DB\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    } else {
        if ((strcmp(c_key, record.key.keyval.charkey) != 0) || 
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (c,1) from getNext\n");
            DID_SECONDARY_PASS = -1;
            return NULL;
        }
    }
    
    //insert (c, 2)
    if ((errCode = insertRecord(idx, txn, &k_c, value_two)) != SUCCESS) {
        printf("could not insert (c, 2) into secondary DB\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    //getNext should return (c, 2) or DB_END
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        if (errCode != DB_END) {
            printf("could not getNext in secondary DB\n");
            DID_SECONDARY_PASS = -1;
            return NULL;
        }
    } else {
        if ((strcmp(c_key, record.key.keyval.charkey) != 0) || 
            (strcmp(value_two, record.payload) != 0)) {
            printf("failed to return (c,2) from getNext\n");
            DID_SECONDARY_PASS = -1;
            return NULL;
        }
    }
    
    //insert the 'small payload' of one character
    if ((errCode = insertRecord(idx, txn, &k_d, small_payload)) != SUCCESS) {
        printf("could not insert a 1-character payload into secondary DB\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
        
    //make sure that it can be retrieved properly
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, d_key, strlen(d_key)+1);
    if ((errCode = get(idx, txn, &record)) != SUCCESS) {
        printf("could not retrieve small payload\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    } else {
        if ((strcmp(d_key, record.key.keyval.charkey) != 0) ||
            (strcmp(small_payload, record.payload) != 0)) {
            printf("failed to return record with small payload\n");
            DID_SECONDARY_PASS = -1;
            return NULL;
        }
    }
    
    if ((errCode = commitTransaction(txn)) != SUCCESS) {
        printf("could not end transaction in secondary index tester\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    
    if ((errCode = closeIndex(idx)) != SUCCESS) {
        printf("could not close secondary index\n");
        DID_SECONDARY_PASS = -1;
        return NULL;
    }
    printf("successfully passed secondary index tests!\n");
    DID_SECONDARY_PASS = 1;
    return NULL;
}


#ifndef RUNNING_SPEED_TEST
int main(void)
{
    return run_unittests();
}

#endif
 

/*
 This tests the basic behavior of the primary index with two transactions. It pushes some of the edges
 of the getNext() and delete() boundaries, by reaching the end of the DB and deleting all of the values
 under a given key, as well as specific key/payload entries. It also ensures that you don't allow the
 insertion of multiple identical key/payload pairs, and whether you properly abort a transaction so that
 the changes made to the DB are not committed (by aborting after inserting an entry keyed on 'b', for 
 which the test_transaction_func is continuously polling.
 */
int run_unittests(void)
{
    int errCode;
    IdxState *idx;
    TxnState *txn;
    pthread_t tran_test_thread, sec_test_thread;
    
    
    static Key k_a;
    k_a.type = VARCHAR;
    memcpy(k_a.keyval.charkey, a_key, strlen(a_key)+1);
    
    Key k_b;
    k_b.type = VARCHAR;
    memcpy(k_b.keyval.charkey, b_key, strlen(b_key)+1);
    
    Key k_c;
    k_c.type = VARCHAR;
    memcpy(k_c.keyval.charkey, c_key, strlen(c_key)+1);
    
    Key k_d;
    k_d.type = VARCHAR;
    memcpy(k_d.keyval.charkey, d_key, strlen(d_key)+1);
    
    //create the primary index
    if ((errCode = create(VARCHAR, primary_index)) != SUCCESS) {
        printf("could not create primary index\n");
        return EXIT_FAILURE;
    }
  
    if (pthread_create(&tran_test_thread, NULL, test_transaction_func, NULL) != 0) {
        return EXIT_FAILURE;
    }
    
    if (pthread_create(&sec_test_thread, NULL, secondary_index_func, NULL) != 0) {
        return EXIT_FAILURE;
    }
   
    /*
     Expected initial state at this point is that the primary_index is completely empty.
     */
    //open the primary index
    if ((errCode = openIndex(primary_index, &idx)) != SUCCESS) {
        printf("could not open index\n");
        return EXIT_FAILURE;
    }
    
first_txn:
    //begin main transaction
    if ((errCode = beginTransaction(&txn)) != SUCCESS) {
        printf("failed to begin main txn\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //insert tuple (a,1)
    if ((errCode = insertRecord(idx, txn, &k_a, value_one)) != SUCCESS) {
        printf("failed to insert (a, 1) errCode = %i\n", errCode);
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //get unique(a) to return (a,1)
    Record record;
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, a_key, strlen(a_key)+1);
    if ((errCode = get(idx, txn, &record)) != SUCCESS) {
        printf("failed to get when DB contains single record\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(a_key, record.key.keyval.charkey) != 0) || 
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (a,1) from get\n");
            return EXIT_FAILURE;
        }
    }
    
    //insert tuple (b, 1)
    if ((errCode = insertRecord(idx, txn, &k_b, value_one)) != SUCCESS) {
        printf("failed to insert (b,1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //get unique(a) to return (a,1)
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, a_key, strlen(a_key)+1);
    if ((errCode = get(idx, txn, &record)) != SUCCESS) {
        printf("failed to get\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(a_key, record.key.keyval.charkey) != 0) || 
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (a,1) from get\n");
            return EXIT_FAILURE;
        }
    }
    
    //get next to return (b,1)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("failed to getNext\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(b_key, record.key.keyval.charkey) != 0) ||
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (b,1) from getNext\n");
            printf("record key: %s\nrecord payload: %s\n",record.key.keyval.charkey, record.payload);
            return EXIT_FAILURE;
        }
    }
    
    //insert (c, 1)
    if ((errCode = insertRecord(idx, txn, &k_c, value_one)) != SUCCESS) {
        printf("failed to insert (c,1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    
    //insert (c, 1) should not be able to insert identical tuple
    if ((errCode = insertRecord(idx, txn, &k_c, value_one)) != ENTRY_EXISTS) {
        printf("successfully inserted duplicate entry (c,1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //getNext should return (c,1)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("failed to getNext\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(c_key, record.key.keyval.charkey) != 0) || 
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (c,1) from getNext: %s|, %s\n", record.key.keyval.charkey, record.payload);
            return EXIT_FAILURE;
        }
    }
    
    //getNext should fail with error code DB_END
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != DB_END) {
        printf("did not properly find end of DB with getNext\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //delete (c, 1)
    memset(&record, 0, sizeof(Record));
    record.key = k_c;
    memcpy(record.payload, value_one, strlen(value_one)+1);
    if ((errCode = deleteRecord(idx, txn, &record)) != SUCCESS) {
        printf("failed to delete specific entry (c, 1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //get(c) should return nothing
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, c_key, strlen(c_key)+1);
    if ((errCode = get(idx, txn, &record)) != KEY_NOTFOUND) {
        printf("found an entry that should not exist: (%s, %s)\n", record.key.keyval.charkey, record.payload);
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //insert (b,2)
    if ((errCode = insertRecord(idx, txn, &k_b, value_two)) != SUCCESS) {
        printf("failed to insert (b,2)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //get(b) should return (b,1) or (b,2)
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, b_key, strlen(b_key)+1);
    if ((errCode = get(idx, txn, &record)) != SUCCESS) {
        printf("did not properly get unique when keyed on two entries\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(b_key, record.key.keyval.charkey) != 0) ||
            ((strcmp(value_one, record.payload) != 0) && (strcmp(value_two, record.payload) != 0))) {
            printf("failed to return (b,1) or (b,2) from get with two possible payloads\n");
            return EXIT_FAILURE;
        }
    }
    
    //getNext should return (b,2) or (b,1)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("failed to getNext on duplicate key values\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(b_key, record.key.keyval.charkey) != 0) ||
            ((strcmp(value_two, record.payload) != 0) && (strcmp(value_one, record.payload) != 0))) {
            printf("failed to return (b,1) or (b,2) from getNext with duplicate key values\n");
            return EXIT_FAILURE;
        }
    }
    
    //delete (b) should erase all values keyed on b
    Record delete_b;
    memset(&delete_b, 0, sizeof(Record));
    delete_b.key = k_b;
    if ((errCode = deleteRecord(idx, txn, &delete_b)) != SUCCESS) {
        printf("failed to delete multiple payloads on same key\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //get(b) should not find any entries
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, b_key, strlen(b_key)+1);
    if ((errCode = get(idx, txn, &record)) != KEY_NOTFOUND) {
        printf("get on a key which has been deleted did not return KEY_NOTFOUND\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //insert (a, 2)
    if ((errCode = insertRecord(idx, txn, &k_a, value_two)) != SUCCESS) {
        printf("could not insert (a, 2)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //delete (a, 1)
    memset(&record, 0, sizeof(Record));
    record.key = k_a;
    memcpy(record.payload, value_one, strlen(value_one)+1);
    if ((errCode = deleteRecord(idx, txn, &record)) != SUCCESS) {
        printf("could not delete (a, 1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    //get(a) should return (a,2)
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, a_key, strlen(a_key)+1);
    if ((errCode = get(idx, txn, &record)) != SUCCESS) {
        printf("could not get(a) after deleted (a, 1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(a_key, record.key.keyval.charkey) != 0) ||
            (strcmp(value_two, record.payload) != 0)) {
            printf("failed to retrieve (a,2) from get call\n");
            return EXIT_FAILURE;
        }
    }
    
    //end transaction, commit all changes
    if ((errCode = commitTransaction(txn)) != SUCCESS) {
        printf("unable to commit transaction\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto first_txn;
        }
        return EXIT_FAILURE;
    }
    
    /*
     At this point the DB should have only one entry in it: (a, 2)
     */
    
second_txn:
    txn = NULL;
    if ((errCode = beginTransaction(&txn)) != SUCCESS) {
        printf("could not begin second main transaction\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto second_txn;
        }
        return EXIT_FAILURE;
    }
    
    memset(&record, 0, sizeof(Record));
    //getNext before a get should return the first (and only, in this case) entry in a DB: (a,2)
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("could not find expected (a, 2) in reconnected database\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto second_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(a_key, record.key.keyval.charkey) != 0) ||
            (strcmp(value_two, record.payload) != 0)) {
            printf("failed to retrieve (a,2) from getNext\n");
            return EXIT_FAILURE;
        }
    }
    
    memset(&record, 0, sizeof(Record));
    //there should be nothing left in the DB to find
    if ((errCode = getNext(idx, txn, &record)) != DB_END) {
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto second_txn;
        }
        printf("found extra tuple in DB\n");
        return EXIT_FAILURE;
    }
    
    //insert (b, 1)
    if ((errCode = insertRecord(idx, txn, &k_b, value_one)) != SUCCESS) {
        printf("could not insert (b,1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto second_txn;
        }
        return EXIT_FAILURE;
    }
    
    //abort transaction so (b,1) is not committed
    if ((errCode = abortTransaction(txn)) != SUCCESS) {
        printf("unable to abort second main transaction\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto second_txn;
        }
        return EXIT_FAILURE;
    }
    
    //verify that the transaction rolled back properly
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, b_key, strlen(b_key)+1);
verify_abort:
    if ((errCode = get(idx, NULL, &record)) != KEY_NOTFOUND) {
        if (errCode == DEADLOCK) {
            printf("DEADLOCK received\n");
            goto verify_abort;
        }
        printf("aborting a transaction did not roll back properly\n");
        return EXIT_FAILURE;
    }
    
    /*
     This transaction tests that getNext() will return the first key after 
     a failed call to get() with a key not found in the DB.
     */
third_txn:
    txn = NULL;
    if ((errCode = beginTransaction(&txn)) != SUCCESS) {
        printf("could not begin third main transaction\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto second_txn;
        }
        return EXIT_FAILURE;
    }
    
    //insert (d,1)
    if ((errCode = insertRecord(idx, txn, &k_d, value_one)) != SUCCESS) {
        printf("could not insert (d, 1)\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto third_txn;
        }
        return EXIT_FAILURE;
    }
    
    //get(b) should not find any entries
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, b_key, strlen(b_key)+1);
    if ((errCode = get(idx, txn, &record)) != KEY_NOTFOUND) {
        printf("get on a key which has been deleted did not return KEY_NOTFOUND\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto third_txn;
        }
        return EXIT_FAILURE;
    }
    
    //getNext should return (d,1)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("failed to getNext\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto third_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(d_key, record.key.keyval.charkey) != 0) ||
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (d,1) from getNext\n");
            printf("record key: %s\nrecord payload: %s\n",record.key.keyval.charkey, record.payload);
            return EXIT_FAILURE;
        }
    }
    
    //end transaction, commit all changes
    if ((errCode = commitTransaction(txn)) != SUCCESS) {
        printf("unable to commit transaction\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto third_txn;
        }
        return EXIT_FAILURE;
    }
    
    //create a third index
    if ((errCode = create(VARCHAR, terciary_index)) != SUCCESS) {
        printf("could not create terciary index\n");
        return EXIT_FAILURE;
    }
    
    //open the third index
    IdxState *terc_idx;
    if ((errCode = openIndex(terciary_index, &terc_idx)) != SUCCESS) {
        printf("could not open terciary index\n");
        return EXIT_FAILURE;
    }
    
    //get(a) on the primary outside of a transaction should return (a,2)
    memset(&record, 0, sizeof(Record));
    record.key.type = VARCHAR;
    memcpy(record.key.keyval.charkey, a_key, strlen(a_key)+1);
    if ((errCode = get(idx, NULL, &record)) != SUCCESS) {
        printf("could not get(a) without a transaction during multi_tbl_txn\n");
        return EXIT_FAILURE;
    } else {
        if ((strcmp(a_key, record.key.keyval.charkey) != 0) ||
            (strcmp(value_two, record.payload) != 0)) {
            printf("failed to retrieve (a,2) from get call outside of transaction\n");
            return EXIT_FAILURE;
        }
    }
    
    /*
     This txn tests that a transaction behaves properly when using multiple tables.
     */
multi_tbl_txn:
    txn = NULL;
    //begin transaction
    if ((errCode = beginTransaction(&txn)) != SUCCESS) {
        printf("could not begin multi-table main transaction\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto multi_tbl_txn;
        }
        return EXIT_FAILURE;
    }
    
    //getNext should return (a,2)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("failed to getNext after beginning txn\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto multi_tbl_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(a_key, record.key.keyval.charkey) != 0) ||
            (strcmp(value_two, record.payload) != 0)) {
            printf("failed to return (a,2) from getNext\n");
            printf("record key: %s\nrecord payload: %s\n",record.key.keyval.charkey, record.payload);
            return EXIT_FAILURE;
        }
    }
    
    //insert (b,1) into the terciary index
    if ((errCode = insertRecord(terc_idx, txn, &k_b, value_one)) != SUCCESS) {
        printf("could not insert (b, 1) into terciary index\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto multi_tbl_txn;
        }
        return EXIT_FAILURE;
    }
    
    //getNext on the primary index should return (d,1)
    memset(&record, 0, sizeof(Record));
    if ((errCode = getNext(idx, txn, &record)) != SUCCESS) {
        printf("failed to getNext after beginning txn\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto multi_tbl_txn;
        }
        return EXIT_FAILURE;
    } else {
        if ((strcmp(d_key, record.key.keyval.charkey) != 0) ||
            (strcmp(value_one, record.payload) != 0)) {
            printf("failed to return (d,1) from getNext\n");
            printf("record key: %s\nrecord payload: %s\n",record.key.keyval.charkey, record.payload);
            return EXIT_FAILURE;
        }
    }
    
    //commit the transaction
    if ((errCode = commitTransaction(txn)) != SUCCESS) {
        printf("unable to commit multi-table transaction\n");
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked multi-table transaction\n");
            }
            goto multi_tbl_txn;
        }
        return EXIT_FAILURE;
    }
    
    printf("successfully passed main function tests!\n");
    CAN_STOP_TRANS = 1;
    
    pthread_join(tran_test_thread, NULL);
    pthread_join(sec_test_thread, NULL);
    
    if ((DID_SECONDARY_PASS == 1) && (DID_TRANSACTION_PASS == 1)) {
        return EXIT_SUCCESS;
    } else {\
        return EXIT_FAILURE;
    }
}
