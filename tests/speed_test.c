/*
 * speed_test.c
 * written by Elizabeth Reid
 * ereid@mit.edu
 *
 */

#define RUNNING_SPEED_TEST 1

#include "../server.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <sys/timeb.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <limits.h>
//                              0123456701234567
const int32_t MAX_SHORT_KEY = 0x7FFFFFFFL;
const int64_t MAX_INT_KEY =   0x7FFFFFFFFFFFFFFFLL;

int NUM_TXN_COMP = 0;
int NUM_TXN_FAIL = 0;
int NUM_DEADLOCK = 0;

//these constants are subject to change when the official benchmark is run
int MAX_NUM_INDICES = 50;
int NUM_POP_INSERTS = 300;      //# inserts a thread does while populating the indices
int NUM_TESTS_PER_THREAD = 300; //# tests a thread runs

pthread_mutex_t TXN_COMP_LOCK = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t TXN_FAIL_LOCK = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t DEADLOCK_LOCK = PTHREAD_MUTEX_INITIALIZER;

int *randSeed;

pthread_mutex_t TOTAL_LOCK =  PTHREAD_MUTEX_INITIALIZER;
int total = 0;

int globalTime = 0;

//use this to switch between 64-bit and 32-bit systems easily
#define uint uint64_t



// Global variables for the randomly generated constants for the test
int nIndices;

// Array of index name strings
char **indexNames;

// Array of index key types
KeyType *indexTypes;



// Array of random number array counters (by thread)
int *randNumCounter;

// Array of arrays of random numbers (one for each thread)
int **randNumArrays;


// Array of random short keys
int *shortKeys;

// Array of random int keys
int *intKeys;

// Array of random charvar keys
char **strKeys;

// Array of random payloads
char **payloads;

int run(int seed);

char* p_randStr(int size)
{
    char *dst = malloc(size * sizeof(char));
    
    static const char text[] =  "abcdefghijklmnopqrstuvwxyz"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    
    int i, len = rand() % (size - 1);
    for (i = 0; i < len; i++) {
        dst[i] = text[rand() % (sizeof(text) - 1)];
    }
    dst[i] = '\0'; //terminates the string with a NULL
    
    return dst;
}

int32_t myrand(int threadNum) 
{

    int counter = randNumCounter[threadNum];
    randNumCounter[threadNum] = counter+1;
    
    int *randNumArray = randNumArrays[threadNum];
    return randNumArray[counter];
}

/*
 Initialize all of the randomly generated variables here after seeding the RNG.
 */
void initialize(unsigned seed) 
{
  
    srand(seed);
    //trash one rand() call -- first one behaves oddly
    rand();
    
    //how many indices will be used in this test
    nIndices = (rand() % (MAX_NUM_INDICES -1)) + 1;
    nIndices = (rand() % (MAX_NUM_INDICES -1)) + 1;
    indexNames = malloc(nIndices * sizeof(char*));
    indexTypes = malloc(nIndices * sizeof(KeyType));
    
    //create the index names
    int i;
    for (i = 0; i < nIndices; i++) {
        char *indexName = malloc(8 * sizeof(char));
        sprintf(indexName, "index%i", i);
        indexNames[i] = indexName;
        KeyType type = (rand() % 3);
        indexTypes[i] = type;
    }
    
    //create the seed locking mechanism for each thread
    randSeed = malloc(MAX_NUM_INDICES * sizeof(int));
    for (i = 0; i < MAX_NUM_INDICES; i++) {
        randSeed[i] = rand();
    }
    
    //initialize the random data-related arrays
    randNumCounter = malloc(MAX_NUM_INDICES * sizeof(int));
    randNumArrays = malloc(MAX_NUM_INDICES * sizeof(int*));
    
    //create random shortkey array (for all threads to share) of size MAX_NUM_INDICES * NUM_POP_INSERTS
    shortKeys = malloc(MAX_NUM_INDICES * NUM_POP_INSERTS * sizeof(int32_t));
    for (i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
        shortKeys[i] = rand();
    }
    
    //create random intkey array (for all threads to share)
    intKeys = malloc(MAX_NUM_INDICES * NUM_POP_INSERTS * sizeof(int32_t));
    for (i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
        intKeys[i] = rand();
    }
    
    //create random charkey array (for all threads to share)
    strKeys = malloc(MAX_NUM_INDICES * NUM_POP_INSERTS * sizeof(char*));
    for (i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
        strKeys[i] = p_randStr(MAX_VARCHAR_LEN);
    }
    
    //create random payload string array (for all threads to share)
    payloads = malloc(MAX_NUM_INDICES * NUM_POP_INSERTS * sizeof(char*));
    for (i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
        payloads[i] = p_randStr(MAX_PAYLOAD_LEN - 1);
    }
    
    //change generate_key to pull from the appropriate random locations
    
    //2 random numbers for each insert: which index to insert into, key and payload
    int numRandNums = NUM_POP_INSERTS * (3) 
    //33 rand nums for each test: which index tested, type of test, # times test is run, and 30 keys + 1 payload
                    + NUM_TESTS_PER_THREAD * (3 + 30 + 1);
    
    //make the sub-arrays for the random numbers and strings
    for (i = 0; i < MAX_NUM_INDICES; i++) {
        int *randNumArray = malloc(numRandNums * sizeof(int));
        randNumArrays[i] = randNumArray;
    }
    
    //initialize the counters to zero
    memset(randNumCounter, 0, MAX_NUM_INDICES * sizeof(int));
    
    //generate all random data for all of the threads, one thread at a time
    for (i = 0; i < MAX_NUM_INDICES; i++) {
        int *randNumArray = randNumArrays[i];
        int j;
        for (j = 0; j < numRandNums; j++) {
            randNumArray[j] = rand();
        }
    }
}


/*
 Generates a random NULL-terminated string no longer than 'size' and places it into 'dst'.
 dst must be pre-allocated.
 */
char* generate_payload(char *dst, int threadNum)
{
    int counter = randNumCounter[threadNum];
    randNumCounter[threadNum] = counter+1;
    
    int *randNumArray = randNumArrays[threadNum];
    int randNum = (randNumArray[counter] % (MAX_NUM_INDICES * NUM_POP_INSERTS));

    strncpy(dst, (char*)(payloads[randNum]), MAX_PAYLOAD_LEN + 1);
    return dst;
}

/*
 Generates a random value for the given Key.
 */
static void generate_key(Key *key, int threadNum)
{
    int counter = randNumCounter[threadNum];
    randNumCounter[threadNum] = counter+1;
    
    int *randNumArray = randNumArrays[threadNum];
    int index = randNumArray[counter];
    
    switch (key->type) {
        case SHORT:
            index = index % (MAX_NUM_INDICES * NUM_POP_INSERTS);
            key->keyval.shortkey = (shortKeys[index] % (MAX_SHORT_KEY - 1)) + 1;
            break;
        case INT:
            index = index % (MAX_NUM_INDICES * NUM_POP_INSERTS - 1);
            key->keyval.intkey = ((((int64_t)intKeys[index] << 32) | (int64_t)intKeys[index+1]) % (MAX_INT_KEY - 1)) + 1;
            break;
        case VARCHAR:
            index = index % (MAX_NUM_INDICES * NUM_POP_INSERTS);
            strncpy(key->keyval.charkey, strKeys[index], sizeof(key->keyval.charkey));
          break;
    }
}

static void *create_test_index(void *arg)
{
    uint indexNum = (uint)arg;
    int             ret;
    IdxState        *idxState;
    TxnState        *txnState;
    
//    printf("Creating index named %s\n", indexNames[indexNum]);
    //create the index
    if ((ret = create(indexTypes[indexNum], indexNames[indexNum])) != SUCCESS) {
        printf("failed to create index %i. ErrCode = %i\n", indexNum, ret);
        return NULL;
    }
}

/*
 Populate.
 */
static void *populate(void *arg)
{
    int             ret;
    uint        threadNum = (uint)arg;
    IdxState        *idxState;
    
    int numPopulateInserts = 0;
populate_insert:
    while (numPopulateInserts < NUM_POP_INSERTS) {
        numPopulateInserts++;
        
        int32_t indexNum = myrand(threadNum) % nIndices;
        
        //open a random index
        if ((ret = openIndex(indexNames[indexNum], &idxState)) != SUCCESS) {
	  //            printf("thread %i failed to open index %i. ErrCode = %i\n", threadNum, indexNum, ret);
            //for an unexpected error, scrap this insert completely and try again
            goto populate_insert;
        }
        
        //make a random key and payload to insert
        char payload[MAX_PAYLOAD_LEN + 1];
        memset(payload, 0, MAX_PAYLOAD_LEN + 1);
        
        Key key;
        memset(&key, 0, sizeof(Key));
        key.type = indexTypes[indexNum];
        
        //generate the key
        generate_key(&key, threadNum);
        
        //generate the payload
        generate_payload(payload, threadNum);
        
    insert:
        //insert the record
        if (((ret = insertRecord(idxState, NULL, &key, payload)) != SUCCESS) && (ret != ENTRY_EXISTS)) {
            if (ret == DEADLOCK) {
                if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                    printf("can't acquire DEADLOCK_LOCK.\n");
                }
                NUM_DEADLOCK++;
                pthread_mutex_unlock(&DEADLOCK_LOCK);
                goto insert; //try again until not deadlocked
            }
            //not a deadlock
            printf("thread %i failed to insert record for index %i. ErrCode = %i\n", threadNum, indexNum, ret);
        }
        
        //close the index
        if ((ret = closeIndex(idxState)) != SUCCESS) {
            printf("could not close index %s\n", indexNames[indexNum]);
        }
    }
}

/*
 return SUCCESS if txn should be commited
 DEADLOCK if txn should be reattempted because of deadlock
 FAILURE if txn should be aborted
 */
static int scan_test(IdxState *idxState, TxnState *txnState, int threadNum, int indexNum)
{
    int ret;
    
    int32_t K = (myrand(threadNum) % 100) + 100; // uniformly distributed between 100 and 200
    
    Record record;
    memset(&record, 0, sizeof(Record));
    
    record.key.type = indexTypes[indexNum];
    generate_key(&(record.key), threadNum);
    
    if (((ret = get(idxState, txnState, &record)) != SUCCESS) && (ret != KEY_NOTFOUND)) {
        if (ret == DEADLOCK) {
            if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                printf("can't acquire DEADLOCK_LOCK.\n");
            }
            NUM_DEADLOCK++;
            pthread_mutex_unlock(&DEADLOCK_LOCK);
            return DEADLOCK;
        }
        
        if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
            printf("can't acquire TXN_FAIL_LOCK.\n");
        }
        NUM_TXN_FAIL++;
        pthread_mutex_unlock(&TXN_FAIL_LOCK);
        
        return FAILURE;
    }
    
    
    int count = 0;
    while (count < K) {
        count++;
        
        memset(&record, 0, sizeof(Record));
        if (((ret = getNext(idxState, txnState, &record)) != SUCCESS) && (ret != DB_END)) {
            if (ret == DEADLOCK) {
                if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                    printf("can't acquire DEADLOCK_LOCK.\n");
                }
                NUM_DEADLOCK++;
                pthread_mutex_unlock(&DEADLOCK_LOCK);
                return DEADLOCK;
            }
            
            if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                printf("can't acquire TXN_FAIL_LOCK.\n");
            }
            NUM_TXN_FAIL++;
            pthread_mutex_unlock(&TXN_FAIL_LOCK);
            
            return FAILURE;
        }
    }
    
    return SUCCESS;
}

/*
 return SUCCESS if txn should be commited
 DEADLOCK if txn should be reattempted because of deadlock
 FAILURE if txn should be aborted
 */
static int get_test(IdxState *idxState, TxnState *txnState, int threadNum, int indexNum)
{
    int ret;
   
    int32_t L = (myrand(threadNum) % 10) + 20; // uniformly distributed between 20 and 30
    
    Record record;

    
    int count = 0;
    while (count < L) {
        count++;
        
        memset(&record, 0, sizeof(Record));
        
        record.key.type = indexTypes[indexNum];
        generate_key(&(record.key), threadNum);
        
        if (((ret = get(idxState, txnState, &record)) != SUCCESS) && (ret != KEY_NOTFOUND)) {
            if (ret == DEADLOCK) {
                if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                    printf("can't acquire DEADLOCK_LOCK.\n");
                }
                NUM_DEADLOCK++;
                pthread_mutex_unlock(&DEADLOCK_LOCK);
                return DEADLOCK;
            }
            
            if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                printf("can't acquire TXN_FAIL_LOCK.\n");
            }
            NUM_TXN_FAIL++;
            pthread_mutex_unlock(&TXN_FAIL_LOCK);
            
            return FAILURE;
        }
    }
    
    return SUCCESS;
}

/*
 return SUCCESS if txn should be commited
 DEADLOCK if txn should be reattempted because of deadlock
 FAILURE if txn should be aborted
 */
static int update_test(IdxState *idxState, TxnState *txnState, int threadNum, int indexNum)
{
    int ret;
    

    int32_t M = (myrand(threadNum) % 5) + 5; // uniformly distributed between 5 and 10
    
    int count = 0;
    while (count < M) {
        count++;
        
        //make a random key/payload pair to insert
        Key key;
        memset(&key, 0, sizeof(Key));
        key.type = indexTypes[indexNum];
        
        //generate the key
        generate_key(&key, threadNum);
        
        //generate the payload
        char payload[MAX_PAYLOAD_LEN + 1];
        memset(payload, 0, MAX_PAYLOAD_LEN + 1);
        generate_payload(payload, threadNum);
        
        if (((ret = insertRecord(idxState, txnState, &key, payload)) != SUCCESS) && (ret != ENTRY_EXISTS)) {
            if (ret == DEADLOCK) {
                if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                    printf("can't acquire DEADLOCK_LOCK.\n");
                }
                NUM_DEADLOCK++;
                pthread_mutex_unlock(&DEADLOCK_LOCK);
                return DEADLOCK;
            }
            
            if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                printf("can't acquire TXN_FAIL_LOCK.\n");
            }
            NUM_TXN_FAIL++;
            pthread_mutex_unlock(&TXN_FAIL_LOCK);
            
            return FAILURE;
        }
        
        //delete a random key
        Record record;
        memset(&record, 0, sizeof(Record));
        record.key.type = indexTypes[indexNum];
        
        //generate the key
        generate_key(&record.key, threadNum);
        
        if (((ret = deleteRecord(idxState, txnState, &record)) != SUCCESS) && (ret != KEY_NOTFOUND)) {
            if (ret == DEADLOCK) {
                if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                    printf("can't acquire DEADLOCK_LOCK.\n");
                }
                NUM_DEADLOCK++;
                pthread_mutex_unlock(&DEADLOCK_LOCK);
                return DEADLOCK;
            }
            
            if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                printf("can't acquire TXN_FAIL_LOCK.\n");
            }
            NUM_TXN_FAIL++;
            pthread_mutex_unlock(&TXN_FAIL_LOCK);
            
            return FAILURE;
        }
        
    }
    
    return SUCCESS;
}

/*
 Test.
 */
static void *test(void *arg)
{
    int             ret;
    uint        threadNum = (uint)arg;
    IdxState        *idxState;
    TxnState        *txnState;
    int             testCounter = 0;
    
test_loop:
    while (testCounter < NUM_TESTS_PER_THREAD) {
        testCounter++;
        //determine what kind of test this will be
        int32_t testType = myrand(threadNum) % 10;
        //determine on which index it will be run
        int32_t indexNum = myrand(threadNum) % nIndices;
        
        //open the index
        if ((ret = openIndex(indexNames[indexNum], &idxState)) != SUCCESS) {
            if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                printf("can't acquire TXN_FAIL_LOCK.\n");
            }
            NUM_TXN_FAIL++;
            pthread_mutex_unlock(&TXN_FAIL_LOCK);
            
            //for an unexpected error, scrap this insert completely and try again
            goto test_loop;
        }
        

    test_transaction:        
        //start the transaction
        if ((ret = beginTransaction(&txnState)) != SUCCESS) {
            if (ret == DEADLOCK) {
                if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                    printf("can't acquire DEADLOCK_LOCK.\n");
                }
                NUM_DEADLOCK++;
                pthread_mutex_unlock(&DEADLOCK_LOCK);
                goto test_transaction;
            }
            printf("failed to begin populate txn for index %i. ErrCode = %i\n", indexNum, ret);
            
            if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                printf("can't acquire TXN_FAIL_LOCK.\n");
            }
            NUM_TXN_FAIL++;
            pthread_mutex_unlock(&TXN_FAIL_LOCK);
            
            goto close_index;
        }
        
        if (testType < 1) {         //10% scan
            ret = scan_test(idxState, txnState, threadNum, indexNum);
        } else if (testType < 4) {  //30% get
            ret = get_test(idxState, txnState, threadNum, indexNum);
        } else {                    //60% update
            ret = update_test(idxState, txnState, threadNum, indexNum);
        }
        
        if (ret == SUCCESS) {
            //if the test ran successfully, commit txn
            if ((ret = commitTransaction(txnState)) != SUCCESS) {
                if (ret == DEADLOCK) {
                    if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                        printf("can't acquire DEADLOCK_LOCK.\n");
                    }
                    NUM_DEADLOCK++;
                    pthread_mutex_unlock(&DEADLOCK_LOCK);
                    goto close_index;
                }
                
                if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                    printf("can't acquire TXN_FAIL_LOCK.\n");
                }
                NUM_TXN_FAIL++;
                pthread_mutex_unlock(&TXN_FAIL_LOCK);
                
                goto close_index;
            }

            //note that this txn completed successfully
            if (pthread_mutex_lock(&TXN_COMP_LOCK) != 0) {
              printf("can't acquire TXN_COMP_LOCK.\n");
            }
            NUM_TXN_COMP++;
            pthread_mutex_unlock(&TXN_COMP_LOCK);
            
        } else {
            //otherwise, abort txn
            if ((ret = abortTransaction(txnState)) != SUCCESS) {
                if (ret == DEADLOCK) {
                    if (pthread_mutex_lock(&DEADLOCK_LOCK) != 0) {
                        printf("can't acquire DEADLOCK_LOCK.\n");
                    }
                    NUM_DEADLOCK++;
                    pthread_mutex_unlock(&DEADLOCK_LOCK);
                    goto close_index;
                }
                
                if (pthread_mutex_lock(&TXN_FAIL_LOCK) != 0) {
                    printf("can't acquire TXN_FAIL_LOCK.\n");
                }
                NUM_TXN_FAIL++;
                pthread_mutex_unlock(&TXN_FAIL_LOCK);
                
                goto close_index;
            }
        }
        
    close_index:
        //close the index
        if ((ret = closeIndex(idxState)) != SUCCESS) {
            printf("Thread %i failed to close an index after a test was run. ErrCode = %i\n", threadNum, ret);
        }
    }
}

int main(int argc, char **argv)
{

  if (argc >= 8) {
    int seed = atoi(argv[1]);
    NUM_DEADLOCK = atoi(argv[2]);
    NUM_TXN_FAIL = atoi(argv[3]);
    NUM_TXN_COMP = atoi(argv[4]);
    globalTime = atoi(argv[5]);
    NUM_POP_INSERTS = atoi(argv[6]);
    NUM_TESTS_PER_THREAD = atoi(argv[7]);
    printf("speed_test called with %d populate inserts per thread and %d tests per thread\n", 
	   NUM_POP_INSERTS, NUM_TESTS_PER_THREAD);
    run(seed);
  } else {
    printf("speed_test called with %d populate inserts per thread and %d tests per thread\n", 
	   NUM_POP_INSERTS, NUM_TESTS_PER_THREAD);
    run(0);
  }
    //print the results in the output file
    FILE *results = fopen("speed_test.results", "w");
    if (results == NULL) {
        printf("Couldn't open speed_test.results file.\n");
    }
    fprintf(results, "NUM_DEADLOCK: %i\n", NUM_DEADLOCK);
    fprintf(results, "NUM_TXN_FAIL: %i\n", NUM_TXN_FAIL);
    fprintf(results, "NUM_TXN_COMP: %i\n", NUM_TXN_COMP);
    fprintf(results, "TIME: %i\n", globalTime);
    
    fclose(results);
}

int run(int seed)
{
    struct timezone tz;    
    struct timeval start, start2, end;
    printf("\nRunning the Speed Test, seed = %d\n", seed);

    
    //make MAX_NUM_INDICES threads to be used throughout the test
    pthread_t *threads = malloc(MAX_NUM_INDICES * sizeof(*threads));
    
    //initialize all of the random data using the given seed
    initialize(seed);
    
    gettimeofday(&start,&tz);
    
    //branch off into nIndices threads and populate and create each index separately
    uint i;
    printf("Creating %i indices.\n", nIndices);
    for (i = 0; i < nIndices; i++) {
      if (pthread_create(&threads[i], NULL, create_test_index, (void *)i) != 0) {
            printf("could not branch off create thread %i\n", i);
        }
    }
    
    //prevent the main thread from continuing until all of the subthreads are done
    for (i = 0; i < nIndices; i++) {
        pthread_join(threads[i], NULL);
    }
    
    printf("Populating indices.\n", nIndices);
    //have ~50 threads populating all of the indices at the same time
    for (i = 0; i < MAX_NUM_INDICES; i++) {
        if (pthread_create(&threads[i], NULL, populate, (void*)i) != 0) {
            printf("could not branch off populate thread %i\n", i);
        }
    }
    
    //prevent the main thread from continuing until all of the subthreads are done
    for (i = 0; i < MAX_NUM_INDICES; i++) {
        pthread_join(threads[i], NULL);
    }
    

    gettimeofday(&end,&tz);
    printf("Time to populate: %i milliseconds.\n", (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000);

    gettimeofday(&start2, &tz);
    //have ~50 threads run the three transactional tests over the indices at the same time
    printf("Testing the indices.\n");
    for (i = 0; i < MAX_NUM_INDICES; i++) {
        if (pthread_create(&threads[i], NULL, test, (void*)i) != 0) {
            printf("could not branch off test thread %i\n", i);
        }
    }
    
    //prevent the main thread from continuing until all of the subthreads are done
    for (i = 0; i < MAX_NUM_INDICES; i++) {
        pthread_join(threads[i], NULL);
    }
    
    gettimeofday(&end,&tz);
    printf("Time to test: %i milliseconds.\n", (end.tv_sec - start2.tv_sec) * 1000 + (end.tv_usec - start2.tv_usec) / 1000);
    
    printf("Testing complete.\n");
    printf("\tNUM_DEADLOCK: %i\n", NUM_DEADLOCK);
    printf("\tNUM_TXN_FAIL: %i\n", NUM_TXN_FAIL);
    printf("\tNUM_TXN_COMP: %i\n", NUM_TXN_COMP);

    gettimeofday(&end,&tz);
    printf("Overall time to run: %i milliseconds.\n", (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000);

    globalTime += (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

    //clean up
    printf("\nCleaning up.\n");
    free(threads);
    for (i = 0; i < nIndices; i++) {
        free(indexNames[i]);
    }
    free(indexNames);
    free(indexTypes);
    free(randSeed);
}

