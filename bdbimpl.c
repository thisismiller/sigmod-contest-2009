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

 bdbimpl.c
 written by Elizabeth Reid
 ereid@mit.edu
 
Version history:

This is version 1.02, released February 25, 2009.
 
 Adjusted the ordering of integral keys to account for the byte order of the machine
 as well as the sign of the integer.

 
 
Older versions:
 
1.01, Made all changes necessary to support version 1.03 of the API specified in server.h:
 
 - Clarified behavior when getNext is called after get returns
 KEY_NOT_FOUND
 
 - Added a new structure, TxnState, that records information about the
 current transaction.  This structure is passed into all API calls.
 
 - Required that transactions be able to span multiple indices.
 
 - Changed the Payload in the Record structure so it is a fixed size array, 
 and clarified that the caller is responsible for allocating this structure
 
 - Changed get/getNext so that they take in only a Record, with the key
 field populated to indicate what record is to be retrieved.
 
1.0, Initial release, December 12, 2008.

 */

#include <db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>

#include "server.h"

uint indexCt = 0;
FILE *stderrfile;
DB_ENV *env;

#define ENV_DIRECTORY "ENV"

#define DEFAULT_HOMEDIR "./"

pthread_mutex_t DBLINK_LOCK = PTHREAD_MUTEX_INITIALIZER;

const char NULL_PAYLOAD[MAX_PAYLOAD_LEN + 1];

typedef struct 
    {
        DB          *dbp;
        KeyType     type;
        const char  *db_name;
        uint32_t    tid;
        Key         lastKey;
        int         keyNotFound;
    } BDBState;

typedef struct CursorLink
    {
        DBC                 *cursor;
        DB                  *dbp;
        struct CursorLink   *cursorLink;
    } CursorLink;

typedef struct
    {
        CursorLink  *cursorLink;
        DB_TXN      *tid;
    } TXNState;

typedef int bool;

typedef struct DBLink
    {
        char            *name;
        DB              *dbp;
        KeyType         type;
        int             numOpenThreads;
        struct DBLink   *link;
    } DBLink;

DBLink *dbLookup;


/*
 Translates the information stored in Key k and inserts it into the DBT key's relevant fields.
 @return -1 if the key type specified in k is invalid.
 */
int p_setKeyDataFromKey(Key *k, DBT *key)
{
    switch (k->type) {
        case SHORT:
        {
            //the key is a 32-bit integer
            //hack: put the converted int after the plain int
            uint8_t *data = ((uint8_t *) k->keyval.charkey) + 4;
            uint32_t i = k->keyval.shortkey;
            data[3] = (i & 0xFF);
            data[2] = (i & 0xFF00) >> 8;
            data[1] = (i & 0xFF0000) >> 16;
            data[0] = (i & 0xFF000000) >> 24;
            data[0] ^= 0x80;
            key->data = data;
            key->size = 4;
            break;  
        }
        case INT:
        {
            //the key is a 64-bit integer
            //hack: put the converted int after the plain int
            uint8_t *data = ((uint8_t *) k->keyval.charkey) + 8;
            uint64_t i = k->keyval.intkey;
            data[7] = (i & 0xFFLL);
            data[6] = (i & 0xFF00LL) >> 8;
            data[5] = (i & 0xFF0000LL) >> 16;
            data[4] = (i & 0xFF000000LL) >> 24;
            data[3] = (i & 0xFF00000000LL) >> 32;
            data[2] = (i & 0xFF0000000000LL) >> 40;
            data[1] = (i & 0xFF000000000000LL) >> 48;
            data[0] = (i & 0xFF00000000000000LL) >> 56;
            data[0] ^= 0x80;
            key->data = data;
            key->size = 8;
            break;
        }
        case VARCHAR:
            //the key is a <128-byte string
            key->data = k->keyval.charkey;
            key->size = strlen(k->keyval.charkey);
            key->ulen = key->size;
            break;
        default:
            return -1;
    }
    return 0;
}

ErrCode p_createEnv()
{
    int ret;
    
    //create the environment
    if ((ret = db_env_create(&env, 0)) != 0) {
        fprintf(stderrfile, "could not create environment. err = %d\n", ret);
        pthread_mutex_unlock(&DBLINK_LOCK);
        return FAILURE;
    }
    
    //create a directory to store environment info into
    struct stat sb;
    if ((ret = stat(ENV_DIRECTORY, &sb)) != 0) { 
        ret = mkdir(ENV_DIRECTORY, S_IRWXU);
        
        if (ret != 0) {
            (void)env->close(env, 0);
            fprintf(stderrfile, "could not create env directory. err = %d\n", ret);
            pthread_mutex_unlock(&DBLINK_LOCK);
            return FAILURE;
        }
    }
    
    if ((ret = env->set_lk_detect(env, DB_LOCK_DEFAULT)) != 0) {
        env->err(env, ret, "set_lk_detect: DB_LOCK_DEFAULT");
        pthread_mutex_unlock(&DBLINK_LOCK);
        return FAILURE;
    }
    
    //open the environment, giving it the directory
    if ((ret = env->open(env, ENV_DIRECTORY, 
                         DB_CREATE | DB_INIT_LOCK | DB_INIT_LOG |
                         DB_INIT_MPOOL | DB_INIT_TXN | DB_RECOVER | DB_THREAD,
                         S_IRUSR | S_IWUSR)) != 0) {
        (void)env->close(env, 0);
        fprintf(stderrfile, "env->open: %s: %s\n",
                ENV_DIRECTORY, db_strerror(ret));
        pthread_mutex_unlock(&DBLINK_LOCK);
        return FAILURE;
    }
    
    return SUCCESS;
}

ErrCode create(KeyType type, char *name)
{
    DB *dbp;
    int ret;
    //lock the dblink system
    if ((ret = pthread_mutex_lock(&DBLINK_LOCK)) != 0) {
        printf("can't acquire mutex lock: %d\n", ret);
    }
    
    //make sure that the name specified is not already in use
    DBLink *link = dbLookup;
    while (link != NULL) {
        if (strcmp(name, link->name) == 0) {
            break;
        } else {
            link = link->link;
        }
    }
    
    if (link != NULL) {
        pthread_mutex_unlock(&DBLINK_LOCK);
        return DB_EXISTS;
    }
    
    //create a file to store error message for database
    //(if doesn't already exist)
    if (stderrfile == NULL) {
        char errFileName[] = "error.log";
        stderrfile = fopen(errFileName, "w");
        if (stderrfile == NULL) {
            pthread_mutex_unlock(&DBLINK_LOCK);
            return FAILURE;
        }
    }
    
    //if there is no environment, make one
    if (env == NULL) {
        ret = p_createEnv();
        if (ret != SUCCESS) {
            return ret;
        }
    }
    
    /* Initialize the DB handle */
    if ((ret = db_create(&dbp, env, 0)) != 0) {
        fprintf(stderrfile, "could not create DB. err = %d\n", ret);
        pthread_mutex_unlock(&DBLINK_LOCK);
        return FAILURE;
    }
    
    
    //set the error file for the DB
    dbp->set_errfile(dbp, stderrfile);
    
    //set the db to handle duplicates (flag must be set before db is opened)
    dbp->set_flags(dbp, DB_DUPSORT);
    
    //store the DB info in our db lookup list
    
    //make a new link object
    DBLink *newLink = malloc(sizeof(DBLink));
    memset(newLink, 0, sizeof(DBLink));
    
    //populate it
    newLink->name = name;
    newLink->dbp = dbp;
    newLink->type = type;
    newLink->numOpenThreads = 0;
    newLink->link = NULL;
    
    //insert it into the linked list headed by dbLookup
    if (dbLookup == NULL) {
        dbLookup = newLink;
    } else {
        DBLink *thisLink = dbLookup;
        while (thisLink->link != NULL) {
            thisLink = thisLink->link;
        }
        thisLink->link = newLink;
    }
    
    //unlock the dblink system, because we're done editing it
    pthread_mutex_unlock(&DBLINK_LOCK);
    
    //create a file to support the database
    dbp->set_errpfx(dbp, name);
    
    return SUCCESS;
}

ErrCode openIndex(const char *name, IdxState **idxState)
{
    int ret;
    DB *dbp;
    //lock the dblink system
    if ((ret = pthread_mutex_lock(&DBLINK_LOCK)) != 0) {
        printf("can't acquire mutex lock: %d\n", ret);
    }
    
    //look up the DBLink for the index of that name
    DBLink *link = dbLookup;
    while (link != NULL) {
        if (strcmp(name, link->name) == 0) {
            break;
        } else {
            link = link->link;
        }
    }
    
    //if no link was found, index was never create()d
    if (link == NULL) {
        pthread_mutex_unlock(&DBLINK_LOCK);
        return DB_DNE;
    }
    
    //add this thread to the link's thread counter
    link->numOpenThreads++;
    
    //if numOpenThreads == 1, we need to open the index
    if (link->numOpenThreads == 1) {
        dbp = link->dbp;
        //open the DB here so that it only happens once
        if ((ret = dbp->open(dbp,
                             NULL,
                             name,
                             NULL,
                             DB_BTREE,
                             DB_AUTO_COMMIT | DB_CREATE | DB_THREAD, 
                             S_IRUSR | S_IWUSR)) != 0) {
            fprintf(stderrfile, "could not open index %s. errno %d. closing index.\n",link->name, ret);
            if ((ret = dbp->close(dbp, 0)) != 0) {
                fprintf(stderrfile,"could not close index %s, either. err: %i\n", link->name, ret);
            }
            pthread_mutex_unlock(&DBLINK_LOCK);
            return FAILURE;
        }
    }
    
    //create a BDBState variable for this thread
    BDBState *state = malloc(sizeof(BDBState));
    memset(state, 0, sizeof(BDBState));
    *idxState = (IdxState *) state;
    state->dbp = link->dbp;
    state->type = link->type;
    state->db_name = name;
    
    //unlock the dblink system
    pthread_mutex_unlock(&DBLINK_LOCK);
    
    return SUCCESS;
}

ErrCode closeIndex(IdxState *ident)
{
    int ret;
    BDBState *state = (BDBState*)ident;
    DB *dbp = state->dbp;
    
    //lock the dblink system
    if ((ret = pthread_mutex_lock(&DBLINK_LOCK)) != 0) {
        printf("can't acquire mutex lock: %d\n", ret);
    }
    
    //check to see if the DB currently exists
    DBLink *prevLink = NULL;
    DBLink *link = dbLookup;
    while (link != NULL) {
        if (strcmp(state->db_name, link->name) == 0) {
            break;
        } else {
            prevLink = link;
            link = link->link;
        }
    }
    
    //if the DB isn't in our linked list, it never existed
    if (link == NULL) {
        fprintf(stderrfile, "closeIndex called on an index that does not exist\n");
        pthread_mutex_unlock(&DBLINK_LOCK);
        return DB_DNE;
    }
    
    //don't try to close the index even if no threads are using it because it's too slow
    state->dbp = NULL;
    pthread_mutex_unlock(&DBLINK_LOCK);
    return SUCCESS;
    
    /*
    //decrement the number of threads for whom this index is open
    link->numOpenThreads--;
    //remove this DBP from this thread's state
    state->dbp = NULL;
    
    // if there are still threads using this index, don't close it
    if (link->numOpenThreads > 0) {
        pthread_mutex_unlock(&DBLINK_LOCK);
        return SUCCESS;
    }
    
    if ((ret = dbp->close(dbp, 0)) != 0) {
        fprintf(stderrfile, "could not close index. errno %d\n", ret);
        pthread_mutex_unlock(&DBLINK_LOCK);
        return FAILURE;
    }
    
    if (link->numOpenThreads < 0) {
        printf("link->numOpenThreads somehow got to < 0. Resetting to 0.\n");
        link->numOpenThreads = 0;
    }
    
    pthread_mutex_unlock(&DBLINK_LOCK);
    return SUCCESS;    
     */
}


ErrCode beginTransaction(TxnState **txn)
{
    int ret;
    //create the state variable for this transaction
    TXNState *txnState = malloc(sizeof(TXNState));
    DB_TXN *tid = NULL;
    
    //if this is the first call, we need to make the environment
    if (env == NULL) {
        ret = p_createEnv();
        if (ret != SUCCESS) {
            free(txnState);
            return ret;
        }
    }
    
    //begin the transaction
    if ((ret = env->txn_begin(env, NULL, &tid, 0)) != 0) {
        env->err(env, ret, "DB_ENV->txn_begin");
        if (ret == DB_LOCK_DEADLOCK) {
            free(txnState);
            return DEADLOCK;
        }
        free(txnState);
        return FAILURE;
    }
    
    txnState->cursorLink = NULL;
    *txn = (TxnState*)txnState;
    txnState->tid = tid;
    
    return SUCCESS;
}

ErrCode abortTransaction(TxnState *txn)
{
    int ret;
    TXNState *txnState = (TXNState*)txn;
    DB_TXN *tid = txnState->tid;
    
    if (tid == NULL) {//if there is no transaction to abort, that's confusing
        fprintf(stderrfile, "cannot abort transaction because none has begun\n");
        return TXN_DNE;
    }
    
    if (env == NULL) { //shouldn't ever be able to get into this state
        fprintf(stderrfile, "cannot abort transaction before environment is initialized\n");
        return FAILURE;
    }
    
    //close all of the txn's cursors
    CursorLink *cursorLink = txnState->cursorLink;
    while (cursorLink != NULL) {
        DBC *cursor = cursorLink->cursor;
#if DB_VERSION_MINOR>=7
        if ((ret = cursor->close(cursor)) != 0) {
#else
        if ((ret = cursor->c_close(cursor)) != 0) {
#endif
            env->err(env, ret, "DB_TXN->abort, cursor->close");
            if (ret == DB_LOCK_DEADLOCK) {
                return DEADLOCK;
            }
            return FAILURE;
        }
        CursorLink *oldLink = cursorLink;
        txnState->cursorLink = cursorLink->cursorLink;
        cursorLink = cursorLink->cursorLink;
        free(oldLink);
    }
    
    //abort the txn
    if ((ret = tid->abort(tid)) != 0) {
        env->err(env, ret, "DB_TXN->abort");
        if (ret == DB_LOCK_DEADLOCK) {
            return DEADLOCK;
        }
        return FAILURE;
    }
    
    free(txnState);
    return SUCCESS;
}

#pragma mark commitTransaction
ErrCode commitTransaction(TxnState *txn)
{
    int ret;
    TXNState *txnState = (TXNState*)txn;
    DB_TXN *tid = txnState->tid;
    
    if (tid == NULL) {//if there is no txn to end, that's confusing
        fprintf(stderrfile, "cannot end transaction because none exist\n");
        return TXN_DNE;
    }
    
    if (env == NULL) { //shouldn't be able to reach this state
        fprintf(stderrfile, "cannot end transaction before environment is initialized\n");
        return FAILURE;
    }
    
    //close all of the txn's cursors
    CursorLink *cursorLink = txnState->cursorLink;
    while (cursorLink != NULL) {
        DBC *cursor = cursorLink->cursor;
#if DB_VERSION_MINOR>=7
        if ((ret = cursor->close(cursor)) != 0) {
#else
        if ((ret = cursor->c_close(cursor)) != 0) {
#endif
            env->err(env, ret, "DB_TXN->abort, cursor->close");
            if (ret == DB_LOCK_DEADLOCK) {
                return DEADLOCK;
            }
            return FAILURE;
        }
        CursorLink *oldLink = cursorLink;
        txnState->cursorLink = cursorLink->cursorLink;
        cursorLink = cursorLink->cursorLink;
        free(oldLink);
    }
    
    //commit the txn, which also ends it
    if ((ret = tid->commit(tid, 0)) != 0) {
        env->err(env, ret, "DB_TXN->commit");
        if (ret == DB_LOCK_DEADLOCK) {
            return DEADLOCK;
        }
        return FAILURE;
    }
    
    free(txnState);
    return SUCCESS;
}
    
    
#pragma mark p_prepTxnCursor
/*
Determine if a transaction is currently in progress. If not, create one (keeping *txn's value NULL). 
Then check to see if there's a cursor for this index, and creates & adds to index/txn state if not, 
and places the cursor in the **cursor parameter.
 */
ErrCode p_prepTxnCursor(BDBState *state, TxnState *txn, TXNState **txnState, DBC **cursor) {
    int ret;
    DB *dbp = state->dbp;
    //if this is not part of a larger transaction, temporarily create one to use
    if (txn == NULL) {
        ret = beginTransaction((TxnState**)txnState);
        if (ret != SUCCESS) {
            return ret;
        }
    } else {
        txnState = (TXNState**)&txn;
    }
    
    //determine if a cursor exists for this transaction/index combo already
    CursorLink *prevCursorLink = NULL;
    CursorLink *cursorLink = (*txnState)->cursorLink;
    while (cursorLink != NULL) {
        if (cursorLink->dbp == state->dbp) {
            *cursor = cursorLink->cursor;
            break;
        } else {
            prevCursorLink = cursorLink;
            cursorLink = cursorLink->cursorLink;
        }
    }
    
    //if the txnState variable didn't have a cursor for this index, make one
    if (*cursor == NULL) {
        state->keyNotFound = 0;
        if ((ret = dbp->cursor(dbp, (*txnState)->tid, cursor, 0)) != 0) {
            dbp->err(dbp, ret, "Creating new cursor in get()");
            return FAILURE;
        }
        
        //add the data for this cursor to the CursorLink chain in the TXNState variable
        CursorLink *newLink = malloc(sizeof(CursorLink));
        newLink->cursor = *cursor;
        newLink->dbp = dbp;
        newLink->cursorLink = NULL;
        if (prevCursorLink == NULL) {
            (*txnState)->cursorLink = newLink;
        } else {
            prevCursorLink->cursorLink = newLink;
        }
    }
    
    //put the cursor and transaction information into the index state variable
    state->tid = (*txnState)->tid->id((*txnState)->tid);
    
    return SUCCESS;
}

    
#pragma mark get
ErrCode get(IdxState *ident, TxnState *txn, Record *record)
{
    BDBState *state = (BDBState*)ident;
    DB *dbp = state->dbp;
    int ret;
    
    //save the key into the state so that getNext will behave properly if key is not found
    memcpy(&(state->lastKey), &(record->key), sizeof(Key));
    //make it clear when get() was unable to find a valid key
    state->keyNotFound = 0;

    DBT key, data;
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    data.data = record->payload;
    data.ulen = MAX_PAYLOAD_LEN+1;
    
    data.flags = DB_DBT_MALLOC;
    
    record->key.type = state->type;
    if (p_setKeyDataFromKey(&record->key, &key) < 0) {
        dbp->errx(dbp,"bad insert key type");
        memset(record->payload, 0, MAX_PAYLOAD_LEN);
        return KEY_NOTFOUND;
    }
    
    TXNState *txnState;
    DBC *cursor = NULL;
    ret = p_prepTxnCursor(state, txn, &txnState, &cursor);
    if (ret != SUCCESS) {
        goto finish;
    }
    
#if DB_VERSION_MINOR>=7
    if ((ret = cursor->get(cursor, &key, &data, DB_SET)) != 0) {
#else
    if ((ret = cursor->c_get(cursor, &key, &data, DB_SET)) != 0) {
#endif
        memset(record->payload, 0, MAX_PAYLOAD_LEN);
        if (ret == DB_LOCK_DEADLOCK) {
            ret = DEADLOCK;
            goto finish;
        }
        state->keyNotFound = 1;
        ret = KEY_NOTFOUND;
        goto finish;
    }
        
    memcpy(record->payload, data.data, data.size < MAX_PAYLOAD_LEN ? data.size : MAX_PAYLOAD_LEN);
    ret = SUCCESS;
    
    //whether return value is success or failure, if we opened a transaction for this function call
    //we need to close it before we return
finish:
    //if we opened a transaction for this call, close it
    if (txn == NULL) {
        if (ret == SUCCESS) {
            if (txnState == NULL) {
            }
            ret = commitTransaction((TxnState*)txnState);
        } else {
            abortTransaction((TxnState*)txnState);
        }
    }
    //return the ErrCode
    return ret;
}
    
#pragma mark getNext
ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record)
{
    BDBState *state = (BDBState*)idxState;
    DB *dbp = state->dbp;
    int ret;
    
    DBT key, data;
    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));
    data.data = record->payload;
    data.ulen = MAX_PAYLOAD_LEN+1;
    
    //retrieve or create a cursor for this index/txn combination (creating a txn if necessary)
    TXNState *txnState;
    DBC *cursor = NULL;
    ret = p_prepTxnCursor(state, txn, &txnState, &cursor);
    if (ret != SUCCESS) {
        goto finish;
    }
    
    //if the last call to get() was given a key not in the DB, getNext() should find
    //the first key in the DB after that key, rather than starting at the beginning
    if (state->keyNotFound == 1) {
        state->keyNotFound = 0;
        if (p_setKeyDataFromKey(&state->lastKey, &key) < 0) {
            dbp->errx(dbp,"bad insert key type");
            memset(record->payload, 0, MAX_PAYLOAD_LEN);
            return KEY_NOTFOUND;
        }
#if DB_VERSION_MINOR>=7
        if ((ret = cursor->get(cursor, &key, &data, DB_SET_RANGE)) != 0) {
#else
        if ((ret = cursor->c_get(cursor, &key, &data, DB_SET_RANGE)) != 0) {
#endif
            dbp->err(dbp, ret, "DBcursor->get");
            memset(record->payload, 0, MAX_PAYLOAD_LEN);
            if (ret == DB_LOCK_DEADLOCK) {
                ret = DEADLOCK;
                goto finish;
            }
            ret = DB_END;
            goto finish;
        }
    } else {
#if DB_VERSION_MINOR>=7
        if ((ret = cursor->get(cursor, &key, &data, DB_NEXT)) != 0) {
#else
        if ((ret = cursor->c_get(cursor, &key, &data, DB_NEXT)) != 0) {
#endif
            dbp->err(dbp, ret, "DBcursor->get");
            memset(record->payload, 0, MAX_PAYLOAD_LEN);
            
            if (ret == DB_LOCK_DEADLOCK) {
                ret = DEADLOCK;
                goto finish;
            }
            ret = DB_END;
            goto finish;
        }
    }
    
    //insert the retrieved data into a Record and return it
        
    Key k;
    memset(&k, 0, sizeof(Key));
    if (state->type == VARCHAR) {
        k.type = VARCHAR;
        memcpy(k.keyval.charkey, key.data, key.size < MAX_VARCHAR_LEN ? key.size : MAX_VARCHAR_LEN);
    } else if (state->type == SHORT) {
        uint32_t i;
        uint8_t *data = key.data;
        
        i = ((uint32_t)data[3])
        | (((uint32_t)data[2]) << 8)
        | (((uint32_t)data[1]) << 16)
        | (((uint32_t)data[0]) << 24);
        i ^= 0x80000000;
        
        k.type = SHORT;
        k.keyval.shortkey = *((int32_t *)&i);
    } else if (state->type == INT) {
        uint64_t i;
        uint8_t *data = key.data;
        
        i = ((uint64_t)data[7])
        | (((uint64_t)data[6]) << 8)
        | (((uint64_t)data[5]) << 16)
        | (((uint64_t)data[4]) << 24)
        | (((uint64_t)data[3]) << 32)
        | (((uint64_t)data[2]) << 40)
        | (((uint64_t)data[1]) << 48)
        | (((uint64_t)data[0]) << 56);
        i ^= 0x8000000000000000LL;
        
        k.type = INT;
        k.keyval.intkey = *((int64_t *)&i);
    }
    memcpy(&record->key, &k, sizeof(Key));
    memcpy(record->payload, data.data, data.size < MAX_PAYLOAD_LEN ? data.size : MAX_PAYLOAD_LEN);
    
    ret = SUCCESS;
        
    //whether return value is success or failure, if we opened a transaction for this function call
    //we need to close it before we return
finish:
    //if we opened a transaction for this call, close it
    if (txn == NULL) {
        if (ret == SUCCESS) {
            ret = commitTransaction((TxnState*)txnState);
        } else {
            abortTransaction((TxnState*)txnState);
        }
    }
    //return the ErrCode
    return ret;
}

#pragma mark insertRecord
ErrCode insertRecord(IdxState *ident, TxnState *txn, Key *k, const char* payload)
{
    BDBState *state = (BDBState*)ident;
    DB *dbp = state->dbp;
    TXNState *txnState = (TXNState*)txn;
    int ret;
    
    //prepare the key and data for insert
    DBT key, data;
    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));
    
    data.flags = DB_DBT_MALLOC;
    
    if (p_setKeyDataFromKey(k, &key) < 0) {
        dbp->errx(dbp,"bad insert key type");
        return FAILURE;
    }
    
    //make a non-const copy of the payload to give to the BDB library
    char payload_copy[MAX_PAYLOAD_LEN + 1];
    memcpy(payload_copy, payload, strlen(payload)+1);
    data.data = payload_copy;
    data.size = strlen(payload)+1;
    
    //insert key and data
    if ((ret = dbp->put(dbp, txnState == NULL ? NULL : txnState->tid, &key, &data, 0)) != 0) {
        dbp->err(dbp, ret, "DB->put");
        if (ret == DB_KEYEXIST) {
            dbp->errx(dbp, "entry (%s, %s) exists", k->keyval.charkey, payload);
            return ENTRY_EXISTS;
        } else if (ret == DB_LOCK_DEADLOCK) {
            return DEADLOCK;
        }
        return FAILURE;
    }
    
    return SUCCESS;
}

        
#pragma mark deleteRecord
ErrCode deleteRecord(IdxState *ident, TxnState *txn, Record *theRecord)
{
    BDBState *state = (BDBState*)ident;
    DB *dbp = state->dbp;
    int ret;
    
    Key k = theRecord->key;

    DBT key, data;
    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));
    
    data.flags = DB_DBT_MALLOC;
    
    if (p_setKeyDataFromKey(&k, &key) < 0) {
        dbp->errx(dbp,"bad insert key type");
        return KEY_NOTFOUND;
    }
    
    TXNState *txnState = (TXNState*)txn;
    DBC *cursor = NULL;
    
    if (memcmp(theRecord->payload, NULL_PAYLOAD, MAX_PAYLOAD_LEN) == 0) {
        //delete all records associated with the key if no payload is specified
        if ((ret = dbp->del(dbp, txnState == NULL ? NULL : txnState->tid, &key, 0)) != 0) {
            dbp->err(dbp, ret, "dbp->del");
            if (ret == DB_NOTFOUND) {
                return KEY_NOTFOUND;
            } else if (ret == DB_LOCK_DEADLOCK) {
                return DEADLOCK;
            }
            return FAILURE;
        }
        
        return SUCCESS;
    } else {
        //otherwise delete a specific pair with a cursor
        data.data = theRecord->payload;
        data.size = strlen(theRecord->payload)+1;
        
        ret = p_prepTxnCursor(state, txn, &txnState, &cursor);
        if (ret != SUCCESS) {
            goto finish;
        }
        
        
        //point the cursor at the entry to be deleted
#if DB_VERSION_MINOR>=7
        if ((ret = cursor->get(cursor, &key, &data, DB_GET_BOTH)) != 0) {
#else
        if ((ret = cursor->c_get(cursor, &key, &data, DB_GET_BOTH)) != 0) {
#endif
            dbp->err(dbp, ret, "cursor->get in delete");
            
            if (ret == DB_LOCK_DEADLOCK) {
                ret = DEADLOCK;
                goto finish;
            }
            
            ret = ENTRY_DNE;
            goto finish;
        }
        
        //delete the entry pointed to by the cursor
#if DB_VERSION_MINOR>=7
        if ((ret = cursor->del(cursor, 0)) != 0) {
#else
        if ((ret = cursor->c_del(cursor, 0)) != 0) {
#endif
            dbp->err(dbp, ret, "cursor->del");
            
            if (ret == DB_LOCK_DEADLOCK) {
                ret = DEADLOCK;
                goto finish;
            }
            ret = FAILURE;
            goto finish;
        }
        
        ret = SUCCESS;
    }
            
    //whether return value is success or failure, if we opened a transaction for this function call
    //we need to close it before we return
finish:
    //if we opened a transaction for this call, close it
    if (txn == NULL) {
        if (ret == SUCCESS) {
            ret = commitTransaction((TxnState*)txnState);
        } else {
            abortTransaction((TxnState*)txnState);
        }
    }
    //return the ErrCode
    return ret;
}


