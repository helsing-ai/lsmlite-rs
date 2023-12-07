

#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_LSM1) 

#if !defined(NDEBUG) && !defined(SQLITE_DEBUG) 
# define NDEBUG 1
#endif
#if defined(NDEBUG) && defined(SQLITE_DEBUG)
# undef NDEBUG
#endif

#line 1 "lsm.h"
/*
** 2011-08-10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file defines the LSM API.
*/
#ifndef _LSM_H
#define _LSM_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif

/*
** Opaque handle types.
*/
typedef struct lsm_compress lsm_compress;   /* Compression library functions */
typedef struct lsm_compress_factory lsm_compress_factory;
typedef struct lsm_cursor lsm_cursor;       /* Database cursor handle */
typedef struct lsm_db lsm_db;               /* Database connection handle */
typedef struct lsm_env lsm_env;             /* Runtime environment */
typedef struct lsm_file lsm_file;           /* OS file handle */
typedef struct lsm_mutex lsm_mutex;         /* Mutex handle */

/* 64-bit integer type used for file offsets. */
typedef long long int lsm_i64;              /* 64-bit signed integer type */

/* Candidate values for the 3rd argument to lsm_env.xLock() */
#define LSM_LOCK_UNLOCK 0
#define LSM_LOCK_SHARED 1
#define LSM_LOCK_EXCL   2

/* Flags for lsm_env.xOpen() */
#define LSM_OPEN_READONLY 0x0001

/*
** CAPI: Database Runtime Environment
**
** Run-time environment used by LSM
*/
struct lsm_env {
  int nByte;                 /* Size of this structure in bytes */
  int iVersion;              /* Version number of this structure (1) */
  /****** file i/o ***********************************************/
  void *pVfsCtx;
  int (*xFullpath)(lsm_env*, const char *, char *, int *);
  int (*xOpen)(lsm_env*, const char *, int flags, lsm_file **);
  int (*xRead)(lsm_file *, lsm_i64, void *, int);
  int (*xWrite)(lsm_file *, lsm_i64, void *, int);
  int (*xTruncate)(lsm_file *, lsm_i64);
  int (*xSync)(lsm_file *);
  int (*xSectorSize)(lsm_file *);
  int (*xRemap)(lsm_file *, lsm_i64, void **, lsm_i64*);
  int (*xFileid)(lsm_file *, void *pBuf, int *pnBuf);
  int (*xClose)(lsm_file *);
  int (*xUnlink)(lsm_env*, const char *);
  int (*xLock)(lsm_file*, int, int);
  int (*xTestLock)(lsm_file*, int, int, int);
  int (*xShmMap)(lsm_file*, int, int, void **);
  void (*xShmBarrier)(void);
  int (*xShmUnmap)(lsm_file*, int);
  /****** memory allocation ****************************************/
  void *pMemCtx;
  void *(*xMalloc)(lsm_env*, size_t);            /* malloc(3) function */
  void *(*xRealloc)(lsm_env*, void *, size_t);   /* realloc(3) function */
  void (*xFree)(lsm_env*, void *);               /* free(3) function */
  size_t (*xSize)(lsm_env*, void *);             /* xSize function */
  /****** mutexes ****************************************************/
  void *pMutexCtx;
  int (*xMutexStatic)(lsm_env*,int,lsm_mutex**); /* Obtain a static mutex */
  int (*xMutexNew)(lsm_env*, lsm_mutex**);       /* Get a new dynamic mutex */
  void (*xMutexDel)(lsm_mutex *);           /* Delete an allocated mutex */
  void (*xMutexEnter)(lsm_mutex *);         /* Grab a mutex */
  int (*xMutexTry)(lsm_mutex *);            /* Attempt to obtain a mutex */
  void (*xMutexLeave)(lsm_mutex *);         /* Leave a mutex */
  int (*xMutexHeld)(lsm_mutex *);           /* Return true if mutex is held */
  int (*xMutexNotHeld)(lsm_mutex *);        /* Return true if mutex not held */
  /****** other ****************************************************/
  int (*xSleep)(lsm_env*, int microseconds);

  /* New fields may be added in future releases, in which case the
  ** iVersion value will increase. */
};

/* 
** Values that may be passed as the second argument to xMutexStatic. 
*/
#define LSM_MUTEX_GLOBAL 1
#define LSM_MUTEX_HEAP   2

/*
** CAPI: LSM Error Codes
*/
#define LSM_OK         0
#define LSM_ERROR      1
#define LSM_BUSY       5
#define LSM_NOMEM      7
#define LSM_READONLY   8
#define LSM_IOERR     10
#define LSM_CORRUPT   11
#define LSM_FULL      13
#define LSM_CANTOPEN  14
#define LSM_PROTOCOL  15
#define LSM_MISUSE    21

#define LSM_MISMATCH  50


#define LSM_IOERR_NOENT (LSM_IOERR | (1<<8))

/* 
** CAPI: Creating and Destroying Database Connection Handles
**
** Open and close a database connection handle.
*/
int lsm_new(lsm_env*, lsm_db **ppDb);
int lsm_close(lsm_db *pDb);

/* 
** CAPI: Connecting to a Database
*/
int lsm_open(lsm_db *pDb, const char *zFilename);

/*
** CAPI: Obtaining pointers to database environments
**
** Return a pointer to the environment used by the database connection 
** passed as the first argument. Assuming the argument is valid, this 
** function always returns a valid environment pointer - it cannot fail.
*/
lsm_env *lsm_get_env(lsm_db *pDb);

/*
** The lsm_default_env() function returns a pointer to the default LSM
** environment for the current platform.
*/
lsm_env *lsm_default_env(void);


/*
** CAPI: Configuring a database connection.
**
** The lsm_config() function is used to configure a database connection.
*/
int lsm_config(lsm_db *, int, ...);

/*
** The following values may be passed as the second argument to lsm_config().
**
** LSM_CONFIG_AUTOFLUSH:
**   A read/write integer parameter. 
**
**   This value determines the amount of data allowed to accumulate in a
**   live in-memory tree before it is marked as old. After committing a
**   transaction, a connection checks if the size of the live in-memory tree,
**   including data structure overhead, is greater than the value of this
**   option in KB. If it is, and there is not already an old in-memory tree,
**   the live in-memory tree is marked as old.
**
**   The maximum allowable value is 1048576 (1GB). There is no minimum 
**   value. If this parameter is set to zero, then an attempt is made to
**   mark the live in-memory tree as old after each transaction is committed.
**
**   The default value is 1024 (1MB).
**
** LSM_CONFIG_PAGE_SIZE:
**   A read/write integer parameter. This parameter may only be set before
**   lsm_open() has been called.
**
** LSM_CONFIG_BLOCK_SIZE:
**   A read/write integer parameter. 
**
**   This parameter may only be set before lsm_open() has been called. It
**   must be set to a power of two between 64 and 65536, inclusive (block 
**   sizes between 64KB and 64MB).
**
**   If the connection creates a new database, the block size of the new
**   database is set to the value of this option in KB. After lsm_open()
**   has been called, querying this parameter returns the actual block
**   size of the opened database.
**
**   The default value is 1024 (1MB blocks).
**
** LSM_CONFIG_SAFETY:
**   A read/write integer parameter. Valid values are 0, 1 (the default) 
**   and 2. This parameter determines how robust the database is in the
**   face of a system crash (e.g. a power failure or operating system 
**   crash). As follows:
**
**     0 (off):    No robustness. A system crash may corrupt the database.
**
**     1 (normal): Some robustness. A system crash may not corrupt the
**                 database file, but recently committed transactions may
**                 be lost following recovery.
**
**     2 (full):   Full robustness. A system crash may not corrupt the
**                 database file. Following recovery the database file
**                 contains all successfully committed transactions.
**
** LSM_CONFIG_AUTOWORK:
**   A read/write integer parameter.
**
** LSM_CONFIG_AUTOCHECKPOINT:
**   A read/write integer parameter.
**
**   If this option is set to non-zero value N, then a checkpoint is
**   automatically attempted after each N KB of data have been written to 
**   the database file.
**
**   The amount of uncheckpointed data already written to the database file
**   is a global parameter. After performing database work (writing to the
**   database file), the process checks if the total amount of uncheckpointed 
**   data exceeds the value of this paramter. If so, a checkpoint is performed.
**   This means that this option may cause the connection to perform a 
**   checkpoint even if the current connection has itself written very little
**   data into the database file.
**
**   The default value is 2048 (checkpoint every 2MB).
**
** LSM_CONFIG_MMAP:
**   A read/write integer parameter. If this value is set to 0, then the 
**   database file is accessed using ordinary read/write IO functions. Or,
**   if it is set to 1, then the database file is memory mapped and accessed
**   that way. If this parameter is set to any value N greater than 1, then
**   up to the first N KB of the file are memory mapped, and any remainder
**   accessed using read/write IO.
**
**   The default value is 1 on 64-bit platforms and 32768 on 32-bit platforms.
**   
**
** LSM_CONFIG_USE_LOG:
**   A read/write boolean parameter. True (the default) to use the log
**   file normally. False otherwise.
**
** LSM_CONFIG_AUTOMERGE:
**   A read/write integer parameter. The minimum number of segments to
**   merge together at a time. Default value 4.
**
** LSM_CONFIG_MAX_FREELIST:
**   A read/write integer parameter. The maximum number of free-list 
**   entries that are stored in a database checkpoint (the others are
**   stored elsewhere in the database).
**
**   There is no reason for an application to configure or query this
**   parameter. It is only present because configuring a small value
**   makes certain parts of the lsm code easier to test.
**
** LSM_CONFIG_MULTIPLE_PROCESSES:
**   A read/write boolean parameter. This parameter may only be set before
**   lsm_open() has been called. If true, the library uses shared-memory
**   and posix advisory locks to co-ordinate access by clients from within
**   multiple processes. Otherwise, if false, all database clients must be 
**   located in the same process. The default value is true.
**
** LSM_CONFIG_SET_COMPRESSION:
**   Set the compression methods used to compress and decompress database
**   content. The argument to this option should be a pointer to a structure
**   of type lsm_compress. The lsm_config() method takes a copy of the 
**   structures contents.
**
**   This option may only be used before lsm_open() is called. Invoking it
**   after lsm_open() has been called results in an LSM_MISUSE error.
**
** LSM_CONFIG_GET_COMPRESSION:
**   Query the compression methods used to compress and decompress database
**   content.
**
** LSM_CONFIG_SET_COMPRESSION_FACTORY:
**   Configure a factory method to be invoked in case of an LSM_MISMATCH
**   error.
**
** LSM_CONFIG_READONLY:
**   A read/write boolean parameter. This parameter may only be set before
**   lsm_open() is called.
*/
#define LSM_CONFIG_AUTOFLUSH                1
#define LSM_CONFIG_PAGE_SIZE                2
#define LSM_CONFIG_SAFETY                   3
#define LSM_CONFIG_BLOCK_SIZE               4
#define LSM_CONFIG_AUTOWORK                 5
#define LSM_CONFIG_MMAP                     7
#define LSM_CONFIG_USE_LOG                  8
#define LSM_CONFIG_AUTOMERGE                9
#define LSM_CONFIG_MAX_FREELIST            10
#define LSM_CONFIG_MULTIPLE_PROCESSES      11
#define LSM_CONFIG_AUTOCHECKPOINT          12
#define LSM_CONFIG_SET_COMPRESSION         13
#define LSM_CONFIG_GET_COMPRESSION         14
#define LSM_CONFIG_SET_COMPRESSION_FACTORY 15
#define LSM_CONFIG_READONLY                16

#define LSM_SAFETY_OFF    0
#define LSM_SAFETY_NORMAL 1
#define LSM_SAFETY_FULL   2

/*
** CAPI: Compression and/or Encryption Hooks
*/
struct lsm_compress {
  void *pCtx;
  unsigned int iId;
  int (*xBound)(void *, int nSrc);
  int (*xCompress)(void *, char *, int *, const char *, int);
  int (*xUncompress)(void *, char *, int *, const char *, int);
  void (*xFree)(void *pCtx);
};

struct lsm_compress_factory {
  void *pCtx;
  int (*xFactory)(void *, lsm_db *, unsigned int);
  void (*xFree)(void *pCtx);
};

#define LSM_COMPRESSION_EMPTY 0
#define LSM_COMPRESSION_NONE  1

/*
** CAPI: Allocating and Freeing Memory
**
** Invoke the memory allocation functions that belong to environment
** pEnv. Or the system defaults if no memory allocation functions have 
** been registered.
*/
void *lsm_malloc(lsm_env*, size_t);
void *lsm_realloc(lsm_env*, void *, size_t);
void lsm_free(lsm_env*, void *);

/*
** CAPI: Querying a Connection For Operational Data
**
** Query a database connection for operational statistics or data.
*/
int lsm_info(lsm_db *, int, ...);

int lsm_get_user_version(lsm_db *, unsigned int *);
int lsm_set_user_version(lsm_db *, unsigned int);

/*
** The following values may be passed as the second argument to lsm_info().
**
** LSM_INFO_NWRITE:
**   The third parameter should be of type (int *). The location pointed
**   to by the third parameter is set to the number of 4KB pages written to
**   the database file during the lifetime of this connection. 
**
** LSM_INFO_NREAD:
**   The third parameter should be of type (int *). The location pointed
**   to by the third parameter is set to the number of 4KB pages read from
**   the database file during the lifetime of this connection.
**
** LSM_INFO_DB_STRUCTURE:
**   The third argument should be of type (char **). The location pointed
**   to is populated with a pointer to a nul-terminated string containing
**   the string representation of a Tcl data-structure reflecting the 
**   current structure of the database file. Specifically, the current state
**   of the worker snapshot. The returned string should be eventually freed 
**   by the caller using lsm_free().
**
**   The returned list contains one element for each level in the database,
**   in order from most to least recent. Each element contains a 
**   single element for each segment comprising the corresponding level,
**   starting with the lhs segment, then each of the rhs segments (if any)
**   in order from most to least recent.
**
**   Each segment element is itself a list of 4 integer values, as follows:
**
**   <ol><li> First page of segment
**       <li> Last page of segment
**       <li> Root page of segment (if applicable)
**       <li> Total number of pages in segment
**   </ol>
**
** LSM_INFO_ARRAY_STRUCTURE:
**   There should be two arguments passed following this option (i.e. a 
**   total of four arguments passed to lsm_info()). The first argument 
**   should be the page number of the first page in a database array 
**   (perhaps obtained from an earlier INFO_DB_STRUCTURE call). The second 
**   trailing argument should be of type (char **). The location pointed 
**   to is populated with a pointer to a nul-terminated string that must 
**   be eventually freed using lsm_free() by the caller.
**
**   The output string contains the text representation of a Tcl list of
**   integers. Each pair of integers represent a range of pages used by
**   the identified array. For example, if the array occupies database
**   pages 993 to 1024, then pages 2048 to 2777, then the returned string
**   will be "993 1024 2048 2777".
**
**   If the specified integer argument does not correspond to the first
**   page of any database array, LSM_ERROR is returned and the output
**   pointer is set to a NULL value.
**
** LSM_INFO_LOG_STRUCTURE:
**   The third argument should be of type (char **). The location pointed
**   to is populated with a pointer to a nul-terminated string containing
**   the string representation of a Tcl data-structure. The returned 
**   string should be eventually freed by the caller using lsm_free().
**
**   The Tcl structure returned is a list of six integers that describe
**   the current structure of the log file.
**
** LSM_INFO_ARRAY_PAGES:
**
** LSM_INFO_PAGE_ASCII_DUMP:
**   As with LSM_INFO_ARRAY_STRUCTURE, there should be two arguments passed
**   with calls that specify this option - an integer page number and a
**   (char **) used to return a nul-terminated string that must be later
**   freed using lsm_free(). In this case the output string is populated
**   with a human-readable description of the page content.
**
**   If the page cannot be decoded, it is not an error. In this case the
**   human-readable output message will report the systems failure to 
**   interpret the page data.
**
** LSM_INFO_PAGE_HEX_DUMP:
**   This argument is similar to PAGE_ASCII_DUMP, except that keys and
**   values are represented using hexadecimal notation instead of ascii.
**
** LSM_INFO_FREELIST:
**   The third argument should be of type (char **). The location pointed
**   to is populated with a pointer to a nul-terminated string containing
**   the string representation of a Tcl data-structure. The returned 
**   string should be eventually freed by the caller using lsm_free().
**
**   The Tcl structure returned is a list containing one element for each
**   free block in the database. The element itself consists of two 
**   integers - the block number and the id of the snapshot that freed it.
**
** LSM_INFO_CHECKPOINT_SIZE:
**   The third argument should be of type (int *). The location pointed to
**   by this argument is populated with the number of KB written to the
**   database file since the most recent checkpoint.
**
** LSM_INFO_TREE_SIZE:
**   If this value is passed as the second argument to an lsm_info() call, it
**   should be followed by two arguments of type (int *) (for a total of four
**   arguments).
**
**   At any time, there are either one or two tree structures held in shared
**   memory that new database clients will access (there may also be additional
**   tree structures being used by older clients - this API does not provide
**   information on them). One tree structure - the current tree - is used to
**   accumulate new data written to the database. The other tree structure -
**   the old tree - is a read-only tree holding older data and may be flushed 
**   to disk at any time.
** 
**   Assuming no error occurs, the location pointed to by the first of the two
**   (int *) arguments is set to the size of the old in-memory tree in KB.
**   The second is set to the size of the current, or live in-memory tree.
**
** LSM_INFO_COMPRESSION_ID:
**   This value should be followed by a single argument of type 
**   (unsigned int *). If successful, the location pointed to is populated 
**   with the database compression id before returning.
*/
#define LSM_INFO_NWRITE           1
#define LSM_INFO_NREAD            2
#define LSM_INFO_DB_STRUCTURE     3
#define LSM_INFO_LOG_STRUCTURE    4
#define LSM_INFO_ARRAY_STRUCTURE  5
#define LSM_INFO_PAGE_ASCII_DUMP  6
#define LSM_INFO_PAGE_HEX_DUMP    7
#define LSM_INFO_FREELIST         8
#define LSM_INFO_ARRAY_PAGES      9
#define LSM_INFO_CHECKPOINT_SIZE 10
#define LSM_INFO_TREE_SIZE       11
#define LSM_INFO_FREELIST_SIZE   12
#define LSM_INFO_COMPRESSION_ID  13


/* 
** CAPI: Opening and Closing Write Transactions
**
** These functions are used to open and close transactions and nested 
** sub-transactions.
**
** The lsm_begin() function is used to open transactions and sub-transactions. 
** A successful call to lsm_begin() ensures that there are at least iLevel 
** nested transactions open. To open a top-level transaction, pass iLevel=1. 
** To open a sub-transaction within the top-level transaction, iLevel=2. 
** Passing iLevel=0 is a no-op.
**
** lsm_commit() is used to commit transactions and sub-transactions. A
** successful call to lsm_commit() ensures that there are at most iLevel 
** nested transactions open. To commit a top-level transaction, pass iLevel=0. 
** To commit all sub-transactions inside the main transaction, pass iLevel=1.
**
** Function lsm_rollback() is used to roll back transactions and
** sub-transactions. A successful call to lsm_rollback() restores the database 
** to the state it was in when the iLevel'th nested sub-transaction (if any) 
** was first opened. And then closes transactions to ensure that there are 
** at most iLevel nested transactions open. Passing iLevel=0 rolls back and 
** closes the top-level transaction. iLevel=1 also rolls back the top-level 
** transaction, but leaves it open. iLevel=2 rolls back the sub-transaction 
** nested directly inside the top-level transaction (and leaves it open).
*/
int lsm_begin(lsm_db *pDb, int iLevel);
int lsm_commit(lsm_db *pDb, int iLevel);
int lsm_rollback(lsm_db *pDb, int iLevel);

/* 
** CAPI: Writing to a Database
**
** Write a new value into the database. If a value with a duplicate key 
** already exists it is replaced.
*/
int lsm_insert(lsm_db*, const void *pKey, int nKey, const void *pVal, int nVal);

/*
** Delete a value from the database. No error is returned if the specified
** key value does not exist in the database.
*/
int lsm_delete(lsm_db *, const void *pKey, int nKey);

/*
** Delete all database entries with keys that are greater than (pKey1/nKey1) 
** and smaller than (pKey2/nKey2). Note that keys (pKey1/nKey1) and
** (pKey2/nKey2) themselves, if they exist in the database, are not deleted.
**
** Return LSM_OK if successful, or an LSM error code otherwise.
*/
int lsm_delete_range(lsm_db *, 
    const void *pKey1, int nKey1, const void *pKey2, int nKey2
);

/*
** CAPI: Explicit Database Work and Checkpointing
**
** This function is called by a thread to work on the database structure.
*/
int lsm_work(lsm_db *pDb, int nMerge, int nKB, int *pnWrite);

int lsm_flush(lsm_db *pDb);

/*
** Attempt to checkpoint the current database snapshot. Return an LSM
** error code if an error occurs or LSM_OK otherwise.
**
** If the current snapshot has already been checkpointed, calling this 
** function is a no-op. In this case if pnKB is not NULL, *pnKB is
** set to 0. Or, if the current snapshot is successfully checkpointed
** by this function and pbKB is not NULL, *pnKB is set to the number
** of bytes written to the database file since the previous checkpoint
** (the same measure as returned by the LSM_INFO_CHECKPOINT_SIZE query).
*/
int lsm_checkpoint(lsm_db *pDb, int *pnKB);

/*
** CAPI: Opening and Closing Database Cursors
**
** Open and close a database cursor.
*/
int lsm_csr_open(lsm_db *pDb, lsm_cursor **ppCsr);
int lsm_csr_close(lsm_cursor *pCsr);

/* 
** CAPI: Positioning Database Cursors
**
** If the fourth parameter is LSM_SEEK_EQ, LSM_SEEK_GE or LSM_SEEK_LE,
** this function searches the database for an entry with key (pKey/nKey). 
** If an error occurs, an LSM error code is returned. Otherwise, LSM_OK.
**
** If no error occurs and the requested key is present in the database, the
** cursor is left pointing to the entry with the specified key. Or, if the 
** specified key is not present in the database the state of the cursor 
** depends on the value passed as the final parameter, as follows:
**
** LSM_SEEK_EQ:
**   The cursor is left at EOF (invalidated). A call to lsm_csr_valid()
**   returns non-zero.
**
** LSM_SEEK_LE:
**   The cursor is left pointing to the largest key in the database that
**   is smaller than (pKey/nKey). If the database contains no keys smaller
**   than (pKey/nKey), the cursor is left at EOF.
**
** LSM_SEEK_GE:
**   The cursor is left pointing to the smallest key in the database that
**   is larger than (pKey/nKey). If the database contains no keys larger
**   than (pKey/nKey), the cursor is left at EOF.
**
** If the fourth parameter is LSM_SEEK_LEFAST, this function searches the
** database in a similar manner to LSM_SEEK_LE, with two differences:
**
** <ol><li>Even if a key can be found (the cursor is not left at EOF), the
** lsm_csr_value() function may not be used (attempts to do so return
** LSM_MISUSE).
**
** <li>The key that the cursor is left pointing to may be one that has 
** been recently deleted from the database. In this case it is
** guaranteed that the returned key is larger than any key currently 
** in the database that is less than or equal to (pKey/nKey).
** </ol>
**
** LSM_SEEK_LEFAST requests are intended to be used to allocate database
** keys.
*/
int lsm_csr_seek(lsm_cursor *pCsr, const void *pKey, int nKey, int eSeek);

int lsm_csr_first(lsm_cursor *pCsr);
int lsm_csr_last(lsm_cursor *pCsr);

/*
** Advance the specified cursor to the next or previous key in the database.
** Return LSM_OK if successful, or an LSM error code otherwise.
**
** Functions lsm_csr_seek(), lsm_csr_first() and lsm_csr_last() are "seek"
** functions. Whether or not lsm_csr_next and lsm_csr_prev may be called
** successfully also depends on the most recent seek function called on
** the cursor. Specifically:
**
** <ul>
** <li> At least one seek function must have been called on the cursor.
** <li> To call lsm_csr_next(), the most recent call to a seek function must
** have been either lsm_csr_first() or a call to lsm_csr_seek() specifying
** LSM_SEEK_GE.
** <li> To call lsm_csr_prev(), the most recent call to a seek function must
** have been either lsm_csr_last() or a call to lsm_csr_seek() specifying
** LSM_SEEK_LE.
** </ul>
**
** Otherwise, if the above conditions are not met when lsm_csr_next or 
** lsm_csr_prev is called, LSM_MISUSE is returned and the cursor position
** remains unchanged.
*/
int lsm_csr_next(lsm_cursor *pCsr);
int lsm_csr_prev(lsm_cursor *pCsr);

/*
** Values that may be passed as the fourth argument to lsm_csr_seek().
*/
#define LSM_SEEK_LEFAST   -2
#define LSM_SEEK_LE       -1
#define LSM_SEEK_EQ        0
#define LSM_SEEK_GE        1

/* 
** CAPI: Extracting Data From Database Cursors
**
** Retrieve data from a database cursor.
*/
int lsm_csr_valid(lsm_cursor *pCsr);
int lsm_csr_key(lsm_cursor *pCsr, const void **ppKey, int *pnKey);
int lsm_csr_value(lsm_cursor *pCsr, const void **ppVal, int *pnVal);

/*
** If no error occurs, this function compares the database key passed via
** the pKey/nKey arguments with the key that the cursor passed as the first
** argument currently points to. If the cursors key is less than, equal to
** or greater than pKey/nKey, *piRes is set to less than, equal to or greater
** than zero before returning. LSM_OK is returned in this case.
**
** Or, if an error occurs, an LSM error code is returned and the final 
** value of *piRes is undefined. If the cursor does not point to a valid
** key when this function is called, LSM_MISUSE is returned.
*/
int lsm_csr_cmp(lsm_cursor *pCsr, const void *pKey, int nKey, int *piRes);

/*
** CAPI: Change these!!
**
** Configure a callback to which debugging and other messages should 
** be directed. Only useful for debugging lsm.
*/
void lsm_config_log(lsm_db *, void (*)(void *, int, const char *), void *);

/*
** Configure a callback that is invoked if the database connection ever
** writes to the database file.
*/
void lsm_config_work_hook(lsm_db *, void (*)(lsm_db *, void *), void *);

/* ENDOFAPI */
#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif /* ifndef _LSM_H */

#line 1 "lsmInt.h"
/*
** 2011-08-18
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Internal structure definitions for the LSM module.
*/
#ifndef _LSM_INT_H
#define _LSM_INT_H

/* #include "lsm.h" */
#include <assert.h>
#include <string.h>

#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#ifdef _WIN32
# ifdef _MSC_VER
#  define snprintf _snprintf
# endif
#else
# include <unistd.h>
#endif

#ifdef NDEBUG
# ifdef LSM_DEBUG_EXPENSIVE
#  undef LSM_DEBUG_EXPENSIVE
# endif
# ifdef LSM_DEBUG
#  undef LSM_DEBUG
# endif
#else
# ifndef LSM_DEBUG
#  define LSM_DEBUG
# endif
#endif

/* #define LSM_DEBUG_EXPENSIVE 1 */

/*
** Default values for various data structure parameters. These may be
** overridden by calls to lsm_config().
*/
#define LSM_DFLT_PAGE_SIZE          (4 * 1024)
#define LSM_DFLT_BLOCK_SIZE         (1 * 1024 * 1024)
#define LSM_DFLT_AUTOFLUSH          (1 * 1024 * 1024)
#define LSM_DFLT_AUTOCHECKPOINT     (i64)(2 * 1024 * 1024)
#define LSM_DFLT_AUTOWORK           1
#define LSM_DFLT_LOG_SIZE           (128*1024)
#define LSM_DFLT_AUTOMERGE          4
#define LSM_DFLT_SAFETY             LSM_SAFETY_NORMAL
#define LSM_DFLT_MMAP               (LSM_IS_64_BIT ? 1 : 32768)
#define LSM_DFLT_MULTIPLE_PROCESSES 1
#define LSM_DFLT_USE_LOG            1

/* Initial values for log file checksums. These are only used if the 
** database file does not contain a valid checkpoint.  */
#define LSM_CKSUM0_INIT 42
#define LSM_CKSUM1_INIT 42

/* "mmap" mode is currently only used in environments with 64-bit address 
** spaces. The following macro is used to test for this.  */
#define LSM_IS_64_BIT (sizeof(void*)==8)

#define LSM_AUTOWORK_QUANT 32

typedef struct Database Database;
typedef struct DbLog DbLog;
typedef struct FileSystem FileSystem;
typedef struct Freelist Freelist;
typedef struct FreelistEntry FreelistEntry;
typedef struct Level Level;
typedef struct LogMark LogMark;
typedef struct LogRegion LogRegion;
typedef struct LogWriter LogWriter;
typedef struct LsmString LsmString;
typedef struct Mempool Mempool;
typedef struct Merge Merge;
typedef struct MergeInput MergeInput;
typedef struct MetaPage MetaPage;
typedef struct MultiCursor MultiCursor;
typedef struct Page Page;
typedef struct Redirect Redirect;
typedef struct Segment Segment;
typedef struct SegmentMerger SegmentMerger;
typedef struct ShmChunk ShmChunk;
typedef struct ShmHeader ShmHeader;
typedef struct ShmReader ShmReader;
typedef struct Snapshot Snapshot;
typedef struct TransMark TransMark;
typedef struct Tree Tree;
typedef struct TreeCursor TreeCursor;
typedef struct TreeHeader TreeHeader;
typedef struct TreeMark TreeMark;
typedef struct TreeRoot TreeRoot;

#ifndef _SQLITEINT_H_
typedef unsigned char u8;
typedef unsigned short int u16;
typedef unsigned int u32;
typedef lsm_i64 i64;
typedef unsigned long long int u64;
#endif

/* A page number is a 64-bit integer. */
typedef i64 LsmPgno;

#ifdef LSM_DEBUG
static int lsmErrorBkpt(int);
#else
# define lsmErrorBkpt(x) (x)
#endif

#define LSM_PROTOCOL_BKPT lsmErrorBkpt(LSM_PROTOCOL)
#define LSM_IOERR_BKPT    lsmErrorBkpt(LSM_IOERR)
#define LSM_NOMEM_BKPT    lsmErrorBkpt(LSM_NOMEM)
#define LSM_CORRUPT_BKPT  lsmErrorBkpt(LSM_CORRUPT)
#define LSM_MISUSE_BKPT   lsmErrorBkpt(LSM_MISUSE)

#define unused_parameter(x) (void)(x)
#define array_size(x) (sizeof(x)/sizeof(x[0]))


/* The size of each shared-memory chunk */
#define LSM_SHM_CHUNK_SIZE (32*1024)

/* The number of bytes reserved at the start of each shm chunk for MM. */
#define LSM_SHM_CHUNK_HDR  (sizeof(ShmChunk))

/* The number of available read locks. */
#define LSM_LOCK_NREADER   6

/* The number of available read-write client locks. */
#define LSM_LOCK_NRWCLIENT   16

/* Lock definitions. 
*/
#define LSM_LOCK_DMS1         1   /* Serialize connect/disconnect ops */
#define LSM_LOCK_DMS2         2   /* Read-write connections */
#define LSM_LOCK_DMS3         3   /* Read-only connections */
#define LSM_LOCK_WRITER       4
#define LSM_LOCK_WORKER       5
#define LSM_LOCK_CHECKPOINTER 6
#define LSM_LOCK_ROTRANS      7
#define LSM_LOCK_READER(i)    ((i) + LSM_LOCK_ROTRANS + 1)
#define LSM_LOCK_RWCLIENT(i)  ((i) + LSM_LOCK_READER(LSM_LOCK_NREADER))

#define LSM_N_LOCK LSM_LOCK_RWCLIENT(LSM_LOCK_NRWCLIENT)

/*
** Meta-page size and usable size.
*/
#define LSM_META_PAGE_SIZE 4096

#define LSM_META_RW_PAGE_SIZE (LSM_META_PAGE_SIZE - LSM_N_LOCK)

/*
** Hard limit on the number of free-list entries that may be stored in 
** a checkpoint (the remainder are stored as a system record in the LSM).
** See also LSM_CONFIG_MAX_FREELIST.
*/
#define LSM_MAX_FREELIST_ENTRIES 24

#define LSM_MAX_BLOCK_REDIRECTS 16

#define LSM_ATTEMPTS_BEFORE_PROTOCOL 10000


/*
** Each entry stored in the LSM (or in-memory tree structure) has an
** associated mask of the following flags.
*/
#define LSM_START_DELETE 0x01     /* Start of open-ended delete range */
#define LSM_END_DELETE   0x02     /* End of open-ended delete range */
#define LSM_POINT_DELETE 0x04     /* Delete this key */
#define LSM_INSERT       0x08     /* Insert this key and value */
#define LSM_SEPARATOR    0x10     /* True if entry is separator key only */
#define LSM_SYSTEMKEY    0x20     /* True if entry is a system key (FREELIST) */

#define LSM_CONTIGUOUS   0x40     /* Used in lsm_tree.c */

/*
** A string that can grow by appending.
*/
struct LsmString {
  lsm_env *pEnv;              /* Run-time environment */
  int n;                      /* Size of string.  -1 indicates error */
  int nAlloc;                 /* Space allocated for z[] */
  char *z;                    /* The string content */
};

typedef struct LsmFile LsmFile;
struct LsmFile {
  lsm_file *pFile;
  LsmFile *pNext;
};

/*
** An instance of the following type is used to store an ordered list of
** u32 values. 
**
** Note: This is a place-holder implementation. It should be replaced by
** a version that avoids making a single large allocation when the array
** contains a large number of values. For this reason, the internals of 
** this object should only manipulated by the intArrayXXX() functions in 
** lsm_tree.c.
*/
typedef struct IntArray IntArray;
struct IntArray {
  int nAlloc;
  int nArray;
  u32 *aArray;
};

struct Redirect {
  int n;                          /* Number of redirects */
  struct RedirectEntry {
    int iFrom;
    int iTo;
  } *a;
};

/*
** An instance of this structure represents a point in the history of the
** tree structure to roll back to. Refer to comments in lsm_tree.c for 
** details.
*/
struct TreeMark {
  u32 iRoot;                      /* Offset of root node in shm file */
  u32 nHeight;                    /* Current height of tree structure */
  u32 iWrite;                     /* Write offset in shm file */
  u32 nChunk;                     /* Number of chunks in shared-memory file */
  u32 iFirst;                     /* First chunk in linked list */
  u32 iNextShmid;                 /* Next id to allocate */
  int iRollback;                  /* Index in lsm->rollback to revert to */
};

/*
** An instance of this structure represents a point in the database log.
*/
struct LogMark {
  i64 iOff;                       /* Offset into log (see lsm_log.c) */
  int nBuf;                       /* Size of in-memory buffer here */
  u8 aBuf[8];                     /* Bytes of content in aBuf[] */
  u32 cksum0;                     /* Checksum 0 at offset (iOff-nBuf) */
  u32 cksum1;                     /* Checksum 1 at offset (iOff-nBuf) */
};

struct TransMark {
  TreeMark tree;
  LogMark log;
};

/*
** A structure that defines the start and end offsets of a region in the
** log file. The size of the region in bytes is (iEnd - iStart), so if
** iEnd==iStart the region is zero bytes in size.
*/
struct LogRegion {
  i64 iStart;                     /* Start of region in log file */
  i64 iEnd;                       /* End of region in log file */
};

struct DbLog {
  u32 cksum0;                     /* Checksum 0 at offset iOff */
  u32 cksum1;                     /* Checksum 1 at offset iOff */
  i64 iSnapshotId;                /* Log space has been reclaimed to this ss */
  LogRegion aRegion[3];           /* Log file regions (see docs in lsm_log.c) */
};

struct TreeRoot {
  u32 iRoot;
  u32 nHeight;
  u32 nByte;                      /* Total size of this tree in bytes */
  u32 iTransId;
};

/*
** Tree header structure. 
*/
struct TreeHeader {
  u32 iUsedShmid;                 /* Id of first shm chunk used by this tree */
  u32 iNextShmid;                 /* Shm-id of next chunk allocated */
  u32 iFirst;                     /* Chunk number of smallest shm-id */
  u32 nChunk;                     /* Number of chunks in shared-memory file */
  TreeRoot root;                  /* Root and height of current tree */
  u32 iWrite;                     /* Write offset in shm file */
  TreeRoot oldroot;               /* Root and height of the previous tree */
  u32 iOldShmid;                  /* Last shm-id used by previous tree */
  u32 iUsrVersion;                /* get/set_user_version() value */
  i64 iOldLog;                    /* Log offset associated with old tree */
  u32 oldcksum0;
  u32 oldcksum1;
  DbLog log;                      /* Current layout of log file */ 
  u32 aCksum[2];                  /* Checksums 1 and 2. */
};

/*
** Database handle structure.
**
** mLock:
**   A bitmask representing the locks currently held by the connection.
**   An LSM database supports N distinct locks, where N is some number less
**   than or equal to 32. Locks are numbered starting from 1 (see the 
**   definitions for LSM_LOCK_WRITER and co.).
**
**   The least significant 32-bits in mLock represent EXCLUSIVE locks. The
**   most significant are SHARED locks. So, if a connection holds a SHARED
**   lock on lock region iLock, then the following is true:
**
**       (mLock & ((iLock+32-1) << 1))
**
**   Or for an EXCLUSIVE lock:
**
**       (mLock & ((iLock-1) << 1))
** 
** pCsr:
**   Points to the head of a linked list that contains all currently open
**   cursors. Once this list becomes empty, the user has no outstanding
**   cursors and the database handle can be successfully closed.
**
** pCsrCache:
**   This list contains cursor objects that have been closed using
**   lsm_csr_close(). Each time a cursor is closed, it is shifted from 
**   the pCsr list to this list. When a new cursor is opened, this list
**   is inspected to see if there exists a cursor object that can be
**   reused. This is an optimization only.
*/
struct lsm_db {

  /* Database handle configuration */
  lsm_env *pEnv;                            /* runtime environment */
  int (*xCmp)(void *, int, void *, int);    /* Compare function */

  /* Values configured by calls to lsm_config */
  int eSafety;                    /* LSM_SAFETY_OFF, NORMAL or FULL */
  int bAutowork;                  /* Configured by LSM_CONFIG_AUTOWORK */
  int nTreeLimit;                 /* Configured by LSM_CONFIG_AUTOFLUSH */
  int nMerge;                     /* Configured by LSM_CONFIG_AUTOMERGE */
  int bUseLog;                    /* Configured by LSM_CONFIG_USE_LOG */
  int nDfltPgsz;                  /* Configured by LSM_CONFIG_PAGE_SIZE */
  int nDfltBlksz;                 /* Configured by LSM_CONFIG_BLOCK_SIZE */
  int nMaxFreelist;               /* Configured by LSM_CONFIG_MAX_FREELIST */
  int iMmap;                      /* Configured by LSM_CONFIG_MMAP */
  i64 nAutockpt;                  /* Configured by LSM_CONFIG_AUTOCHECKPOINT */
  int bMultiProc;                 /* Configured by L_C_MULTIPLE_PROCESSES */
  int bReadonly;                  /* Configured by LSM_CONFIG_READONLY */
  lsm_compress compress;          /* Compression callbacks */
  lsm_compress_factory factory;   /* Compression callback factory */

  /* Sub-system handles */
  FileSystem *pFS;                /* On-disk portion of database */
  Database *pDatabase;            /* Database shared data */

  int iRwclient;                  /* Read-write client lock held (-1 == none) */

  /* Client transaction context */
  Snapshot *pClient;              /* Client snapshot */
  int iReader;                    /* Read lock held (-1 == unlocked) */
  int bRoTrans;                   /* True if a read-only db trans is open */
  MultiCursor *pCsr;              /* List of all open cursors */
  LogWriter *pLogWriter;          /* Context for writing to the log file */
  int nTransOpen;                 /* Number of opened write transactions */
  int nTransAlloc;                /* Allocated size of aTrans[] array */
  TransMark *aTrans;              /* Array of marks for transaction rollback */
  IntArray rollback;              /* List of tree-nodes to roll back */
  int bDiscardOld;                /* True if lsmTreeDiscardOld() was called */

  MultiCursor *pCsrCache;         /* List of all closed cursors */

  /* Worker context */
  Snapshot *pWorker;              /* Worker snapshot (or NULL) */
  Freelist *pFreelist;            /* See sortedNewToplevel() */
  int bUseFreelist;               /* True to use pFreelist */
  int bIncrMerge;                 /* True if currently doing a merge */

  int bInFactory;                 /* True if within factory.xFactory() */

  /* Debugging message callback */
  void (*xLog)(void *, int, const char *);
  void *pLogCtx;

  /* Work done notification callback */
  void (*xWork)(lsm_db *, void *);
  void *pWorkCtx;

  u64 mLock;                      /* Mask of current locks. See lsmShmLock(). */
  lsm_db *pNext;                  /* Next connection to same database */

  int nShm;                       /* Size of apShm[] array */
  void **apShm;                   /* Shared memory chunks */
  ShmHeader *pShmhdr;             /* Live shared-memory header */
  TreeHeader treehdr;             /* Local copy of tree-header */
  u32 aSnapshot[LSM_META_PAGE_SIZE / sizeof(u32)];
};

struct Segment {
  LsmPgno iFirst;                  /* First page of this run */
  LsmPgno iLastPg;                 /* Last page of this run */
  LsmPgno iRoot;                   /* Root page number (if any) */
  LsmPgno nSize;                   /* Size of this run in pages */

  Redirect *pRedirect;             /* Block redirects (or NULL) */
};

/*
** iSplitTopic/pSplitKey/nSplitKey:
**   If nRight>0, this buffer contains a copy of the largest key that has
**   already been written to the left-hand-side of the level.
*/
struct Level {
  Segment lhs;                    /* Left-hand (main) segment */
  int nRight;                     /* Size of apRight[] array */
  Segment *aRhs;                  /* Old segments being merged into this */
  int iSplitTopic;                /* Split key topic (if nRight>0) */
  void *pSplitKey;                /* Pointer to split-key (if nRight>0) */
  int nSplitKey;                  /* Number of bytes in split-key */

  u16 iAge;                       /* Number of times data has been written */
  u16 flags;                      /* Mask of LEVEL_XXX bits */
  Merge *pMerge;                  /* Merge operation currently underway */
  Level *pNext;                   /* Next level in tree */
};

/*
** The Level.flags field is set to a combination of the following bits.
**
** LEVEL_FREELIST_ONLY:
**   Set if the level consists entirely of free-list entries. 
**
** LEVEL_INCOMPLETE:
**   This is set while a new toplevel level is being constructed. It is
**   never set for any level other than a new toplevel.
*/
#define LEVEL_FREELIST_ONLY      0x0001
#define LEVEL_INCOMPLETE         0x0002


/*
** A structure describing an ongoing merge. There is an instance of this
** structure for every Level currently undergoing a merge in the worker
** snapshot.
**
** It is assumed that code that uses an instance of this structure has
** access to the associated Level struct.
**
** iOutputOff:
**   The byte offset to write to next within the last page of the 
**   output segment.
*/
struct MergeInput {
  LsmPgno iPg;                    /* Page on which next input is stored */
  int iCell;                      /* Cell containing next input to merge */
};
struct Merge {
  int nInput;                     /* Number of input runs being merged */
  MergeInput *aInput;             /* Array nInput entries in size */
  MergeInput splitkey;            /* Location in file of current splitkey */
  int nSkip;                      /* Number of separators entries to skip */
  int iOutputOff;                 /* Write offset on output page */
  LsmPgno iCurrentPtr;            /* Current pointer value */
};

/* 
** The first argument to this macro is a pointer to a Segment structure.
** Returns true if the structure instance indicates that the separators
** array is valid.
*/
#define segmentHasSeparators(pSegment) ((pSegment)->sep.iFirst>0)

/*
** The values that accompany the lock held by a database reader.
*/
struct ShmReader {
  u32 iTreeId;
  i64 iLsmId;
};

/*
** An instance of this structure is stored in the first shared-memory
** page. The shared-memory header.
**
** bWriter:
**   Immediately after opening a write transaction taking the WRITER lock, 
**   each writer client sets this flag. It is cleared right before the 
**   WRITER lock is relinquished. If a subsequent writer finds that this
**   flag is already set when a write transaction is opened, this indicates
**   that a previous writer failed mid-transaction.
**
** iMetaPage:
**   If the database file does not contain a valid, synced, checkpoint, this
**   value is set to 0. Otherwise, it is set to the meta-page number that
**   contains the most recently written checkpoint (either 1 or 2).
**
** hdr1, hdr2:
**   The two copies of the in-memory tree header. Two copies are required
**   in case a writer fails while updating one of them.
*/
struct ShmHeader {
  u32 aSnap1[LSM_META_PAGE_SIZE / 4];
  u32 aSnap2[LSM_META_PAGE_SIZE / 4];
  u32 bWriter;
  u32 iMetaPage;
  TreeHeader hdr1;
  TreeHeader hdr2;
  ShmReader aReader[LSM_LOCK_NREADER];
};

/*
** An instance of this structure is stored at the start of each shared-memory
** chunk except the first (which is the header chunk - see above).
*/
struct ShmChunk {
  u32 iShmid;
  u32 iNext;
};

/*
** Maximum number of shared-memory chunks allowed in the *-shm file. Since
** each shared-memory chunk is 32KB in size, this is a theoretical limit only.
*/
#define LSM_MAX_SHMCHUNKS  (1<<30)

/* Return true if shm-sequence "a" is larger than or equal to "b" */
#define shm_sequence_ge(a, b) (((u32)a-(u32)b) < LSM_MAX_SHMCHUNKS)

#define LSM_APPLIST_SZ 4

/*
** An instance of the following structure stores the in-memory part of
** the current free block list. This structure is to the free block list
** as the in-memory tree is to the users database content. The contents 
** of the free block list is found by merging the in-memory components 
** with those stored in the LSM, just as the contents of the database is
** found by merging the in-memory tree with the user data entries in the
** LSM.
**
** Each FreelistEntry structure in the array represents either an insert
** or delete operation on the free-list. For deletes, the FreelistEntry.iId
** field is set to -1. For inserts, it is set to zero or greater. 
**
** The array of FreelistEntry structures is always sorted in order of
** block number (ascending).
**
** When the in-memory free block list is written into the LSM, each insert
** operation is written separately. The entry key is the bitwise inverse
** of the block number as a 32-bit big-endian integer. This is done so that
** the entries in the LSM are sorted in descending order of block id. 
** The associated value is the snapshot id, formated as a varint.
*/
struct Freelist {
  FreelistEntry *aEntry;          /* Free list entries */
  int nEntry;                     /* Number of valid slots in aEntry[] */
  int nAlloc;                     /* Allocated size of aEntry[] */
};
struct FreelistEntry {
  u32 iBlk;                       /* Block number */
  i64 iId;                        /* Largest snapshot id to use this block */
};

/*
** A snapshot of a database. A snapshot contains all the information required
** to read or write a database file on disk. See the description of struct
** Database below for futher details.
*/
struct Snapshot {
  Database *pDatabase;            /* Database this snapshot belongs to */
  u32 iCmpId;                     /* Id of compression scheme */
  Level *pLevel;                  /* Pointer to level 0 of snapshot (or NULL) */
  i64 iId;                        /* Snapshot id */
  i64 iLogOff;                    /* Log file offset */
  Redirect redirect;              /* Block redirection array */

  /* Used by worker snapshots only */
  int nBlock;                        /* Number of blocks in database file */
  LsmPgno aiAppend[LSM_APPLIST_SZ];  /* Append point list */
  Freelist freelist;                 /* Free block list */
  u32 nWrite;                        /* Total number of pages written to disk */
};
#define LSM_INITIAL_SNAPSHOT_ID 11

/*
** Functions from file "lsm_ckpt.c".
*/
static int lsmCheckpointWrite(lsm_db *, u32 *);
static int lsmCheckpointLevels(lsm_db *, int, void **, int *);
static int lsmCheckpointLoadLevels(lsm_db *pDb, void *pVal, int nVal);

static int lsmCheckpointRecover(lsm_db *);
static int lsmCheckpointDeserialize(lsm_db *, int, u32 *, Snapshot **);

static int lsmCheckpointLoadWorker(lsm_db *pDb);
static int lsmCheckpointStore(lsm_db *pDb, int);

static int lsmCheckpointLoad(lsm_db *pDb, int *);
static int lsmCheckpointLoadOk(lsm_db *pDb, int);
static int lsmCheckpointClientCacheOk(lsm_db *);

static u32 lsmCheckpointNBlock(u32 *);
static i64 lsmCheckpointId(u32 *, int);
static u32 lsmCheckpointNWrite(u32 *, int);
static i64 lsmCheckpointLogOffset(u32 *);
static int lsmCheckpointPgsz(u32 *);
static int lsmCheckpointBlksz(u32 *);
static void lsmCheckpointLogoffset(u32 *aCkpt, DbLog *pLog);
static void lsmCheckpointZeroLogoffset(lsm_db *);

static int lsmCheckpointSaveWorker(lsm_db *pDb, int);
static int lsmDatabaseFull(lsm_db *pDb);
static int lsmCheckpointSynced(lsm_db *pDb, i64 *piId, i64 *piLog, u32 *pnWrite);

static int lsmCheckpointSize(lsm_db *db, int *pnByte);

static int lsmInfoCompressionId(lsm_db *db, u32 *piCmpId);

/* 
** Functions from file "lsm_tree.c".
*/
static int lsmTreeNew(lsm_env *, int (*)(void *, int, void *, int), Tree **ppTree);
static void lsmTreeRelease(lsm_env *, Tree *);
static int lsmTreeInit(lsm_db *);
static int lsmTreeRepair(lsm_db *);

static void lsmTreeMakeOld(lsm_db *pDb);
static void lsmTreeDiscardOld(lsm_db *pDb);
static int lsmTreeHasOld(lsm_db *pDb);

static int lsmTreeSize(lsm_db *);
static int lsmTreeEndTransaction(lsm_db *pDb, int bCommit);
static int lsmTreeLoadHeader(lsm_db *pDb, int *);
static int lsmTreeLoadHeaderOk(lsm_db *, int);

static int lsmTreeInsert(lsm_db *pDb, void *pKey, int nKey, void *pVal, int nVal);
static int lsmTreeDelete(lsm_db *db, void *pKey1, int nKey1, void *pKey2, int nKey2);
static void lsmTreeRollback(lsm_db *pDb, TreeMark *pMark);
static void lsmTreeMark(lsm_db *pDb, TreeMark *pMark);

static int lsmTreeCursorNew(lsm_db *pDb, int, TreeCursor **);
static void lsmTreeCursorDestroy(TreeCursor *);

static int lsmTreeCursorSeek(TreeCursor *pCsr, void *pKey, int nKey, int *pRes);
static int lsmTreeCursorNext(TreeCursor *pCsr);
static int lsmTreeCursorPrev(TreeCursor *pCsr);
static int lsmTreeCursorEnd(TreeCursor *pCsr, int bLast);
static void lsmTreeCursorReset(TreeCursor *pCsr);
static int lsmTreeCursorKey(TreeCursor *pCsr, int *pFlags, void **ppKey, int *pnKey);
static int lsmTreeCursorFlags(TreeCursor *pCsr);
static int lsmTreeCursorValue(TreeCursor *pCsr, void **ppVal, int *pnVal);
static int lsmTreeCursorValid(TreeCursor *pCsr);
static int lsmTreeCursorSave(TreeCursor *pCsr);

static void lsmFlagsToString(int flags, char *zFlags);

/* 
** Functions from file "mem.c".
*/
static void *lsmMalloc(lsm_env*, size_t);
static void lsmFree(lsm_env*, void *);
static void *lsmRealloc(lsm_env*, void *, size_t);
static void *lsmReallocOrFree(lsm_env*, void *, size_t);
static void *lsmReallocOrFreeRc(lsm_env *, void *, size_t, int *);

static void *lsmMallocZeroRc(lsm_env*, size_t, int *);
static void *lsmMallocRc(lsm_env*, size_t, int *);

static void *lsmMallocZero(lsm_env *pEnv, size_t);
static char *lsmMallocStrdup(lsm_env *pEnv, const char *);

/* 
** Functions from file "lsm_mutex.c".
*/
static int lsmMutexStatic(lsm_env*, int, lsm_mutex **);
static int lsmMutexNew(lsm_env*, lsm_mutex **);
static void lsmMutexDel(lsm_env*, lsm_mutex *);
static void lsmMutexEnter(lsm_env*, lsm_mutex *);
static int lsmMutexTry(lsm_env*, lsm_mutex *);
static void lsmMutexLeave(lsm_env*, lsm_mutex *);

#ifndef NDEBUG
static int lsmMutexHeld(lsm_env *, lsm_mutex *);
static int lsmMutexNotHeld(lsm_env *, lsm_mutex *);
#endif

/**************************************************************************
** Start of functions from "lsm_file.c".
*/
static int lsmFsOpen(lsm_db *, const char *, int);
static int lsmFsOpenLog(lsm_db *, int *);
static void lsmFsCloseLog(lsm_db *);
static void lsmFsClose(FileSystem *);

static int lsmFsUnmap(FileSystem *);

static int lsmFsConfigure(lsm_db *db);

static int lsmFsBlockSize(FileSystem *);
static void lsmFsSetBlockSize(FileSystem *, int);
static int lsmFsMoveBlock(FileSystem *pFS, Segment *pSeg, int iTo, int iFrom);

static int lsmFsPageSize(FileSystem *);
static void lsmFsSetPageSize(FileSystem *, int);

static int lsmFsFileid(lsm_db *pDb, void **ppId, int *pnId);

/* Creating, populating, gobbling and deleting sorted runs. */
static void lsmFsGobble(lsm_db *, Segment *, LsmPgno *, int);
static int lsmFsSortedDelete(FileSystem *, Snapshot *, int, Segment *);
static int lsmFsSortedFinish(FileSystem *, Segment *);
static int lsmFsSortedAppend(FileSystem *, Snapshot *, Level *, int, Page **);
static int lsmFsSortedPadding(FileSystem *, Snapshot *, Segment *);

/* Functions to retrieve the lsm_env pointer from a FileSystem or Page object */
lsm_env *lsmFsEnv(FileSystem *);
lsm_env *lsmPageEnv(Page *);
static FileSystem *lsmPageFS(Page *);

static int lsmFsSectorSize(FileSystem *);

static void lsmSortedSplitkey(lsm_db *, Level *, int *);

/* Reading sorted run content. */
static int lsmFsDbPageLast(FileSystem *pFS, Segment *pSeg, Page **ppPg);
static int lsmFsDbPageGet(FileSystem *, Segment *, LsmPgno, Page **);
static int lsmFsDbPageNext(Segment *, Page *, int eDir, Page **);

static u8 *lsmFsPageData(Page *, int *);
static int lsmFsPageRelease(Page *);
static int lsmFsPagePersist(Page *);
static void lsmFsPageRef(Page *);
static LsmPgno lsmFsPageNumber(Page *);

static int lsmFsNRead(FileSystem *);
static int lsmFsNWrite(FileSystem *);

static int lsmFsMetaPageGet(FileSystem *, int, int, MetaPage **);
static int lsmFsMetaPageRelease(MetaPage *);
static u8 *lsmFsMetaPageData(MetaPage *, int *);

#ifdef LSM_DEBUG
static int lsmFsDbPageIsLast(Segment *pSeg, Page *pPg);
static int lsmFsIntegrityCheck(lsm_db *);
#endif

static LsmPgno lsmFsRedirectPage(FileSystem *, Redirect *, LsmPgno);

static int lsmFsPageWritable(Page *);

/* Functions to read, write and sync the log file. */
static int lsmFsWriteLog(FileSystem *pFS, i64 iOff, LsmString *pStr);
static int lsmFsSyncLog(FileSystem *pFS);
static int lsmFsReadLog(FileSystem *pFS, i64 iOff, int nRead, LsmString *pStr);
static int lsmFsTruncateLog(FileSystem *pFS, i64 nByte);
static int lsmFsTruncateDb(FileSystem *pFS, i64 nByte);
static int lsmFsCloseAndDeleteLog(FileSystem *pFS);

static LsmFile *lsmFsDeferClose(FileSystem *pFS);

/* And to sync the db file */
static int lsmFsSyncDb(FileSystem *, int);

static void lsmFsFlushWaiting(FileSystem *, int *);

/* Used by lsm_info(ARRAY_STRUCTURE) and lsm_config(MMAP) */
static int lsmInfoArrayStructure(lsm_db *pDb, int bBlock, LsmPgno iFirst, char **pz);
static int lsmInfoArrayPages(lsm_db *pDb, LsmPgno iFirst, char **pzOut);
static int lsmConfigMmap(lsm_db *pDb, int *piParam);

static int lsmEnvOpen(lsm_env *, const char *, int, lsm_file **);
static int lsmEnvClose(lsm_env *pEnv, lsm_file *pFile);
static int lsmEnvLock(lsm_env *pEnv, lsm_file *pFile, int iLock, int eLock);
static int lsmEnvTestLock(lsm_env *pEnv, lsm_file *pFile, int iLock, int nLock, int);

static int lsmEnvShmMap(lsm_env *, lsm_file *, int, int, void **); 
static void lsmEnvShmBarrier(lsm_env *);
static void lsmEnvShmUnmap(lsm_env *, lsm_file *, int);

static void lsmEnvSleep(lsm_env *, int);

static int lsmFsReadSyncedId(lsm_db *db, int, i64 *piVal);

static int lsmFsSegmentContainsPg(FileSystem *pFS, Segment *, LsmPgno, int *);

static void lsmFsPurgeCache(FileSystem *);

/*
** End of functions from "lsm_file.c".
**************************************************************************/

/* 
** Functions from file "lsm_sorted.c".
*/
static int lsmInfoPageDump(lsm_db *, LsmPgno, int, char **);
static void lsmSortedCleanup(lsm_db *);
static int lsmSortedAutoWork(lsm_db *, int nUnit);

static int lsmSortedWalkFreelist(lsm_db *, int, int (*)(void *, int, i64), void *);

static int lsmSaveWorker(lsm_db *, int);

static int lsmFlushTreeToDisk(lsm_db *pDb);

static void lsmSortedRemap(lsm_db *pDb);

static void lsmSortedFreeLevel(lsm_env *pEnv, Level *);

static int lsmSortedAdvanceAll(lsm_db *pDb);

static int lsmSortedLoadMerge(lsm_db *, Level *, u32 *, int *);
static int lsmSortedLoadFreelist(lsm_db *pDb, void **, int *);

static void *lsmSortedSplitKey(Level *pLevel, int *pnByte);

static void lsmSortedSaveTreeCursors(lsm_db *);

static int lsmMCursorNew(lsm_db *, MultiCursor **);
static void lsmMCursorClose(MultiCursor *, int);
static int lsmMCursorSeek(MultiCursor *, int, void *, int , int);
static int lsmMCursorFirst(MultiCursor *);
static int lsmMCursorPrev(MultiCursor *);
static int lsmMCursorLast(MultiCursor *);
static int lsmMCursorValid(MultiCursor *);
static int lsmMCursorNext(MultiCursor *);
static int lsmMCursorKey(MultiCursor *, void **, int *);
static int lsmMCursorValue(MultiCursor *, void **, int *);
static int lsmMCursorType(MultiCursor *, int *);
lsm_db *lsmMCursorDb(MultiCursor *);
static void lsmMCursorFreeCache(lsm_db *);

static int lsmSaveCursors(lsm_db *pDb);
static int lsmRestoreCursors(lsm_db *pDb);

static void lsmSortedDumpStructure(lsm_db *pDb, Snapshot *, int, int, const char *);
static void lsmFsDumpBlocklists(lsm_db *);

static void lsmSortedExpandBtreePage(Page *pPg, int nOrig);

static void lsmPutU32(u8 *, u32);
static u32 lsmGetU32(u8 *);
static u64 lsmGetU64(u8 *);

/*
** Functions from "lsm_varint.c".
*/
static int lsmVarintPut32(u8 *, int);
static int lsmVarintGet32(u8 *, int *);
static int lsmVarintPut64(u8 *aData, i64 iVal);
static int lsmVarintGet64(const u8 *aData, i64 *piVal);

static int lsmVarintLen64(i64);

static int lsmVarintLen32(int);
static int lsmVarintSize(u8 c);

/* 
** Functions from file "main.c".
*/
static void lsmLogMessage(lsm_db *, int, const char *, ...);
static int lsmInfoFreelist(lsm_db *pDb, char **pzOut);

/*
** Functions from file "lsm_log.c".
*/
static int lsmLogBegin(lsm_db *pDb);
static int lsmLogWrite(lsm_db *, int, void *, int, void *, int);
static int lsmLogCommit(lsm_db *);
static void lsmLogEnd(lsm_db *pDb, int bCommit);
static void lsmLogTell(lsm_db *, LogMark *);
static void lsmLogSeek(lsm_db *, LogMark *);
static void lsmLogClose(lsm_db *);

static int lsmLogRecover(lsm_db *);
static int lsmInfoLogStructure(lsm_db *pDb, char **pzVal);

/* Valid values for the second argument to lsmLogWrite(). */
#define LSM_WRITE        0x06
#define LSM_DELETE       0x08
#define LSM_DRANGE       0x0A

/**************************************************************************
** Functions from file "lsm_shared.c".
*/

static int lsmDbDatabaseConnect(lsm_db*, const char *);
static void lsmDbDatabaseRelease(lsm_db *);

static int lsmBeginReadTrans(lsm_db *);
static int lsmBeginWriteTrans(lsm_db *);
static int lsmBeginFlush(lsm_db *);

static int lsmDetectRoTrans(lsm_db *db, int *);
static int lsmBeginRoTrans(lsm_db *db);

static int lsmBeginWork(lsm_db *);
static void lsmFinishWork(lsm_db *, int, int *);

static int lsmFinishRecovery(lsm_db *);
static void lsmFinishReadTrans(lsm_db *);
static int lsmFinishWriteTrans(lsm_db *, int);
static int lsmFinishFlush(lsm_db *, int);

static int lsmSnapshotSetFreelist(lsm_db *, int *, int);

static Snapshot *lsmDbSnapshotClient(lsm_db *);
static Snapshot *lsmDbSnapshotWorker(lsm_db *);

static void lsmSnapshotSetCkptid(Snapshot *, i64);

static Level *lsmDbSnapshotLevel(Snapshot *);
static void lsmDbSnapshotSetLevel(Snapshot *, Level *);

static void lsmDbRecoveryComplete(lsm_db *, int);

static int lsmBlockAllocate(lsm_db *, int, int *);
static int lsmBlockFree(lsm_db *, int);
static int lsmBlockRefree(lsm_db *, int);

static void lsmFreelistDeltaBegin(lsm_db *);
static void lsmFreelistDeltaEnd(lsm_db *);
static int lsmFreelistDelta(lsm_db *pDb);

static DbLog *lsmDatabaseLog(lsm_db *pDb);

#ifdef LSM_DEBUG
  int lsmHoldingClientMutex(lsm_db *pDb);
  int lsmShmAssertLock(lsm_db *db, int iLock, int eOp);
  int lsmShmAssertWorker(lsm_db *db);
#endif

static void lsmFreeSnapshot(lsm_env *, Snapshot *);


/* Candidate values for the 3rd argument to lsmShmLock() */
#define LSM_LOCK_UNLOCK 0
#define LSM_LOCK_SHARED 1
#define LSM_LOCK_EXCL   2

static int lsmShmCacheChunks(lsm_db *db, int nChunk);
static int lsmShmLock(lsm_db *db, int iLock, int eOp, int bBlock);
static int lsmShmTestLock(lsm_db *db, int iLock, int nLock, int eOp);
static void lsmShmBarrier(lsm_db *db);

#ifdef LSM_DEBUG
static void lsmShmHasLock(lsm_db *db, int iLock, int eOp);
#else
# define lsmShmHasLock(x,y,z)
#endif

static int lsmReadlock(lsm_db *, i64 iLsm, u32 iShmMin, u32 iShmMax);

static int lsmLsmInUse(lsm_db *db, i64 iLsmId, int *pbInUse);
static int lsmTreeInUse(lsm_db *db, u32 iLsmId, int *pbInUse);
static int lsmFreelistAppend(lsm_env *pEnv, Freelist *p, int iBlk, i64 iId);

static int lsmDbMultiProc(lsm_db *);
static void lsmDbDeferredClose(lsm_db *, lsm_file *, LsmFile *);
static LsmFile *lsmDbRecycleFd(lsm_db *);

static int lsmWalkFreelist(lsm_db *, int, int (*)(void *, int, i64), void *);

static int lsmCheckCompressionId(lsm_db *, u32);


/**************************************************************************
** functions in lsm_str.c
*/
static void lsmStringInit(LsmString*, lsm_env *pEnv);
static int lsmStringExtend(LsmString*, int);
static int lsmStringAppend(LsmString*, const char *, int);
static void lsmStringVAppendf(LsmString*, const char *zFormat, va_list, va_list);
static void lsmStringAppendf(LsmString*, const char *zFormat, ...);
static void lsmStringClear(LsmString*);
static char *lsmMallocPrintf(lsm_env*, const char*, ...);
static int lsmStringBinAppend(LsmString *pStr, const u8 *a, int n);

static int lsmStrlen(const char *zName);



/* 
** Round up a number to the next larger multiple of 8.  This is used
** to force 8-byte alignment on 64-bit architectures.
*/
#define ROUND8(x)     (((x)+7)&~7)

#define LSM_MIN(x,y) ((x)>(y) ? (y) : (x))
#define LSM_MAX(x,y) ((x)>(y) ? (x) : (y))

#endif

#line 1 "lsm_ckpt.c"
/*
** 2011-09-11
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code to read and write checkpoints.
**
** A checkpoint represents the database layout at a single point in time.
** It includes a log offset. When an existing database is opened, the
** current state is determined by reading the newest checkpoint and updating
** it with all committed transactions from the log that follow the specified
** offset.
*/
/* #include "lsmInt.h" */

/*
** CHECKPOINT BLOB FORMAT:
**
** A checkpoint blob is a series of unsigned 32-bit integers stored in
** big-endian byte order. As follows:
**
**   Checkpoint header (see the CKPT_HDR_XXX #defines):
**
**     1. The checkpoint id MSW.
**     2. The checkpoint id LSW.
**     3. The number of integer values in the entire checkpoint, including 
**        the two checksum values.
**     4. The compression scheme id.
**     5. The total number of blocks in the database.
**     6. The block size.
**     7. The number of levels.
**     8. The nominal database page size.
**     9. The number of pages (in total) written to the database file.
**
**   Log pointer:
**
**     1. The log offset MSW.
**     2. The log offset LSW.
**     3. Log checksum 0.
**     4. Log checksum 1.
**
**     Note that the "log offset" is not the literal byte offset. Instead,
**     it is the byte offset multiplied by 2, with least significant bit
**     toggled each time the log pointer value is changed. This is to make
**     sure that this field changes each time the log pointer is updated,
**     even if the log file itself is disabled. See lsmTreeMakeOld().
**
**     See ckptExportLog() and ckptImportLog().
**
**   Append points:
**
**     8 integers (4 * 64-bit page numbers). See ckptExportAppendlist().
**
**   For each level in the database, a level record. Formatted as follows:
**
**     0. Age of the level (least significant 16-bits). And flags mask (most
**        significant 16-bits).
**     1. The number of right-hand segments (nRight, possibly 0),
**     2. Segment record for left-hand segment (8 integers defined below),
**     3. Segment record for each right-hand segment (8 integers defined below),
**     4. If nRight>0, The number of segments involved in the merge
**     5. if nRight>0, Current nSkip value (see Merge structure defn.),
**     6. For each segment in the merge:
**        5a. Page number of next cell to read during merge (this field
**            is 64-bits - 2 integers)
**        5b. Cell number of next cell to read during merge
**     7. Page containing current split-key (64-bits - 2 integers).
**     8. Cell within page containing current split-key.
**     9. Current pointer value (64-bits - 2 integers).
**
**   The block redirect array:
**
**     1. Number of redirections (maximum LSM_MAX_BLOCK_REDIRECTS).
**     2. For each redirection:
**        a. "from" block number
**        b. "to" block number
**
**   The in-memory freelist entries. Each entry is either an insert or a
**   delete. The in-memory freelist is to the free-block-list as the
**   in-memory tree is to the users database content.
**
**     1. Number of free-list entries stored in checkpoint header.
**     2. Number of free blocks (in total).
**     3. Total number of blocks freed during database lifetime.
**     4. For each entry:
**        2a. Block number of free block.
**        2b. A 64-bit integer (MSW followed by LSW). -1 for a delete entry,
**            or the associated checkpoint id for an insert.
**
**   The checksum:
**
**     1. Checksum value 1.
**     2. Checksum value 2.
**
** In the above, a segment record consists of the following four 64-bit 
** fields (converted to 2 * u32 by storing the MSW followed by LSW):
**
**     1. First page of array,
**     2. Last page of array,
**     3. Root page of array (or 0),
**     4. Size of array in pages.
*/

/*
** LARGE NUMBERS OF LEVEL RECORDS:
**
** A limit on the number of rhs segments that may be present in the database
** file. Defining this limit ensures that all level records fit within
** the 4096 byte limit for checkpoint blobs.
**
** The number of right-hand-side segments in a database is counted as 
** follows:
**
**   * For each level in the database not undergoing a merge, add 1.
**
**   * For each level in the database that is undergoing a merge, add 
**     the number of segments on the rhs of the level.
**
** A level record not undergoing a merge is 10 integers. A level record 
** with nRhs rhs segments and (nRhs+1) input segments (i.e. including the 
** separators from the next level) is (11*nRhs+20) integers. The maximum
** per right-hand-side level is therefore 21 integers. So the maximum
** size of all level records in a checkpoint is 21*40=820 integers.
**
** TODO: Before pointer values were changed from 32 to 64 bits, the above
** used to come to 420 bytes - leaving significant space for a free-list
** prefix. No more. To fix this, reduce the size of the level records in
** a db snapshot, and improve management of the free-list tail in 
** lsm_sorted.c. 
*/
#define LSM_MAX_RHS_SEGMENTS 40

/*
** LARGE NUMBERS OF FREELIST ENTRIES:
**
** There is also a limit (LSM_MAX_FREELIST_ENTRIES - defined in lsmInt.h)
** on the number of free-list entries stored in a checkpoint. Since each 
** free-list entry consists of 3 integers, the maximum free-list size is 
** 3*100=300 integers. Combined with the limit on rhs segments defined
** above, this ensures that a checkpoint always fits within a 4096 byte
** meta page.
**
** If the database contains more than 100 free blocks, the "overflow" flag
** in the checkpoint header is set and the remainder are stored in the
** system FREELIST entry in the LSM (along with user data). The value
** accompanying the FREELIST key in the LSM is, like a checkpoint, an array
** of 32-bit big-endian integers. As follows:
**
**     For each entry:
**       a. Block number of free block.
**       b. MSW of associated checkpoint id.
**       c. LSW of associated checkpoint id.
**
** The number of entries is not required - it is implied by the size of the
** value blob containing the integer array.
**
** Note that the limit defined by LSM_MAX_FREELIST_ENTRIES is a hard limit.
** The actual value used may be configured using LSM_CONFIG_MAX_FREELIST.
*/

/*
** The argument to this macro must be of type u32. On a little-endian
** architecture, it returns the u32 value that results from interpreting
** the 4 bytes as a big-endian value. On a big-endian architecture, it
** returns the value that would be produced by intepreting the 4 bytes
** of the input value as a little-endian integer.
*/
#define BYTESWAP32(x) ( \
   (((x)&0x000000FF)<<24) + (((x)&0x0000FF00)<<8)  \
 + (((x)&0x00FF0000)>>8)  + (((x)&0xFF000000)>>24) \
)

static const int one = 1;
#define LSM_LITTLE_ENDIAN (*(u8 *)(&one))

/* Sizes, in integers, of various parts of the checkpoint. */
#define CKPT_HDR_SIZE         9
#define CKPT_LOGPTR_SIZE      4
#define CKPT_APPENDLIST_SIZE  (LSM_APPLIST_SZ * 2)

/* A #define to describe each integer in the checkpoint header. */
#define CKPT_HDR_ID_MSW   0
#define CKPT_HDR_ID_LSW   1
#define CKPT_HDR_NCKPT    2
#define CKPT_HDR_CMPID    3
#define CKPT_HDR_NBLOCK   4
#define CKPT_HDR_BLKSZ    5
#define CKPT_HDR_NLEVEL   6
#define CKPT_HDR_PGSZ     7
#define CKPT_HDR_NWRITE   8

#define CKPT_HDR_LO_MSW     9
#define CKPT_HDR_LO_LSW    10
#define CKPT_HDR_LO_CKSUM1 11
#define CKPT_HDR_LO_CKSUM2 12

typedef struct CkptBuffer CkptBuffer;

/*
** Dynamic buffer used to accumulate data for a checkpoint.
*/
struct CkptBuffer {
  lsm_env *pEnv;
  int nAlloc;
  u32 *aCkpt;
};

/*
** Calculate the checksum of the checkpoint specified by arguments aCkpt and
** nCkpt. Store the checksum in *piCksum1 and *piCksum2 before returning.
**
** The value of the nCkpt parameter includes the two checksum values at
** the end of the checkpoint. They are not used as inputs to the checksum 
** calculation. The checksum is based on the array of (nCkpt-2) integers
** at aCkpt[].
*/
static void ckptChecksum(u32 *aCkpt, u32 nCkpt, u32 *piCksum1, u32 *piCksum2){
  u32 i;
  u32 cksum1 = 1;
  u32 cksum2 = 2;

  if( nCkpt % 2 ){
    cksum1 += aCkpt[nCkpt-3] & 0x0000FFFF;
    cksum2 += aCkpt[nCkpt-3] & 0xFFFF0000;
  }

  for(i=0; (i+3)<nCkpt; i+=2){
    cksum1 += cksum2 + aCkpt[i];
    cksum2 += cksum1 + aCkpt[i+1];
  }

  *piCksum1 = cksum1;
  *piCksum2 = cksum2;
}

/*
** Set integer iIdx of the checkpoint accumulating in buffer *p to iVal.
*/
static void ckptSetValue(CkptBuffer *p, int iIdx, u32 iVal, int *pRc){
  if( *pRc ) return;
  if( iIdx>=p->nAlloc ){
    int nNew = LSM_MAX(8, iIdx*2);
    p->aCkpt = (u32 *)lsmReallocOrFree(p->pEnv, p->aCkpt, nNew*sizeof(u32));
    if( !p->aCkpt ){
      *pRc = LSM_NOMEM_BKPT;
      return;
    }
    p->nAlloc = nNew;
  }
  p->aCkpt[iIdx] = iVal;
}

/*
** Argument aInt points to an array nInt elements in size. Switch the 
** endian-ness of each element of the array.
*/
static void ckptChangeEndianness(u32 *aInt, int nInt){
  if( LSM_LITTLE_ENDIAN ){
    int i;
    for(i=0; i<nInt; i++) aInt[i] = BYTESWAP32(aInt[i]);
  }
}

/*
** Object *p contains a checkpoint in native byte-order. The checkpoint is
** nCkpt integers in size, not including any checksum. This function sets
** the two checksum elements of the checkpoint accordingly.
*/
static void ckptAddChecksum(CkptBuffer *p, int nCkpt, int *pRc){
  if( *pRc==LSM_OK ){
    u32 aCksum[2] = {0, 0};
    ckptChecksum(p->aCkpt, nCkpt+2, &aCksum[0], &aCksum[1]);
    ckptSetValue(p, nCkpt, aCksum[0], pRc);
    ckptSetValue(p, nCkpt+1, aCksum[1], pRc);
  }
}

static void ckptAppend64(CkptBuffer *p, int *piOut, i64 iVal, int *pRc){
  int iOut = *piOut;
  ckptSetValue(p, iOut++, (iVal >> 32) & 0xFFFFFFFF, pRc);
  ckptSetValue(p, iOut++, (iVal & 0xFFFFFFFF), pRc);
  *piOut = iOut;
}

static i64 ckptRead64(u32 *a){
  return (((i64)a[0]) << 32) + (i64)a[1];
}

static i64 ckptGobble64(u32 *a, int *piIn){
  int iIn = *piIn;
  *piIn += 2;
  return ckptRead64(&a[iIn]);
}


/*
** Append a 6-value segment record corresponding to pSeg to the checkpoint 
** buffer passed as the third argument.
*/
static void ckptExportSegment(
  Segment *pSeg, 
  CkptBuffer *p, 
  int *piOut, 
  int *pRc
){
  ckptAppend64(p, piOut, pSeg->iFirst, pRc);
  ckptAppend64(p, piOut, pSeg->iLastPg, pRc);
  ckptAppend64(p, piOut, pSeg->iRoot, pRc);
  ckptAppend64(p, piOut, pSeg->nSize, pRc);
}

static void ckptExportLevel(
  Level *pLevel,                  /* Level object to serialize */
  CkptBuffer *p,                  /* Append new level record to this ckpt */
  int *piOut,                     /* IN/OUT: Size of checkpoint so far */
  int *pRc                        /* IN/OUT: Error code */
){
  int iOut = *piOut;
  Merge *pMerge;

  pMerge = pLevel->pMerge;
  ckptSetValue(p, iOut++, (u32)pLevel->iAge + (u32)(pLevel->flags<<16), pRc);
  ckptSetValue(p, iOut++, pLevel->nRight, pRc);
  ckptExportSegment(&pLevel->lhs, p, &iOut, pRc);

  assert( (pLevel->nRight>0)==(pMerge!=0) );
  if( pMerge ){
    int i;
    for(i=0; i<pLevel->nRight; i++){
      ckptExportSegment(&pLevel->aRhs[i], p, &iOut, pRc);
    }
    assert( pMerge->nInput==pLevel->nRight 
         || pMerge->nInput==pLevel->nRight+1 
    );
    ckptSetValue(p, iOut++, pMerge->nInput, pRc);
    ckptSetValue(p, iOut++, pMerge->nSkip, pRc);
    for(i=0; i<pMerge->nInput; i++){
      ckptAppend64(p, &iOut, pMerge->aInput[i].iPg, pRc);
      ckptSetValue(p, iOut++, pMerge->aInput[i].iCell, pRc);
    }
    ckptAppend64(p, &iOut, pMerge->splitkey.iPg, pRc);
    ckptSetValue(p, iOut++, pMerge->splitkey.iCell, pRc);
    ckptAppend64(p, &iOut, pMerge->iCurrentPtr, pRc);
  }

  *piOut = iOut;
}

/*
** Populate the log offset fields of the checkpoint buffer. 4 values.
*/
static void ckptExportLog(
  lsm_db *pDb, 
  int bFlush,
  CkptBuffer *p, 
  int *piOut, 
  int *pRc
){
  int iOut = *piOut;

  assert( iOut==CKPT_HDR_LO_MSW );

  if( bFlush ){
    i64 iOff = pDb->treehdr.iOldLog;
    ckptAppend64(p, &iOut, iOff, pRc);
    ckptSetValue(p, iOut++, pDb->treehdr.oldcksum0, pRc);
    ckptSetValue(p, iOut++, pDb->treehdr.oldcksum1, pRc);
  }else{
    for(; iOut<=CKPT_HDR_LO_CKSUM2; iOut++){
      ckptSetValue(p, iOut, pDb->pShmhdr->aSnap2[iOut], pRc);
    }
  }

  assert( *pRc || iOut==CKPT_HDR_LO_CKSUM2+1 );
  *piOut = iOut;
}

static void ckptExportAppendlist(
  lsm_db *db,                     /* Database connection */
  CkptBuffer *p,                  /* Checkpoint buffer to write to */
  int *piOut,                     /* IN/OUT: Offset within checkpoint buffer */
  int *pRc                        /* IN/OUT: Error code */
){
  int i;
  LsmPgno *aiAppend = db->pWorker->aiAppend;

  for(i=0; i<LSM_APPLIST_SZ; i++){
    ckptAppend64(p, piOut, aiAppend[i], pRc);
  }
};

static int ckptExportSnapshot( 
  lsm_db *pDb,                    /* Connection handle */
  int bLog,                       /* True to update log-offset fields */
  i64 iId,                        /* Checkpoint id */
  int bCksum,                     /* If true, include checksums */
  void **ppCkpt,                  /* OUT: Buffer containing checkpoint */
  int *pnCkpt                     /* OUT: Size of checkpoint in bytes */
){
  int rc = LSM_OK;                /* Return Code */
  FileSystem *pFS = pDb->pFS;     /* File system object */
  Snapshot *pSnap = pDb->pWorker; /* Worker snapshot */
  int nLevel = 0;                 /* Number of levels in checkpoint */
  int iLevel;                     /* Used to count out nLevel levels */
  int iOut = 0;                   /* Current offset in aCkpt[] */
  Level *pLevel;                  /* Level iterator */
  int i;                          /* Iterator used while serializing freelist */
  CkptBuffer ckpt;

  /* Initialize the output buffer */
  memset(&ckpt, 0, sizeof(CkptBuffer));
  ckpt.pEnv = pDb->pEnv;
  iOut = CKPT_HDR_SIZE;

  /* Write the log offset into the checkpoint. */
  ckptExportLog(pDb, bLog, &ckpt, &iOut, &rc);

  /* Write the append-point list */
  ckptExportAppendlist(pDb, &ckpt, &iOut, &rc);

  /* Figure out how many levels will be written to the checkpoint. */
  for(pLevel=lsmDbSnapshotLevel(pSnap); pLevel; pLevel=pLevel->pNext) nLevel++;

  /* Serialize nLevel levels. */
  iLevel = 0;
  for(pLevel=lsmDbSnapshotLevel(pSnap); iLevel<nLevel; pLevel=pLevel->pNext){
    ckptExportLevel(pLevel, &ckpt, &iOut, &rc);
    iLevel++;
  }

  /* Write the block-redirect list */
  ckptSetValue(&ckpt, iOut++, pSnap->redirect.n, &rc);
  for(i=0; i<pSnap->redirect.n; i++){
    ckptSetValue(&ckpt, iOut++, pSnap->redirect.a[i].iFrom, &rc);
    ckptSetValue(&ckpt, iOut++, pSnap->redirect.a[i].iTo, &rc);
  }

  /* Write the freelist */
  assert( pSnap->freelist.nEntry<=pDb->nMaxFreelist );
  if( rc==LSM_OK ){
    int nFree = pSnap->freelist.nEntry;
    ckptSetValue(&ckpt, iOut++, nFree, &rc);
    for(i=0; i<nFree; i++){
      FreelistEntry *p = &pSnap->freelist.aEntry[i];
      ckptSetValue(&ckpt, iOut++, p->iBlk, &rc);
      ckptSetValue(&ckpt, iOut++, (p->iId >> 32) & 0xFFFFFFFF, &rc);
      ckptSetValue(&ckpt, iOut++, p->iId & 0xFFFFFFFF, &rc);
    }
  }

  /* Write the checkpoint header */
  assert( iId>=0 );
  assert( pSnap->iCmpId==pDb->compress.iId
       || pSnap->iCmpId==LSM_COMPRESSION_EMPTY 
  );
  ckptSetValue(&ckpt, CKPT_HDR_ID_MSW, (u32)(iId>>32), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_ID_LSW, (u32)(iId&0xFFFFFFFF), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NCKPT, iOut+2, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_CMPID, pDb->compress.iId, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NBLOCK, pSnap->nBlock, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_BLKSZ, lsmFsBlockSize(pFS), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NLEVEL, nLevel, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_PGSZ, lsmFsPageSize(pFS), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NWRITE, pSnap->nWrite, &rc);

  if( bCksum ){
    ckptAddChecksum(&ckpt, iOut, &rc);
  }else{
    ckptSetValue(&ckpt, iOut, 0, &rc);
    ckptSetValue(&ckpt, iOut+1, 0, &rc);
  }
  iOut += 2;
  assert( iOut<=1024 );

#ifdef LSM_LOG_FREELIST
  lsmLogMessage(pDb, rc, 
      "ckptExportSnapshot(): id=%lld freelist: %d", iId, pSnap->freelist.nEntry
  );
  for(i=0; i<pSnap->freelist.nEntry; i++){
  lsmLogMessage(pDb, rc, 
      "ckptExportSnapshot(): iBlk=%d id=%lld", 
      pSnap->freelist.aEntry[i].iBlk,
      pSnap->freelist.aEntry[i].iId
  );
  }
#endif

  *ppCkpt = (void *)ckpt.aCkpt;
  if( pnCkpt ) *pnCkpt = sizeof(u32)*iOut;
  return rc;
}


/*
** Helper function for ckptImport().
*/
static void ckptNewSegment(
  u32 *aIn,
  int *piIn,
  Segment *pSegment               /* Populate this structure */
){
  assert( pSegment->iFirst==0 && pSegment->iLastPg==0 );
  assert( pSegment->nSize==0 && pSegment->iRoot==0 );
  pSegment->iFirst = ckptGobble64(aIn, piIn);
  pSegment->iLastPg = ckptGobble64(aIn, piIn);
  pSegment->iRoot = ckptGobble64(aIn, piIn);
  pSegment->nSize = ckptGobble64(aIn, piIn);
  assert( pSegment->iFirst );
}

static int ckptSetupMerge(lsm_db *pDb, u32 *aInt, int *piIn, Level *pLevel){
  Merge *pMerge;                  /* Allocated Merge object */
  int nInput;                     /* Number of input segments in merge */
  int iIn = *piIn;                /* Next value to read from aInt[] */
  int i;                          /* Iterator variable */
  int nByte;                      /* Number of bytes to allocate */

  /* Allocate the Merge object. If malloc() fails, return LSM_NOMEM. */
  nInput = (int)aInt[iIn++];
  nByte = sizeof(Merge) + sizeof(MergeInput) * nInput;
  pMerge = (Merge *)lsmMallocZero(pDb->pEnv, nByte);
  if( !pMerge ) return LSM_NOMEM_BKPT;
  pLevel->pMerge = pMerge;

  /* Populate the Merge object. */
  pMerge->aInput = (MergeInput *)&pMerge[1];
  pMerge->nInput = nInput;
  pMerge->iOutputOff = -1;
  pMerge->nSkip = (int)aInt[iIn++];
  for(i=0; i<nInput; i++){
    pMerge->aInput[i].iPg = ckptGobble64(aInt, &iIn);
    pMerge->aInput[i].iCell = (int)aInt[iIn++];
  }
  pMerge->splitkey.iPg = ckptGobble64(aInt, &iIn);
  pMerge->splitkey.iCell = (int)aInt[iIn++];
  pMerge->iCurrentPtr = ckptGobble64(aInt, &iIn);

  /* Set *piIn and return LSM_OK. */
  *piIn = iIn;
  return LSM_OK;
}


static int ckptLoadLevels(
  lsm_db *pDb,
  u32 *aIn, 
  int *piIn, 
  int nLevel,
  Level **ppLevel
){
  int i;
  int rc = LSM_OK;
  Level *pRet = 0;
  Level **ppNext;
  int iIn = *piIn;

  ppNext = &pRet;
  for(i=0; rc==LSM_OK && i<nLevel; i++){
    int iRight;
    Level *pLevel;

    /* Allocate space for the Level structure and Level.apRight[] array */
    pLevel = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);
    if( rc==LSM_OK ){
      pLevel->iAge = (u16)(aIn[iIn] & 0x0000FFFF);
      pLevel->flags = (u16)((aIn[iIn]>>16) & 0x0000FFFF);
      iIn++;
      pLevel->nRight = aIn[iIn++];
      if( pLevel->nRight ){
        int nByte = sizeof(Segment) * pLevel->nRight;
        pLevel->aRhs = (Segment *)lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
      }
      if( rc==LSM_OK ){
        *ppNext = pLevel;
        ppNext = &pLevel->pNext;

        /* Allocate the main segment */
        ckptNewSegment(aIn, &iIn, &pLevel->lhs);

        /* Allocate each of the right-hand segments, if any */
        for(iRight=0; iRight<pLevel->nRight; iRight++){
          ckptNewSegment(aIn, &iIn, &pLevel->aRhs[iRight]);
        }

        /* Set up the Merge object, if required */
        if( pLevel->nRight>0 ){
          rc = ckptSetupMerge(pDb, aIn, &iIn, pLevel);
        }
      }
    }
  }

  if( rc!=LSM_OK ){
    /* An OOM must have occurred. Free any level structures allocated and
    ** return the error to the caller. */
    lsmSortedFreeLevel(pDb->pEnv, pRet);
    pRet = 0;
  }

  *ppLevel = pRet;
  *piIn = iIn;
  return rc;
}


static int lsmCheckpointLoadLevels(lsm_db *pDb, void *pVal, int nVal){
  int rc = LSM_OK;
  if( nVal>0 ){
    u32 *aIn;

    aIn = lsmMallocRc(pDb->pEnv, nVal, &rc);
    if( aIn ){
      Level *pLevel = 0;
      Level *pParent;

      int nIn;
      int nLevel;
      int iIn = 1;
      memcpy(aIn, pVal, nVal);
      nIn = nVal / sizeof(u32);

      ckptChangeEndianness(aIn, nIn);
      nLevel = aIn[0];
      rc = ckptLoadLevels(pDb, aIn, &iIn, nLevel, &pLevel);
      lsmFree(pDb->pEnv, aIn);
      assert( rc==LSM_OK || pLevel==0 );
      if( rc==LSM_OK ){
        pParent = lsmDbSnapshotLevel(pDb->pWorker);
        assert( pParent );
        while( pParent->pNext ) pParent = pParent->pNext;
        pParent->pNext = pLevel;
      }
    }
  }

  return rc;
}

/*
** Return the data for the LEVELS record.
**
** The size of the checkpoint that can be stored in the database header
** must not exceed 1024 32-bit integers. Normally, it does not. However,
** if it does, part of the checkpoint must be stored in the LSM. This
** routine returns that part.
*/
static int lsmCheckpointLevels(
  lsm_db *pDb,                    /* Database handle */
  int nLevel,                     /* Number of levels to write to blob */
  void **paVal,                   /* OUT: Pointer to LEVELS blob */
  int *pnVal                      /* OUT: Size of LEVELS blob in bytes */
){
  Level *p;                       /* Used to iterate through levels */
  int nAll= 0;
  int rc;
  int i;
  int iOut;
  CkptBuffer ckpt;
  assert( nLevel>0 );

  for(p=lsmDbSnapshotLevel(pDb->pWorker); p; p=p->pNext) nAll++;

  assert( nAll>nLevel );
  nAll -= nLevel;
  for(p=lsmDbSnapshotLevel(pDb->pWorker); p && nAll>0; p=p->pNext) nAll--;

  memset(&ckpt, 0, sizeof(CkptBuffer));
  ckpt.pEnv = pDb->pEnv;

  ckptSetValue(&ckpt, 0, nLevel, &rc);
  iOut = 1;
  for(i=0; rc==LSM_OK && i<nLevel; i++){
    ckptExportLevel(p, &ckpt, &iOut, &rc);
    p = p->pNext;
  }
  assert( rc!=LSM_OK || p==0 );

  if( rc==LSM_OK ){
    ckptChangeEndianness(ckpt.aCkpt, iOut);
    *paVal = (void *)ckpt.aCkpt;
    *pnVal = iOut * sizeof(u32);
  }else{
    *pnVal = 0;
    *paVal = 0;
  }

  return rc;
}

/*
** Read the checkpoint id from meta-page pPg.
*/
static i64 ckptLoadId(MetaPage *pPg){
  i64 ret = 0;
  if( pPg ){
    int nData;
    u8 *aData = lsmFsMetaPageData(pPg, &nData);
    ret = (((i64)lsmGetU32(&aData[CKPT_HDR_ID_MSW*4])) << 32) + 
          ((i64)lsmGetU32(&aData[CKPT_HDR_ID_LSW*4]));
  }
  return ret;
}

/*
** Return true if the buffer passed as an argument contains a valid
** checkpoint.
*/
static int ckptChecksumOk(u32 *aCkpt){
  u32 nCkpt = aCkpt[CKPT_HDR_NCKPT];
  u32 cksum1;
  u32 cksum2;

  if( nCkpt<CKPT_HDR_NCKPT || nCkpt>(LSM_META_RW_PAGE_SIZE)/sizeof(u32) ){
    return 0;
  }
  ckptChecksum(aCkpt, nCkpt, &cksum1, &cksum2);
  return (cksum1==aCkpt[nCkpt-2] && cksum2==aCkpt[nCkpt-1]);
}

/*
** Attempt to load a checkpoint from meta page iMeta.
**
** This function is a no-op if *pRc is set to any value other than LSM_OK
** when it is called. If an error occurs, *pRc is set to an LSM error code
** before returning.
**
** If no error occurs and the checkpoint is successfully loaded, copy it to
** ShmHeader.aSnap1[] and ShmHeader.aSnap2[], and set ShmHeader.iMetaPage 
** to indicate its origin. In this case return 1. Or, if the checkpoint 
** cannot be loaded (because the checksum does not compute), return 0.
*/
static int ckptTryLoad(lsm_db *pDb, MetaPage *pPg, u32 iMeta, int *pRc){
  int bLoaded = 0;                /* Return value */
  if( *pRc==LSM_OK ){
    int rc = LSM_OK;              /* Error code */
    u32 *aCkpt = 0;               /* Pointer to buffer containing checkpoint */
    u32 nCkpt;                    /* Number of elements in aCkpt[] */
    int nData;                    /* Bytes of data in aData[] */
    u8 *aData;                    /* Meta page data */
   
    aData = lsmFsMetaPageData(pPg, &nData);
    nCkpt = (u32)lsmGetU32(&aData[CKPT_HDR_NCKPT*sizeof(u32)]);
    if( nCkpt<=nData/sizeof(u32) && nCkpt>CKPT_HDR_NCKPT ){
      aCkpt = (u32 *)lsmMallocRc(pDb->pEnv, nCkpt*sizeof(u32), &rc);
    }
    if( aCkpt ){
      memcpy(aCkpt, aData, nCkpt*sizeof(u32));
      ckptChangeEndianness(aCkpt, nCkpt);
      if( ckptChecksumOk(aCkpt) ){
        ShmHeader *pShm = pDb->pShmhdr;
        memcpy(pShm->aSnap1, aCkpt, nCkpt*sizeof(u32));
        memcpy(pShm->aSnap2, aCkpt, nCkpt*sizeof(u32));
        memcpy(pDb->aSnapshot, aCkpt, nCkpt*sizeof(u32));
        pShm->iMetaPage = iMeta;
        bLoaded = 1;
      }
    }

    lsmFree(pDb->pEnv, aCkpt);
    *pRc = rc;
  }
  return bLoaded;
}

/*
** Initialize the shared-memory header with an empty snapshot. This function
** is called when no valid snapshot can be found in the database header.
*/
static void ckptLoadEmpty(lsm_db *pDb){
  u32 aCkpt[] = {
    0,                       /* CKPT_HDR_ID_MSW */
    10,                      /* CKPT_HDR_ID_LSW */
    0,                       /* CKPT_HDR_NCKPT */
    LSM_COMPRESSION_EMPTY,   /* CKPT_HDR_CMPID */
    0,                       /* CKPT_HDR_NBLOCK */
    0,                       /* CKPT_HDR_BLKSZ */
    0,                       /* CKPT_HDR_NLEVEL */
    0,                       /* CKPT_HDR_PGSZ */
    0,                       /* CKPT_HDR_NWRITE */
    0, 0, 1234, 5678,        /* The log pointer and initial checksum */
    0,0,0,0, 0,0,0,0,        /* The append list */
    0,                       /* The redirected block list */
    0,                       /* The free block list */
    0, 0                     /* Space for checksum values */
  };
  u32 nCkpt = array_size(aCkpt);
  ShmHeader *pShm = pDb->pShmhdr;

  aCkpt[CKPT_HDR_NCKPT] = nCkpt;
  aCkpt[CKPT_HDR_BLKSZ] = pDb->nDfltBlksz;
  aCkpt[CKPT_HDR_PGSZ] = pDb->nDfltPgsz;
  ckptChecksum(aCkpt, array_size(aCkpt), &aCkpt[nCkpt-2], &aCkpt[nCkpt-1]);

  memcpy(pShm->aSnap1, aCkpt, nCkpt*sizeof(u32));
  memcpy(pShm->aSnap2, aCkpt, nCkpt*sizeof(u32));
  memcpy(pDb->aSnapshot, aCkpt, nCkpt*sizeof(u32));
}

/*
** This function is called as part of database recovery to initialize the
** ShmHeader.aSnap1[] and ShmHeader.aSnap2[] snapshots.
*/
static int lsmCheckpointRecover(lsm_db *pDb){
  int rc = LSM_OK;                /* Return Code */
  i64 iId1;                       /* Id of checkpoint on meta-page 1 */
  i64 iId2;                       /* Id of checkpoint on meta-page 2 */
  int bLoaded = 0;                /* True once checkpoint has been loaded */
  int cmp;                        /* True if (iId2>iId1) */
  MetaPage *apPg[2] = {0, 0};     /* Meta-pages 1 and 2 */

  rc = lsmFsMetaPageGet(pDb->pFS, 0, 1, &apPg[0]);
  if( rc==LSM_OK ) rc = lsmFsMetaPageGet(pDb->pFS, 0, 2, &apPg[1]);

  iId1 = ckptLoadId(apPg[0]);
  iId2 = ckptLoadId(apPg[1]);
  cmp = (iId2 > iId1);
  bLoaded = ckptTryLoad(pDb, apPg[cmp?1:0], (cmp?2:1), &rc);
  if( bLoaded==0 ){
    bLoaded = ckptTryLoad(pDb, apPg[cmp?0:1], (cmp?1:2), &rc);
  }

  /* The database does not contain a valid checkpoint. Initialize the shared
  ** memory header with an empty checkpoint.  */
  if( bLoaded==0 ){
    ckptLoadEmpty(pDb);
  }

  lsmFsMetaPageRelease(apPg[0]);
  lsmFsMetaPageRelease(apPg[1]);

  return rc;
}

/* 
** Store the snapshot in pDb->aSnapshot[] in meta-page iMeta.
*/
static int lsmCheckpointStore(lsm_db *pDb, int iMeta){
  MetaPage *pPg = 0;
  int rc;

  assert( iMeta==1 || iMeta==2 );
  rc = lsmFsMetaPageGet(pDb->pFS, 1, iMeta, &pPg);
  if( rc==LSM_OK ){
    u8 *aData;
    int nData;
    int nCkpt;

    nCkpt = (int)pDb->aSnapshot[CKPT_HDR_NCKPT];
    aData = lsmFsMetaPageData(pPg, &nData);
    memcpy(aData, pDb->aSnapshot, nCkpt*sizeof(u32));
    ckptChangeEndianness((u32 *)aData, nCkpt);
    rc = lsmFsMetaPageRelease(pPg);
  }
      
  return rc;
}

/*
** Copy the current client snapshot from shared-memory to pDb->aSnapshot[].
*/
static int lsmCheckpointLoad(lsm_db *pDb, int *piRead){
  int nRem = LSM_ATTEMPTS_BEFORE_PROTOCOL;
  ShmHeader *pShm = pDb->pShmhdr;
  while( (nRem--)>0 ){
    int nInt;

    nInt = pShm->aSnap1[CKPT_HDR_NCKPT];
    if( nInt<=(LSM_META_RW_PAGE_SIZE / sizeof(u32)) ){
      memcpy(pDb->aSnapshot, pShm->aSnap1, nInt*sizeof(u32));
      if( ckptChecksumOk(pDb->aSnapshot) ){
        if( piRead ) *piRead = 1;
        return LSM_OK;
      }
    }

    nInt = pShm->aSnap2[CKPT_HDR_NCKPT];
    if( nInt<=(LSM_META_RW_PAGE_SIZE / sizeof(u32)) ){
      memcpy(pDb->aSnapshot, pShm->aSnap2, nInt*sizeof(u32));
      if( ckptChecksumOk(pDb->aSnapshot) ){
        if( piRead ) *piRead = 2;
        return LSM_OK;
      }
    }

    lsmShmBarrier(pDb);
  }
  return LSM_PROTOCOL_BKPT;
}

static int lsmInfoCompressionId(lsm_db *db, u32 *piCmpId){
  int rc;

  assert( db->pClient==0 && db->pWorker==0 );
  rc = lsmCheckpointLoad(db, 0);
  if( rc==LSM_OK ){
    *piCmpId = db->aSnapshot[CKPT_HDR_CMPID];
  }

  return rc;
}

static int lsmCheckpointLoadOk(lsm_db *pDb, int iSnap){
  u32 *aShm;
  assert( iSnap==1 || iSnap==2 );
  aShm = (iSnap==1) ? pDb->pShmhdr->aSnap1 : pDb->pShmhdr->aSnap2;
  return (lsmCheckpointId(pDb->aSnapshot, 0)==lsmCheckpointId(aShm, 0) );
}

static int lsmCheckpointClientCacheOk(lsm_db *pDb){
  return ( pDb->pClient 
        && pDb->pClient->iId==lsmCheckpointId(pDb->aSnapshot, 0)
        && pDb->pClient->iId==lsmCheckpointId(pDb->pShmhdr->aSnap1, 0)
        && pDb->pClient->iId==lsmCheckpointId(pDb->pShmhdr->aSnap2, 0)
  );
}

static int lsmCheckpointLoadWorker(lsm_db *pDb){
  int rc;
  ShmHeader *pShm = pDb->pShmhdr;
  int nInt1;
  int nInt2;

  /* Must be holding the WORKER lock to do this. Or DMS2. */
  assert( 
      lsmShmAssertLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL) 
   || lsmShmAssertLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_EXCL) 
  );

  /* Check that the two snapshots match. If not, repair them. */
  nInt1 = pShm->aSnap1[CKPT_HDR_NCKPT];
  nInt2 = pShm->aSnap2[CKPT_HDR_NCKPT];
  if( nInt1!=nInt2 || memcmp(pShm->aSnap1, pShm->aSnap2, nInt2*sizeof(u32)) ){
    if( ckptChecksumOk(pShm->aSnap1) ){
      memcpy(pShm->aSnap2, pShm->aSnap1, sizeof(u32)*nInt1);
    }else if( ckptChecksumOk(pShm->aSnap2) ){
      memcpy(pShm->aSnap1, pShm->aSnap2, sizeof(u32)*nInt2);
    }else{
      return LSM_PROTOCOL_BKPT;
    }
  }

  rc = lsmCheckpointDeserialize(pDb, 1, pShm->aSnap1, &pDb->pWorker);
  if( pDb->pWorker ) pDb->pWorker->pDatabase = pDb->pDatabase;

  if( rc==LSM_OK ){
    rc = lsmCheckCompressionId(pDb, pDb->pWorker->iCmpId);
  }

#if 0
  assert( rc!=LSM_OK || lsmFsIntegrityCheck(pDb) );
#endif
  return rc;
}

static int lsmCheckpointDeserialize(
  lsm_db *pDb, 
  int bInclFreelist,              /* If true, deserialize free-list */
  u32 *aCkpt, 
  Snapshot **ppSnap
){
  int rc = LSM_OK;
  Snapshot *pNew;

  pNew = (Snapshot *)lsmMallocZeroRc(pDb->pEnv, sizeof(Snapshot), &rc);
  if( rc==LSM_OK ){
    Level *pLvl;
    int nFree;
    int i;
    int nLevel = (int)aCkpt[CKPT_HDR_NLEVEL];
    int iIn = CKPT_HDR_SIZE + CKPT_APPENDLIST_SIZE + CKPT_LOGPTR_SIZE;

    pNew->iId = lsmCheckpointId(aCkpt, 0);
    pNew->nBlock = aCkpt[CKPT_HDR_NBLOCK];
    pNew->nWrite = aCkpt[CKPT_HDR_NWRITE];
    rc = ckptLoadLevels(pDb, aCkpt, &iIn, nLevel, &pNew->pLevel);
    pNew->iLogOff = lsmCheckpointLogOffset(aCkpt);
    pNew->iCmpId = aCkpt[CKPT_HDR_CMPID];

    /* Make a copy of the append-list */
    for(i=0; i<LSM_APPLIST_SZ; i++){
      u32 *a = &aCkpt[CKPT_HDR_SIZE + CKPT_LOGPTR_SIZE + i*2];
      pNew->aiAppend[i] = ckptRead64(a);
    }

    /* Read the block-redirect list */
    pNew->redirect.n = aCkpt[iIn++];
    if( pNew->redirect.n ){
      pNew->redirect.a = lsmMallocZeroRc(pDb->pEnv, 
          (sizeof(struct RedirectEntry) * LSM_MAX_BLOCK_REDIRECTS), &rc
      );
      if( rc==LSM_OK ){
        for(i=0; i<pNew->redirect.n; i++){
          pNew->redirect.a[i].iFrom = aCkpt[iIn++];
          pNew->redirect.a[i].iTo = aCkpt[iIn++];
        }
      }
      for(pLvl=pNew->pLevel; pLvl->pNext; pLvl=pLvl->pNext);
      if( pLvl->nRight ){
        pLvl->aRhs[pLvl->nRight-1].pRedirect = &pNew->redirect;
      }else{
        pLvl->lhs.pRedirect = &pNew->redirect;
      }
    }

    /* Copy the free-list */
    if( rc==LSM_OK && bInclFreelist ){
      nFree = aCkpt[iIn++];
      if( nFree ){
        pNew->freelist.aEntry = (FreelistEntry *)lsmMallocZeroRc(
            pDb->pEnv, sizeof(FreelistEntry)*nFree, &rc
        );
        if( rc==LSM_OK ){
          int j;
          for(j=0; j<nFree; j++){
            FreelistEntry *p = &pNew->freelist.aEntry[j];
            p->iBlk = aCkpt[iIn++];
            p->iId = ((i64)(aCkpt[iIn])<<32) + aCkpt[iIn+1];
            iIn += 2;
          }
          pNew->freelist.nEntry = pNew->freelist.nAlloc = nFree;
        }
      }
    }
  }

  if( rc!=LSM_OK ){
    lsmFreeSnapshot(pDb->pEnv, pNew);
    pNew = 0;
  }

  *ppSnap = pNew;
  return rc;
}

/*
** Connection pDb must be the worker connection in order to call this
** function. It returns true if the database already contains the maximum
** number of levels or false otherwise.
**
** This is used when flushing the in-memory tree to disk. If the database
** is already full, then the caller should invoke lsm_work() or similar
** until it is not full before creating a new level by flushing the in-memory
** tree to disk. Limiting the number of levels in the database ensures that
** the records describing them always fit within the checkpoint blob.
*/
static int lsmDatabaseFull(lsm_db *pDb){
  Level *p;
  int nRhs = 0;

  assert( lsmShmAssertLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL) );
  assert( pDb->pWorker );

  for(p=pDb->pWorker->pLevel; p; p=p->pNext){
    nRhs += (p->nRight ? p->nRight : 1);
  }

  return (nRhs >= LSM_MAX_RHS_SEGMENTS);
}

/*
** The connection passed as the only argument is currently the worker
** connection. Some work has been performed on the database by the connection,
** but no new snapshot has been written into shared memory.
**
** This function updates the shared-memory worker and client snapshots with
** the new snapshot produced by the work performed by pDb.
**
** If successful, LSM_OK is returned. Otherwise, if an error occurs, an LSM
** error code is returned.
*/
static int lsmCheckpointSaveWorker(lsm_db *pDb, int bFlush){
  Snapshot *pSnap = pDb->pWorker;
  ShmHeader *pShm = pDb->pShmhdr;
  void *p = 0;
  int n = 0;
  int rc;

  pSnap->iId++;
  rc = ckptExportSnapshot(pDb, bFlush, pSnap->iId, 1, &p, &n);
  if( rc!=LSM_OK ) return rc;
  assert( ckptChecksumOk((u32 *)p) );

  assert( n<=LSM_META_RW_PAGE_SIZE );
  memcpy(pShm->aSnap2, p, n);
  lsmShmBarrier(pDb);
  memcpy(pShm->aSnap1, p, n);
  lsmFree(pDb->pEnv, p);

  /* assert( lsmFsIntegrityCheck(pDb) ); */
  return LSM_OK;
}

/*
** This function is used to determine the snapshot-id of the most recently
** checkpointed snapshot. Variable ShmHeader.iMetaPage indicates which of
** the two meta-pages said snapshot resides on (if any). 
**
** If successful, this function loads the snapshot from the meta-page, 
** verifies its checksum and sets *piId to the snapshot-id before returning
** LSM_OK. Or, if the checksum attempt fails, *piId is set to zero and
** LSM_OK returned. If an error occurs, an LSM error code is returned and
** the final value of *piId is undefined.
*/
static int lsmCheckpointSynced(lsm_db *pDb, i64 *piId, i64 *piLog, u32 *pnWrite){
  int rc = LSM_OK;
  MetaPage *pPg;
  u32 iMeta;

  iMeta = pDb->pShmhdr->iMetaPage;
  if( iMeta==1 || iMeta==2 ){
    rc = lsmFsMetaPageGet(pDb->pFS, 0, iMeta, &pPg);
    if( rc==LSM_OK ){
      int nCkpt;
      int nData;
      u8 *aData; 

      aData = lsmFsMetaPageData(pPg, &nData);
      assert( nData==LSM_META_RW_PAGE_SIZE );
      nCkpt = lsmGetU32(&aData[CKPT_HDR_NCKPT*sizeof(u32)]);
      if( nCkpt<(LSM_META_RW_PAGE_SIZE/sizeof(u32)) ){
        u32 *aCopy = lsmMallocRc(pDb->pEnv, sizeof(u32) * nCkpt, &rc);
        if( aCopy ){
          memcpy(aCopy, aData, nCkpt*sizeof(u32));
          ckptChangeEndianness(aCopy, nCkpt);
          if( ckptChecksumOk(aCopy) ){
            if( piId ) *piId = lsmCheckpointId(aCopy, 0);
            if( piLog ) *piLog = (lsmCheckpointLogOffset(aCopy) >> 1);
            if( pnWrite ) *pnWrite = aCopy[CKPT_HDR_NWRITE];
          }
          lsmFree(pDb->pEnv, aCopy);
        }
      }
      lsmFsMetaPageRelease(pPg);
    }
  }

  if( (iMeta!=1 && iMeta!=2) || rc!=LSM_OK || pDb->pShmhdr->iMetaPage!=iMeta ){
    if( piId ) *piId = 0;
    if( piLog ) *piLog = 0;
    if( pnWrite ) *pnWrite = 0;
  }
  return rc;
}

/*
** Return the checkpoint-id of the checkpoint array passed as the first
** argument to this function. If the second argument is true, then assume
** that the checkpoint is made up of 32-bit big-endian integers. If it
** is false, assume that the integers are in machine byte order.
*/
static i64 lsmCheckpointId(u32 *aCkpt, int bDisk){
  i64 iId;
  if( bDisk ){
    u8 *aData = (u8 *)aCkpt;
    iId = (((i64)lsmGetU32(&aData[CKPT_HDR_ID_MSW*4])) << 32);
    iId += ((i64)lsmGetU32(&aData[CKPT_HDR_ID_LSW*4]));
  }else{
    iId = ((i64)aCkpt[CKPT_HDR_ID_MSW] << 32) + (i64)aCkpt[CKPT_HDR_ID_LSW];
  }
  return iId;
}

static u32 lsmCheckpointNBlock(u32 *aCkpt){
  return aCkpt[CKPT_HDR_NBLOCK];
}

static u32 lsmCheckpointNWrite(u32 *aCkpt, int bDisk){
  if( bDisk ){
    return lsmGetU32((u8 *)&aCkpt[CKPT_HDR_NWRITE]);
  }else{
    return aCkpt[CKPT_HDR_NWRITE];
  }
}

static i64 lsmCheckpointLogOffset(u32 *aCkpt){
  return ((i64)aCkpt[CKPT_HDR_LO_MSW] << 32) + (i64)aCkpt[CKPT_HDR_LO_LSW];
}

static int lsmCheckpointPgsz(u32 *aCkpt){ return (int)aCkpt[CKPT_HDR_PGSZ]; }

static int lsmCheckpointBlksz(u32 *aCkpt){ return (int)aCkpt[CKPT_HDR_BLKSZ]; }

static void lsmCheckpointLogoffset(
  u32 *aCkpt,
  DbLog *pLog
){ 
  pLog->aRegion[2].iStart = (lsmCheckpointLogOffset(aCkpt) >> 1);

  pLog->cksum0 = aCkpt[CKPT_HDR_LO_CKSUM1];
  pLog->cksum1 = aCkpt[CKPT_HDR_LO_CKSUM2];
  pLog->iSnapshotId = lsmCheckpointId(aCkpt, 0);
}

static void lsmCheckpointZeroLogoffset(lsm_db *pDb){
  u32 nCkpt;

  nCkpt = pDb->aSnapshot[CKPT_HDR_NCKPT];
  assert( nCkpt>CKPT_HDR_NCKPT );
  assert( nCkpt==pDb->pShmhdr->aSnap1[CKPT_HDR_NCKPT] );
  assert( 0==memcmp(pDb->aSnapshot, pDb->pShmhdr->aSnap1, nCkpt*sizeof(u32)) );
  assert( 0==memcmp(pDb->aSnapshot, pDb->pShmhdr->aSnap2, nCkpt*sizeof(u32)) );

  pDb->aSnapshot[CKPT_HDR_LO_MSW] = 0;
  pDb->aSnapshot[CKPT_HDR_LO_LSW] = 0;
  ckptChecksum(pDb->aSnapshot, nCkpt, 
      &pDb->aSnapshot[nCkpt-2], &pDb->aSnapshot[nCkpt-1]
  );

  memcpy(pDb->pShmhdr->aSnap1, pDb->aSnapshot, nCkpt*sizeof(u32));
  memcpy(pDb->pShmhdr->aSnap2, pDb->aSnapshot, nCkpt*sizeof(u32));
}

/*
** Set the output variable to the number of KB of data written into the
** database file since the most recent checkpoint.
*/
static int lsmCheckpointSize(lsm_db *db, int *pnKB){
  int rc = LSM_OK;
  u32 nSynced;

  /* Set nSynced to the number of pages that had been written when the 
  ** database was last checkpointed. */
  rc = lsmCheckpointSynced(db, 0, 0, &nSynced);

  if( rc==LSM_OK ){
    u32 nPgsz = db->pShmhdr->aSnap1[CKPT_HDR_PGSZ];
    u32 nWrite = db->pShmhdr->aSnap1[CKPT_HDR_NWRITE];
    *pnKB = (int)(( ((i64)(nWrite - nSynced) * nPgsz) + 1023) / 1024);
  }

  return rc;
}

#line 1 "lsm_file.c"
/*
** 2011-08-26
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** 
** NORMAL DATABASE FILE FORMAT
**
** The following database file format concepts are used by the code in
** this file to read and write the database file.
**
** Pages:
**
**   A database file is divided into pages. The first 8KB of the file consists
**   of two 4KB meta-pages. The meta-page size is not configurable. The 
**   remainder of the file is made up of database pages. The default database
**   page size is 4KB. Database pages are aligned to page-size boundaries,
**   so if the database page size is larger than 8KB there is a gap between
**   the end of the meta pages and the start of the database pages.
**
**   Database pages are numbered based on their position in the file. Page N
**   begins at byte offset ((N-1)*pgsz). This means that page 1 does not 
**   exist - since it would always overlap with the meta pages. If the 
**   page-size is (say) 512 bytes, then the first usable page in the database
**   is page 33.
**
**   It is assumed that the first two meta pages and the data that follows
**   them are located on different disk sectors. So that if a power failure 
**   while writing to a meta page there is no risk of damage to the other
**   meta page or any other part of the database file. TODO: This may need
**   to be revisited.
**
** Blocks:
**
**   The database file is also divided into blocks. The default block size is
**   1MB. When writing to the database file, an attempt is made to write data
**   in contiguous block-sized chunks.
**
**   The first and last page on each block are special in that they are 4 
**   bytes smaller than all other pages. This is because the last four bytes 
**   of space on the first and last pages of each block are reserved for
**   pointers to other blocks (i.e. a 32-bit block number).
**
** Runs:
**
**   A run is a sequence of pages that the upper layer uses to store a 
**   sorted array of database keys (and accompanying data - values, FC 
**   pointers and so on). Given a page within a run, it is possible to
**   navigate to the next page in the run as follows:
**
**     a) if the current page is not the last in a block, the next page 
**        in the run is located immediately after the current page, OR
**
**     b) if the current page is the last page in a block, the next page 
**        in the run is the first page on the block identified by the
**        block pointer stored in the last 4 bytes of the current block.
**
**   It is possible to navigate to the previous page in a similar fashion,
**   using the block pointer embedded in the last 4 bytes of the first page
**   of each block as required.
**
**   The upper layer is responsible for identifying by page number the 
**   first and last page of any run that it needs to navigate - there are
**   no "end-of-run" markers stored or identified by this layer. This is
**   necessary as clients reading different database snapshots may access 
**   different subsets of a run.
**
** THE LOG FILE 
**
** This file opens and closes the log file. But it does not contain any
** logic related to the log file format. Instead, it exports the following
** functions that are used by the code in lsm_log.c to read and write the
** log file:
**
**     lsmFsOpenLog
**     lsmFsWriteLog
**     lsmFsSyncLog
**     lsmFsReadLog
**     lsmFsTruncateLog
**     lsmFsCloseAndDeleteLog
**
** COMPRESSED DATABASE FILE FORMAT
**
** The compressed database file format is very similar to the normal format.
** The file still begins with two 4KB meta-pages (which are never compressed).
** It is still divided into blocks.
**
** The first and last four bytes of each block are reserved for 32-bit 
** pointer values. Similar to the way four bytes are carved from the end of 
** the first and last page of each block in uncompressed databases. From
** the point of view of the upper layer, all pages are the same size - this
** is different from the uncompressed format where the first and last pages
** on each block are 4 bytes smaller than the others.
**
** Pages are stored in variable length compressed form, as follows:
**
**     * 3-byte size field containing the size of the compressed page image
**       in bytes. The most significant bit of each byte of the size field
**       is always set. The remaining 7 bits are used to store a 21-bit
**       integer value (in big-endian order - the first byte in the field
**       contains the most significant 7 bits). Since the maximum allowed 
**       size of a compressed page image is (2^17 - 1) bytes, there are
**       actually 4 unused bits in the size field.
**
**       In other words, if the size of the compressed page image is nSz,
**       the header can be serialized as follows:
**
**         u8 aHdr[3]
**         aHdr[0] = 0x80 | (u8)(nSz >> 14);
**         aHdr[1] = 0x80 | (u8)(nSz >>  7);
**         aHdr[2] = 0x80 | (u8)(nSz >>  0);
**
**     * Compressed page image.
**
**     * A second copy of the 3-byte record header.
**
** A page number is a byte offset into the database file. So the smallest
** possible page number is 8192 (immediately after the two meta-pages).
** The first and root page of a segment are identified by a page number
** corresponding to the byte offset of the first byte in the corresponding
** page record. The last page of a segment is identified by the byte offset
** of the last byte in its record.
**
** Unlike uncompressed pages, compressed page records may span blocks.
**
** Sometimes, in order to avoid touching sectors that contain synced data
** when writing, it is necessary to insert unused space between compressed
** page records. This can be done as follows:
**
**     * For less than 6 bytes of empty space, the first and last byte
**       of the free space contain the total number of free bytes. For
**       example:
**
**         Block of 4 free bytes: 0x04 0x?? 0x?? 0x04
**         Block of 2 free bytes: 0x02 0x02
**         A single free byte:    0x01
**
**     * For 6 or more bytes of empty space, a record similar to a 
**       compressed page record is added to the segment. A padding record
**       is distinguished from a compressed page record by the most 
**       significant bit of the second byte of the size field, which is
**       cleared instead of set. 
*/
/* #include "lsmInt.h" */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/*
** File-system object. Each database connection allocates a single instance
** of the following structure. It is used for all access to the database and
** log files.
**
** The database file may be accessed via two methods - using mmap() or using
** read() and write() calls. In the general case both methods are used - a
** prefix of the file is mapped into memory and the remainder accessed using
** read() and write(). This is helpful when accessing very large files (or
** files that may grow very large during the lifetime of a database
** connection) on systems with 32-bit address spaces. However, it also requires
** that this object manage two distinct types of Page objects simultaneously -
** those that carry pointers to the mapped file and those that carry arrays
** populated by read() calls.
**
** pFree:
**   The head of a singly-linked list that containing currently unused Page 
**   structures suitable for use as mmap-page handles. Connected by the
**   Page.pFreeNext pointers.
**
** pMapped:
**   The head of a singly-linked list that contains all pages that currently
**   carry pointers to the mapped region. This is used if the region is
**   every remapped - the pointers carried by existing pages can be adjusted
**   to account for the remapping. Connected by the Page.pMappedNext pointers.
**
** pWaiting:
**   When the upper layer wishes to append a new b-tree page to a segment,
**   it allocates a Page object that carries a malloc'd block of memory -
**   regardless of the mmap-related configuration. The page is not assigned
**   a page number at first. When the upper layer has finished constructing
**   the page contents, it calls lsmFsPagePersist() to assign a page number
**   to it. At this point it is likely that N pages have been written to the
**   segment, the (N+1)th page is still outstanding and the b-tree page is
**   assigned page number (N+2). To avoid writing page (N+2) before page 
**   (N+1), the recently completed b-tree page is held in the singly linked
**   list headed by pWaiting until page (N+1) has been written. 
**
**   Function lsmFsFlushWaiting() is responsible for eventually writing 
**   waiting pages to disk.
**
** apHash/nHash:
**   Hash table used to store all Page objects that carry malloc'd arrays,
**   except those b-tree pages that have not yet been assigned page numbers.
**   Once they have been assigned page numbers - they are added to this
**   hash table.
**
**   Hash table overflow chains are connected using the Page.pHashNext
**   pointers.
**
** pLruFirst, pLruLast:
**   The first and last entries in a doubly-linked list of pages. This
**   list contains all pages with malloc'd data that are present in the
**   hash table and have a ref-count of zero.
*/
struct FileSystem {
  lsm_db *pDb;                    /* Database handle that owns this object */
  lsm_env *pEnv;                  /* Environment pointer */
  char *zDb;                      /* Database file name */
  char *zLog;                     /* Database file name */
  int nMetasize;                  /* Size of meta pages in bytes */
  int nMetaRwSize;                /* Read/written size of meta pages in bytes */
  i64 nPagesize;                  /* Database page-size in bytes */
  i64 nBlocksize;                 /* Database block-size in bytes */

  /* r/w file descriptors for both files. */
  LsmFile *pLsmFile;              /* Used after lsm_close() to link into list */
  lsm_file *fdDb;                 /* Database file */
  lsm_file *fdLog;                /* Log file */
  int szSector;                   /* Database file sector size */

  /* If this is a compressed database, a pointer to the compression methods.
  ** For an uncompressed database, a NULL pointer.  */
  lsm_compress *pCompress;
  u8 *aIBuffer;                   /* Buffer to compress to */
  u8 *aOBuffer;                   /* Buffer to uncompress from */
  int nBuffer;                    /* Allocated size of above buffers in bytes */

  /* mmap() page related things */
  i64 nMapLimit;                  /* Maximum bytes of file to map */
  void *pMap;                     /* Current mapping of database file */
  i64 nMap;                       /* Bytes mapped at pMap */
  Page *pFree;                    /* Unused Page structures */
  Page *pMapped;                  /* List of Page structs that point to pMap */

  /* Page cache parameters for non-mmap() pages */
  int nCacheMax;                  /* Configured cache size (in pages) */
  int nCacheAlloc;                /* Current cache size (in pages) */
  Page *pLruFirst;                /* Head of the LRU list */
  Page *pLruLast;                 /* Tail of the LRU list */
  int nHash;                      /* Number of hash slots in hash table */
  Page **apHash;                  /* nHash Hash slots */
  Page *pWaiting;                 /* b-tree pages waiting to be written */

  /* Statistics */
  int nOut;                       /* Number of outstanding pages */
  int nWrite;                     /* Total number of pages written */
  int nRead;                      /* Total number of pages read */
};

/*
** Database page handle.
**
** pSeg:
**   When lsmFsSortedAppend() is called on a compressed database, the new
**   page is not assigned a page number or location in the database file
**   immediately. Instead, these are assigned by the lsmFsPagePersist() call
**   right before it writes the compressed page image to disk.
**
**   The lsmFsSortedAppend() function sets the pSeg pointer to point to the
**   segment that the new page will be a part of. It is unset by
**   lsmFsPagePersist() after the page is written to disk.
*/
struct Page {
  u8 *aData;                      /* Buffer containing page data */
  int nData;                      /* Bytes of usable data at aData[] */
  LsmPgno iPg;                    /* Page number */
  int nRef;                       /* Number of outstanding references */
  int flags;                      /* Combination of PAGE_XXX flags */
  Page *pHashNext;                /* Next page in hash table slot */
  Page *pLruNext;                 /* Next page in LRU list */
  Page *pLruPrev;                 /* Previous page in LRU list */
  FileSystem *pFS;                /* File system that owns this page */

  /* Only used in compressed database mode: */
  int nCompress;                  /* Compressed size (or 0 for uncomp. db) */
  int nCompressPrev;              /* Compressed size of prev page */
  Segment *pSeg;                  /* Segment this page will be written to */

  /* Pointers for singly linked lists */
  Page *pWaitingNext;             /* Next page in FileSystem.pWaiting list */
  Page *pFreeNext;                /* Next page in FileSystem.pFree list */
  Page *pMappedNext;              /* Next page in FileSystem.pMapped list */
};

/*
** Meta-data page handle. There are two meta-data pages at the start of
** the database file, each FileSystem.nMetasize bytes in size.
*/
struct MetaPage {
  int iPg;                        /* Either 1 or 2 */
  int bWrite;                     /* Write back to db file on release */
  u8 *aData;                      /* Pointer to buffer */
  FileSystem *pFS;                /* FileSystem that owns this page */
};

/* 
** Values for LsmPage.flags 
*/
#define PAGE_DIRTY   0x00000001   /* Set if page is dirty */
#define PAGE_FREE    0x00000002   /* Set if Page.aData requires lsmFree() */
#define PAGE_HASPREV 0x00000004   /* Set if page is first on uncomp. block */

/*
** Number of pgsz byte pages omitted from the start of block 1. The start
** of block 1 contains two 4096 byte meta pages (8192 bytes in total).
*/
#define BLOCK1_HDR_SIZE(pgsz)  LSM_MAX(1, 8192/(pgsz))

/*
** If NDEBUG is not defined, set a breakpoint in function lsmIoerrBkpt()
** to catch IO errors (any error returned by a VFS method). 
*/
#ifndef NDEBUG
static void lsmIoerrBkpt(void){
  static int nErr = 0;
  nErr++;
}
static int IOERR_WRAPPER(int rc){
  if( rc!=LSM_OK ) lsmIoerrBkpt();
  return rc;
}
#else
# define IOERR_WRAPPER(rc) (rc)
#endif

#ifdef NDEBUG
# define assert_lists_are_ok(x)
#else
static Page *fsPageFindInHash(FileSystem *pFS, LsmPgno iPg, int *piHash);

static void assert_lists_are_ok(FileSystem *pFS){
#if 0
  Page *p;

  assert( pFS->nMapLimit>=0 );

  /* Check that all pages in the LRU list have nRef==0, pointers to buffers
  ** in heap memory, and corresponding entries in the hash table.  */
  for(p=pFS->pLruFirst; p; p=p->pLruNext){
    assert( p==pFS->pLruFirst || p->pLruPrev!=0 );
    assert( p==pFS->pLruLast || p->pLruNext!=0 );
    assert( p->pLruPrev==0 || p->pLruPrev->pLruNext==p );
    assert( p->pLruNext==0 || p->pLruNext->pLruPrev==p );
    assert( p->nRef==0 );
    assert( p->flags & PAGE_FREE );
    assert( p==fsPageFindInHash(pFS, p->iPg, 0) );
  }
#endif
}
#endif

/*
** Wrappers around the VFS methods of the lsm_env object:
**
**     lsmEnvOpen()
**     lsmEnvRead()
**     lsmEnvWrite()
**     lsmEnvSync()
**     lsmEnvSectorSize()
**     lsmEnvClose()
**     lsmEnvTruncate()
**     lsmEnvUnlink()
**     lsmEnvRemap()
*/
static int lsmEnvOpen(lsm_env *pEnv, const char *zFile, int flags, lsm_file **ppNew){
  return pEnv->xOpen(pEnv, zFile, flags, ppNew);
}

static int lsmEnvRead(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  lsm_i64 iOff, 
  void *pRead, 
  int nRead
){
  return IOERR_WRAPPER( pEnv->xRead(pFile, iOff, pRead, nRead) );
}

static int lsmEnvWrite(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  lsm_i64 iOff, 
  const void *pWrite, 
  int nWrite
){
  return IOERR_WRAPPER( pEnv->xWrite(pFile, iOff, (void *)pWrite, nWrite) );
}

static int lsmEnvSync(lsm_env *pEnv, lsm_file *pFile){
  return IOERR_WRAPPER( pEnv->xSync(pFile) );
}

static int lsmEnvSectorSize(lsm_env *pEnv, lsm_file *pFile){
  return pEnv->xSectorSize(pFile);
}

static int lsmEnvClose(lsm_env *pEnv, lsm_file *pFile){
  return IOERR_WRAPPER( pEnv->xClose(pFile) );
}

static int lsmEnvTruncate(lsm_env *pEnv, lsm_file *pFile, lsm_i64 nByte){
  return IOERR_WRAPPER( pEnv->xTruncate(pFile, nByte) );
}

static int lsmEnvUnlink(lsm_env *pEnv, const char *zDel){
  return IOERR_WRAPPER( pEnv->xUnlink(pEnv, zDel) );
}

static int lsmEnvRemap(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  i64 szMin,
  void **ppMap,
  i64 *pszMap
){
  return pEnv->xRemap(pFile, szMin, ppMap, pszMap);
}

static int lsmEnvLock(lsm_env *pEnv, lsm_file *pFile, int iLock, int eLock){
  if( pFile==0 ) return LSM_OK;
  return pEnv->xLock(pFile, iLock, eLock);
}

static int lsmEnvTestLock(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  int iLock, 
  int nLock, 
  int eLock
){
  return pEnv->xTestLock(pFile, iLock, nLock, eLock);
}

static int lsmEnvShmMap(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  int iChunk, 
  int sz, 
  void **ppOut
){
  return pEnv->xShmMap(pFile, iChunk, sz, ppOut);
}

static void lsmEnvShmBarrier(lsm_env *pEnv){
  pEnv->xShmBarrier();
}

static void lsmEnvShmUnmap(lsm_env *pEnv, lsm_file *pFile, int bDel){
  pEnv->xShmUnmap(pFile, bDel);
}

static void lsmEnvSleep(lsm_env *pEnv, int nUs){
  pEnv->xSleep(pEnv, nUs);
}


/*
** Write the contents of string buffer pStr into the log file, starting at
** offset iOff.
*/
static int lsmFsWriteLog(FileSystem *pFS, i64 iOff, LsmString *pStr){
  assert( pFS->fdLog );
  return lsmEnvWrite(pFS->pEnv, pFS->fdLog, iOff, pStr->z, pStr->n);
}

/*
** fsync() the log file.
*/
static int lsmFsSyncLog(FileSystem *pFS){
  assert( pFS->fdLog );
  return lsmEnvSync(pFS->pEnv, pFS->fdLog);
}

/*
** Read nRead bytes of data starting at offset iOff of the log file. Append
** the results to string buffer pStr.
*/
static int lsmFsReadLog(FileSystem *pFS, i64 iOff, int nRead, LsmString *pStr){
  int rc;                         /* Return code */
  assert( pFS->fdLog );
  rc = lsmStringExtend(pStr, nRead);
  if( rc==LSM_OK ){
    rc = lsmEnvRead(pFS->pEnv, pFS->fdLog, iOff, &pStr->z[pStr->n], nRead);
    pStr->n += nRead;
  }
  return rc;
}

/*
** Truncate the log file to nByte bytes in size.
*/
static int lsmFsTruncateLog(FileSystem *pFS, i64 nByte){
  if( pFS->fdLog==0 ) return LSM_OK;
  return lsmEnvTruncate(pFS->pEnv, pFS->fdLog, nByte);
}

/*
** Truncate the db file to nByte bytes in size.
*/
static int lsmFsTruncateDb(FileSystem *pFS, i64 nByte){
  if( pFS->fdDb==0 ) return LSM_OK;
  return lsmEnvTruncate(pFS->pEnv, pFS->fdDb, nByte);
}

/*
** Close the log file. Then delete it from the file-system. This function
** is called during database shutdown only.
*/
static int lsmFsCloseAndDeleteLog(FileSystem *pFS){
  char *zDel;

  if( pFS->fdLog ){
    lsmEnvClose(pFS->pEnv, pFS->fdLog );
    pFS->fdLog = 0;
  }

  zDel = lsmMallocPrintf(pFS->pEnv, "%s-log", pFS->zDb);
  if( zDel ){
    lsmEnvUnlink(pFS->pEnv, zDel);
    lsmFree(pFS->pEnv, zDel);
  }
  return LSM_OK;
}

/*
** Return true if page iReal of the database should be accessed using mmap.
** False otherwise.
*/
static int fsMmapPage(FileSystem *pFS, LsmPgno iReal){
  return ((i64)iReal*pFS->nPagesize <= pFS->nMapLimit);
}

/*
** Given that there are currently nHash slots in the hash table, return 
** the hash key for file iFile, page iPg.
*/
static int fsHashKey(int nHash, LsmPgno iPg){
  return (iPg % nHash);
}

/*
** This is a helper function for lsmFsOpen(). It opens a single file on
** disk (either the database or log file).
*/
static lsm_file *fsOpenFile(
  FileSystem *pFS,                /* File system object */
  int bReadonly,                  /* True to open this file read-only */
  int bLog,                       /* True for log, false for db */
  int *pRc                        /* IN/OUT: Error code */
){
  lsm_file *pFile = 0;
  if( *pRc==LSM_OK ){
    int flags = (bReadonly ? LSM_OPEN_READONLY : 0);
    const char *zPath = (bLog ? pFS->zLog : pFS->zDb);

    *pRc = lsmEnvOpen(pFS->pEnv, zPath, flags, &pFile);
  }
  return pFile;
}

/*
** If it is not already open, this function opens the log file. It returns
** LSM_OK if successful (or if the log file was already open) or an LSM
** error code otherwise.
**
** The log file must be opened before any of the following may be called:
**
**     lsmFsWriteLog
**     lsmFsSyncLog
**     lsmFsReadLog
*/
static int lsmFsOpenLog(lsm_db *db, int *pbOpen){
  int rc = LSM_OK;
  FileSystem *pFS = db->pFS;

  if( 0==pFS->fdLog ){ 
    pFS->fdLog = fsOpenFile(pFS, db->bReadonly, 1, &rc); 

    if( rc==LSM_IOERR_NOENT && db->bReadonly ){
      rc = LSM_OK;
    }
  }

  if( pbOpen ) *pbOpen = (pFS->fdLog!=0);
  return rc;
}

/*
** Close the log file, if it is open.
*/
static void lsmFsCloseLog(lsm_db *db){
  FileSystem *pFS = db->pFS;
  if( pFS->fdLog ){
    lsmEnvClose(pFS->pEnv, pFS->fdLog);
    pFS->fdLog = 0;
  }
}

/*
** Open a connection to a database stored within the file-system.
**
** If parameter bReadonly is true, then open a read-only file-descriptor
** on the database file. It is possible that bReadonly will be false even
** if the user requested that pDb be opened read-only. This is because the
** file-descriptor may later on be recycled by a read-write connection.
** If the db file can be opened for read-write access, it always is. Parameter
** bReadonly is only ever true if it has already been determined that the
** db can only be opened for read-only access.
**
** Return LSM_OK if successful or an lsm error code otherwise.
*/
static int lsmFsOpen(
  lsm_db *pDb,                    /* Database connection to open fd for */
  const char *zDb,                /* Full path to database file */
  int bReadonly                   /* True to open db file read-only */
){
  FileSystem *pFS;
  int rc = LSM_OK;
  int nDb = strlen(zDb);
  int nByte;

  assert( pDb->pFS==0 );
  assert( pDb->pWorker==0 && pDb->pClient==0 );

  nByte = sizeof(FileSystem) + nDb+1 + nDb+4+1;
  pFS = (FileSystem *)lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
  if( pFS ){
    LsmFile *pLsmFile;
    pFS->zDb = (char *)&pFS[1];
    pFS->zLog = &pFS->zDb[nDb+1];
    pFS->nPagesize = LSM_DFLT_PAGE_SIZE;
    pFS->nBlocksize = LSM_DFLT_BLOCK_SIZE;
    pFS->nMetasize = LSM_META_PAGE_SIZE;
    pFS->nMetaRwSize = LSM_META_RW_PAGE_SIZE;
    pFS->pDb = pDb;
    pFS->pEnv = pDb->pEnv;

    /* Make a copy of the database and log file names. */
    memcpy(pFS->zDb, zDb, nDb+1);
    memcpy(pFS->zLog, zDb, nDb);
    memcpy(&pFS->zLog[nDb], "-log", 5);

    /* Allocate the hash-table here. At some point, it should be changed
    ** so that it can grow dynamicly. */
    pFS->nCacheMax = 2048*1024 / pFS->nPagesize;
    pFS->nHash = 4096;
    pFS->apHash = lsmMallocZeroRc(pDb->pEnv, sizeof(Page *) * pFS->nHash, &rc);

    /* Open the database file */
    pLsmFile = lsmDbRecycleFd(pDb);
    if( pLsmFile ){
      pFS->pLsmFile = pLsmFile;
      pFS->fdDb = pLsmFile->pFile;
      memset(pLsmFile, 0, sizeof(LsmFile));
    }else{
      pFS->pLsmFile = lsmMallocZeroRc(pDb->pEnv, sizeof(LsmFile), &rc);
      if( rc==LSM_OK ){
        pFS->fdDb = fsOpenFile(pFS, bReadonly, 0, &rc);
      }
    }

    if( rc!=LSM_OK ){
      lsmFsClose(pFS);
      pFS = 0;
    }else{
      pFS->szSector = lsmEnvSectorSize(pFS->pEnv, pFS->fdDb);
    }
  }

  pDb->pFS = pFS;
  return rc;
}

/*
** Configure the file-system object according to the current values of
** the LSM_CONFIG_MMAP and LSM_CONFIG_SET_COMPRESSION options.
*/
static int lsmFsConfigure(lsm_db *db){
  FileSystem *pFS = db->pFS;
  if( pFS ){
    lsm_env *pEnv = pFS->pEnv;
    Page *pPg;

    assert( pFS->nOut==0 );
    assert( pFS->pWaiting==0 );
    assert( pFS->pMapped==0 );

    /* Reset any compression/decompression buffers already allocated */
    lsmFree(pEnv, pFS->aIBuffer);
    lsmFree(pEnv, pFS->aOBuffer);
    pFS->nBuffer = 0;

    /* Unmap the file, if it is currently mapped */
    if( pFS->pMap ){
      lsmEnvRemap(pEnv, pFS->fdDb, -1, &pFS->pMap, &pFS->nMap);
      pFS->nMapLimit = 0;
    }

    /* Free all allocated page structures */
    pPg = pFS->pLruFirst;
    while( pPg ){
      Page *pNext = pPg->pLruNext;
      assert( pPg->flags & PAGE_FREE );
      lsmFree(pEnv, pPg->aData);
      lsmFree(pEnv, pPg);
      pPg = pNext;
    }

    pPg = pFS->pFree;
    while( pPg ){
      Page *pNext = pPg->pFreeNext;
      lsmFree(pEnv, pPg);
      pPg = pNext;
    }

    /* Zero pointers that point to deleted page objects */
    pFS->nCacheAlloc = 0;
    pFS->pLruFirst = 0;
    pFS->pLruLast = 0;
    pFS->pFree = 0;
    if( pFS->apHash ){
      memset(pFS->apHash, 0, pFS->nHash*sizeof(pFS->apHash[0]));
    }

    /* Configure the FileSystem object */
    if( db->compress.xCompress ){
      pFS->pCompress = &db->compress;
      pFS->nMapLimit = 0;
    }else{
      pFS->pCompress = 0;
      if( db->iMmap==1 ){
        /* Unlimited */
        pFS->nMapLimit = (i64)1 << 60;
      }else{
        /* iMmap is a limit in KB. Set nMapLimit to the same value in bytes. */
        pFS->nMapLimit = (i64)db->iMmap * 1024;
      }
    }
  }

  return LSM_OK;
}

/*
** Close and destroy a FileSystem object.
*/
static void lsmFsClose(FileSystem *pFS){
  if( pFS ){
    Page *pPg;
    lsm_env *pEnv = pFS->pEnv;

    assert( pFS->nOut==0 );
    pPg = pFS->pLruFirst;
    while( pPg ){
      Page *pNext = pPg->pLruNext;
      if( pPg->flags & PAGE_FREE ) lsmFree(pEnv, pPg->aData);
      lsmFree(pEnv, pPg);
      pPg = pNext;
    }

    pPg = pFS->pFree;
    while( pPg ){
      Page *pNext = pPg->pFreeNext;
      if( pPg->flags & PAGE_FREE ) lsmFree(pEnv, pPg->aData);
      lsmFree(pEnv, pPg);
      pPg = pNext;
    }

    if( pFS->fdDb ) lsmEnvClose(pFS->pEnv, pFS->fdDb );
    if( pFS->fdLog ) lsmEnvClose(pFS->pEnv, pFS->fdLog );
    lsmFree(pEnv, pFS->pLsmFile);
    lsmFree(pEnv, pFS->apHash);
    lsmFree(pEnv, pFS->aIBuffer);
    lsmFree(pEnv, pFS->aOBuffer);
    lsmFree(pEnv, pFS);
  }
}

/*
** This function is called when closing a database handle (i.e. lsm_close()) 
** if there exist other connections to the same database within this process.
** In that case the file-descriptor open on the database file is not closed
** when the FileSystem object is destroyed, as this would cause any POSIX
** locks held by the other connections to be silently dropped (see "man close"
** for details). Instead, the file-descriptor is stored in a list by the
** lsm_shared.c module until it is either closed or reused.
**
** This function returns a pointer to an object that can be linked into
** the list described above. The returned object now 'owns' the database
** file descriptr, so that when the FileSystem object is destroyed, it
** will not be closed. 
**
** This function may be called at most once in the life-time of a 
** FileSystem object. The results of any operations involving the database 
** file descriptor are undefined once this function has been called.
**
** None of this is necessary on non-POSIX systems. But we do it anyway in
** the name of using as similar code as possible on all platforms.
*/
static LsmFile *lsmFsDeferClose(FileSystem *pFS){
  LsmFile *p = pFS->pLsmFile;
  assert( p->pNext==0 );
  p->pFile = pFS->fdDb;
  pFS->fdDb = 0;
  pFS->pLsmFile = 0;
  return p;
}

/*
** Allocate a buffer and populate it with the output of the xFileid() 
** method of the database file handle. If successful, set *ppId to point 
** to the buffer and *pnId to the number of bytes in the buffer and return
** LSM_OK. Otherwise, set *ppId and *pnId to zero and return an LSM
** error code.
*/
static int lsmFsFileid(lsm_db *pDb, void **ppId, int *pnId){
  lsm_env *pEnv = pDb->pEnv;
  FileSystem *pFS = pDb->pFS;
  int rc;
  int nId = 0;
  void *pId;

  rc = pEnv->xFileid(pFS->fdDb, 0, &nId);
  pId = lsmMallocZeroRc(pEnv, nId, &rc);
  if( rc==LSM_OK ) rc = pEnv->xFileid(pFS->fdDb, pId, &nId);

  if( rc!=LSM_OK ){
    lsmFree(pEnv, pId);
    pId = 0;
    nId = 0;
  }

  *ppId = pId;
  *pnId = nId;
  return rc;
}

/*
** Return the nominal page-size used by this file-system. Actual pages
** may be smaller or larger than this value.
*/
static int lsmFsPageSize(FileSystem *pFS){
  return pFS->nPagesize;
}

/*
** Return the block-size used by this file-system.
*/
static int lsmFsBlockSize(FileSystem *pFS){
  return pFS->nBlocksize;
}

/*
** Configure the nominal page-size used by this file-system. Actual 
** pages may be smaller or larger than this value.
*/
static void lsmFsSetPageSize(FileSystem *pFS, int nPgsz){
  pFS->nPagesize = nPgsz;
  pFS->nCacheMax = 2048*1024 / pFS->nPagesize;
}

/*
** Configure the block-size used by this file-system. 
*/
static void lsmFsSetBlockSize(FileSystem *pFS, int nBlocksize){
  pFS->nBlocksize = nBlocksize;
}

/*
** Return the page number of the first page on block iBlock. Blocks are
** numbered starting from 1.
**
** For a compressed database, page numbers are byte offsets. The first
** page on each block is the byte offset immediately following the 4-byte
** "previous block" pointer at the start of each block.
*/
static LsmPgno fsFirstPageOnBlock(FileSystem *pFS, int iBlock){
  LsmPgno iPg;
  if( pFS->pCompress ){
    if( iBlock==1 ){
      iPg = pFS->nMetasize * 2 + 4;
    }else{
      iPg = pFS->nBlocksize * (LsmPgno)(iBlock-1) + 4;
    }
  }else{
    const i64 nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    if( iBlock==1 ){
      iPg = 1 + ((pFS->nMetasize*2 + pFS->nPagesize - 1) / pFS->nPagesize);
    }else{
      iPg = 1 + (iBlock-1) * nPagePerBlock;
    }
  }
  return iPg;
}

/*
** Return the page number of the last page on block iBlock. Blocks are
** numbered starting from 1.
**
** For a compressed database, page numbers are byte offsets. The first
** page on each block is the byte offset of the byte immediately before 
** the 4-byte "next block" pointer at the end of each block.
*/
static LsmPgno fsLastPageOnBlock(FileSystem *pFS, int iBlock){
  if( pFS->pCompress ){
    return pFS->nBlocksize * (LsmPgno)iBlock - 1 - 4;
  }else{
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    return iBlock * nPagePerBlock;
  }
}

/*
** Return the block number of the block that page iPg is located on. 
** Blocks are numbered starting from 1.
*/
static int fsPageToBlock(FileSystem *pFS, LsmPgno iPg){
  if( pFS->pCompress ){
    return (int)((iPg / pFS->nBlocksize) + 1);
  }else{
    return (int)(1 + ((iPg-1) / (pFS->nBlocksize / pFS->nPagesize)));
  }
}

/*
** Return true if page iPg is the last page on its block.
**
** This function is only called in non-compressed database mode.
*/
static int fsIsLast(FileSystem *pFS, LsmPgno iPg){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  assert( !pFS->pCompress );
  return ( iPg && (iPg % nPagePerBlock)==0 );
}

/*
** Return true if page iPg is the first page on its block.
**
** This function is only called in non-compressed database mode.
*/
static int fsIsFirst(FileSystem *pFS, LsmPgno iPg){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  assert( !pFS->pCompress );
  return ( (iPg % nPagePerBlock)==1
        || (iPg<nPagePerBlock && iPg==fsFirstPageOnBlock(pFS, 1))
  );
}

/*
** Given a page reference, return a pointer to the buffer containing the 
** pages contents. If parameter pnData is not NULL, set *pnData to the size
** of the buffer in bytes before returning.
*/
static u8 *lsmFsPageData(Page *pPage, int *pnData){
  if( pnData ){
    *pnData = pPage->nData;
  }
  return pPage->aData;
}

/*
** Return the page number of a page.
*/
static LsmPgno lsmFsPageNumber(Page *pPage){
  /* assert( (pPage->flags & PAGE_DIRTY)==0 ); */
  return pPage ? pPage->iPg : 0;
}

/*
** Page pPg is currently part of the LRU list belonging to pFS. Remove
** it from the list. pPg->pLruNext and pPg->pLruPrev are cleared by this
** operation.
*/
static void fsPageRemoveFromLru(FileSystem *pFS, Page *pPg){
  assert( pPg->pLruNext || pPg==pFS->pLruLast );
  assert( pPg->pLruPrev || pPg==pFS->pLruFirst );
  if( pPg->pLruNext ){
    pPg->pLruNext->pLruPrev = pPg->pLruPrev;
  }else{
    pFS->pLruLast = pPg->pLruPrev;
  }
  if( pPg->pLruPrev ){
    pPg->pLruPrev->pLruNext = pPg->pLruNext;
  }else{
    pFS->pLruFirst = pPg->pLruNext;
  }
  pPg->pLruPrev = 0;
  pPg->pLruNext = 0;
}

/*
** Page pPg is not currently part of the LRU list belonging to pFS. Add it.
*/
static void fsPageAddToLru(FileSystem *pFS, Page *pPg){
  assert( pPg->pLruNext==0 && pPg->pLruPrev==0 );
  pPg->pLruPrev = pFS->pLruLast;
  if( pPg->pLruPrev ){
    pPg->pLruPrev->pLruNext = pPg;
  }else{
    pFS->pLruFirst = pPg;
  }
  pFS->pLruLast = pPg;
}

/*
** Page pPg is currently stored in the apHash/nHash hash table. Remove it.
*/
static void fsPageRemoveFromHash(FileSystem *pFS, Page *pPg){
  int iHash;
  Page **pp;

  iHash = fsHashKey(pFS->nHash, pPg->iPg);
  for(pp=&pFS->apHash[iHash]; *pp!=pPg; pp=&(*pp)->pHashNext);
  *pp = pPg->pHashNext;
  pPg->pHashNext = 0;
}

/*
** Free a Page object allocated by fsPageBuffer().
*/
static void fsPageBufferFree(Page *pPg){
  pPg->pFS->nCacheAlloc--;
  lsmFree(pPg->pFS->pEnv, pPg->aData);
  lsmFree(pPg->pFS->pEnv, pPg);
}


/*
** Purge the cache of all non-mmap pages with nRef==0.
*/
static void lsmFsPurgeCache(FileSystem *pFS){
  Page *pPg;

  pPg = pFS->pLruFirst;
  while( pPg ){
    Page *pNext = pPg->pLruNext;
    assert( pPg->flags & PAGE_FREE );
    fsPageRemoveFromHash(pFS, pPg);
    fsPageBufferFree(pPg);
    pPg = pNext;
  }
  pFS->pLruFirst = 0;
  pFS->pLruLast = 0;

  assert( pFS->nCacheAlloc<=pFS->nOut && pFS->nCacheAlloc>=0 );
}

/*
** Search the hash-table for page iPg. If an entry is round, return a pointer
** to it. Otherwise, return NULL.
**
** Either way, if argument piHash is not NULL set *piHash to the hash slot
** number that page iPg would be stored in before returning.
*/
static Page *fsPageFindInHash(FileSystem *pFS, LsmPgno iPg, int *piHash){
  Page *p;                        /* Return value */
  int iHash = fsHashKey(pFS->nHash, iPg);

  if( piHash ) *piHash = iHash;
  for(p=pFS->apHash[iHash]; p; p=p->pHashNext){
    if( p->iPg==iPg) break;
  }
  return p;
}

/*
** Allocate and return a non-mmap Page object. If there are already 
** nCacheMax such Page objects outstanding, try to recycle an existing 
** Page instead.
*/
static int fsPageBuffer(
  FileSystem *pFS, 
  Page **ppOut
){
  int rc = LSM_OK;
  Page *pPage = 0;
  if( pFS->pLruFirst==0 || pFS->nCacheAlloc<pFS->nCacheMax ){
    /* Allocate a new Page object */
    pPage = lsmMallocZero(pFS->pEnv, sizeof(Page));
    if( !pPage ){
      rc = LSM_NOMEM_BKPT;
    }else{
      pPage->aData = (u8 *)lsmMalloc(pFS->pEnv, pFS->nPagesize);
      if( !pPage->aData ){
        lsmFree(pFS->pEnv, pPage);
        rc = LSM_NOMEM_BKPT;
        pPage = 0;
      }else{
        pFS->nCacheAlloc++;
      }
    }
  }else{
    /* Reuse an existing Page object */
    u8 *aData;
    pPage = pFS->pLruFirst;
    aData = pPage->aData;
    fsPageRemoveFromLru(pFS, pPage);
    fsPageRemoveFromHash(pFS, pPage);

    memset(pPage, 0, sizeof(Page));
    pPage->aData = aData;
  }

  if( pPage ){
    pPage->flags = PAGE_FREE;
  }
  *ppOut = pPage;
  return rc;
}

/*
** Assuming *pRc is initially LSM_OK, attempt to ensure that the 
** memory-mapped region is at least iSz bytes in size. If it is not already,
** iSz bytes in size, extend it and update the pointers associated with any
** outstanding Page objects.
**
** If *pRc is not LSM_OK when this function is called, it is a no-op. 
** Otherwise, *pRc is set to an lsm error code if an error occurs, or
** left unmodified otherwise.
**
** This function is never called in compressed database mode.
*/
static void fsGrowMapping(
  FileSystem *pFS,                /* File system object */
  i64 iSz,                        /* Minimum size to extend mapping to */
  int *pRc                        /* IN/OUT: Error code */
){
  assert( PAGE_HASPREV==4 );

  if( *pRc==LSM_OK && iSz>pFS->nMap ){
    int rc;
    u8 *aOld = pFS->pMap;
    rc = lsmEnvRemap(pFS->pEnv, pFS->fdDb, iSz, &pFS->pMap, &pFS->nMap);
    if( rc==LSM_OK && pFS->pMap!=aOld ){
      Page *pFix;
      i64 iOff = (u8 *)pFS->pMap - aOld;
      for(pFix=pFS->pMapped; pFix; pFix=pFix->pMappedNext){
        pFix->aData += iOff;
      }
      lsmSortedRemap(pFS->pDb);
    }
    *pRc = rc;
  }
}

/*
** If it is mapped, unmap the database file.
*/
static int lsmFsUnmap(FileSystem *pFS){
  int rc = LSM_OK;
  if( pFS ){
    rc = lsmEnvRemap(pFS->pEnv, pFS->fdDb, -1, &pFS->pMap, &pFS->nMap);
  }
  return rc;
}

/*
** fsync() the database file.
*/
static int lsmFsSyncDb(FileSystem *pFS, int nBlock){
  return lsmEnvSync(pFS->pEnv, pFS->fdDb);
}

/*
** If block iBlk has been redirected according to the redirections in the
** object passed as the first argument, return the destination block to
** which it is redirected. Otherwise, return a copy of iBlk.
*/
static int fsRedirectBlock(Redirect *p, int iBlk){
  if( p ){
    int i;
    for(i=0; i<p->n; i++){
      if( iBlk==p->a[i].iFrom ) return p->a[i].iTo;
    }
  }
  assert( iBlk!=0 );
  return iBlk;
}

/*
** If page iPg has been redirected according to the redirections in the
** object passed as the second argument, return the destination page to
** which it is redirected. Otherwise, return a copy of iPg.
*/
static LsmPgno lsmFsRedirectPage(FileSystem *pFS, Redirect *pRedir, LsmPgno iPg){
  LsmPgno iReal = iPg;

  if( pRedir ){
    const int nPagePerBlock = (
        pFS->pCompress ? pFS->nBlocksize : (pFS->nBlocksize / pFS->nPagesize)
    );
    int iBlk = fsPageToBlock(pFS, iPg);
    int i;
    for(i=0; i<pRedir->n; i++){
      int iFrom = pRedir->a[i].iFrom;
      if( iFrom>iBlk ) break;
      if( iFrom==iBlk ){
        int iTo = pRedir->a[i].iTo;
        iReal = iPg - (LsmPgno)(iFrom - iTo) * nPagePerBlock;
        if( iTo==1 ){
          iReal += (fsFirstPageOnBlock(pFS, 1)-1);
        }
        break;
      }
    }
  }

  assert( iReal!=0 );
  return iReal;
}

/* Required by the circular fsBlockNext<->fsPageGet dependency. */
static int fsPageGet(FileSystem *, Segment *, LsmPgno, int, Page **, int *);

/*
** Parameter iBlock is a database file block. This function reads the value 
** stored in the blocks "next block" pointer and stores it in *piNext.
** LSM_OK is returned if everything is successful, or an LSM error code
** otherwise.
*/
static int fsBlockNext(
  FileSystem *pFS,                /* File-system object handle */
  Segment *pSeg,                  /* Use this segment for block redirects */
  int iBlock,                     /* Read field from this block */
  int *piNext                     /* OUT: Next block in linked list */
){
  int rc;
  int iRead;                      /* Read block from here */
  
  if( pSeg ){
    iRead = fsRedirectBlock(pSeg->pRedirect, iBlock);
  }else{
    iRead = iBlock;
  }

  assert( pFS->nMapLimit==0 || pFS->pCompress==0 );
  if( pFS->pCompress ){
    i64 iOff;                     /* File offset to read data from */
    u8 aNext[4];                  /* 4-byte pointer read from db file */

    iOff = (i64)iRead * pFS->nBlocksize - sizeof(aNext);
    rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, aNext, sizeof(aNext));
    if( rc==LSM_OK ){
      *piNext = (int)lsmGetU32(aNext);
    }
  }else{
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    Page *pLast;
    rc = fsPageGet(pFS, 0, iRead*nPagePerBlock, 0, &pLast, 0);
    if( rc==LSM_OK ){
      *piNext = lsmGetU32(&pLast->aData[pFS->nPagesize-4]);
      lsmFsPageRelease(pLast);
    }
  }

  if( pSeg ){
    *piNext = fsRedirectBlock(pSeg->pRedirect, *piNext);
  }
  return rc;
}

/*
** Return the page number of the last page on the same block as page iPg.
*/
LsmPgno fsLastPageOnPagesBlock(FileSystem *pFS, LsmPgno iPg){
  return fsLastPageOnBlock(pFS, fsPageToBlock(pFS, iPg));
}

/*
** Read nData bytes of data from offset iOff of the database file into
** buffer aData. If this means reading past the end of a block, follow
** the block pointer to the next block and continue reading.
**
** Offset iOff is an absolute offset - not subject to any block redirection.
** However any block pointer followed is. Use pSeg->pRedirect in this case.
**
** This function is only called in compressed database mode.
*/
static int fsReadData(
  FileSystem *pFS,                /* File-system handle */
  Segment *pSeg,                  /* Block redirection */
  i64 iOff,                       /* Read data from this offset */
  u8 *aData,                      /* Buffer to read data into */
  int nData                       /* Number of bytes to read */
){
  i64 iEob;                       /* End of block */
  int nRead;
  int rc;

  assert( pFS->pCompress );

  iEob = fsLastPageOnPagesBlock(pFS, iOff) + 1;
  nRead = (int)LSM_MIN(iEob - iOff, nData);

  rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, aData, nRead);
  if( rc==LSM_OK && nRead!=nData ){
    int iBlk;

    rc = fsBlockNext(pFS, pSeg, fsPageToBlock(pFS, iOff), &iBlk);
    if( rc==LSM_OK ){
      i64 iOff2 = fsFirstPageOnBlock(pFS, iBlk);
      rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff2, &aData[nRead], nData-nRead);
    }
  }

  return rc;
}

/*
** Parameter iBlock is a database file block. This function reads the value 
** stored in the blocks "previous block" pointer and stores it in *piPrev.
** LSM_OK is returned if everything is successful, or an LSM error code
** otherwise.
*/
static int fsBlockPrev(
  FileSystem *pFS,                /* File-system object handle */
  Segment *pSeg,                  /* Use this segment for block redirects */
  int iBlock,                     /* Read field from this block */
  int *piPrev                     /* OUT: Previous block in linked list */
){
  int rc = LSM_OK;                /* Return code */

  assert( pFS->nMapLimit==0 || pFS->pCompress==0 );
  assert( iBlock>0 );

  if( pFS->pCompress ){
    i64 iOff = fsFirstPageOnBlock(pFS, iBlock) - 4;
    u8 aPrev[4];                  /* 4-byte pointer read from db file */
    rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, aPrev, sizeof(aPrev));
    if( rc==LSM_OK ){
      Redirect *pRedir = (pSeg ? pSeg->pRedirect : 0);
      *piPrev = fsRedirectBlock(pRedir, (int)lsmGetU32(aPrev));
    }
  }else{
    assert( 0 );
  }
  return rc;
}

/*
** Encode and decode routines for record size fields.
*/
static void putRecordSize(u8 *aBuf, int nByte, int bFree){
  aBuf[0] = (u8)(nByte >> 14) | 0x80;
  aBuf[1] = ((u8)(nByte >>  7) & 0x7F) | (bFree ? 0x00 : 0x80);
  aBuf[2] = (u8)nByte | 0x80;
}
static int getRecordSize(u8 *aBuf, int *pbFree){
  int nByte;
  nByte  = (aBuf[0] & 0x7F) << 14;
  nByte += (aBuf[1] & 0x7F) << 7;
  nByte += (aBuf[2] & 0x7F);
  *pbFree = !(aBuf[1] & 0x80);
  return nByte;
}

/*
** Subtract iSub from database file offset iOff and set *piRes to the
** result. If doing so means passing the start of a block, follow the
** block pointer stored in the first 4 bytes of the block.
**
** Offset iOff is an absolute offset - not subject to any block redirection.
** However any block pointer followed is. Use pSeg->pRedirect in this case.
**
** Return LSM_OK if successful or an lsm error code if an error occurs.
*/
static int fsSubtractOffset(
  FileSystem *pFS, 
  Segment *pSeg,
  i64 iOff, 
  int iSub, 
  i64 *piRes
){
  i64 iStart;
  int iBlk = 0;
  int rc;

  assert( pFS->pCompress );

  iStart = fsFirstPageOnBlock(pFS, fsPageToBlock(pFS, iOff));
  if( (iOff-iSub)>=iStart ){
    *piRes = (iOff-iSub);
    return LSM_OK;
  }

  rc = fsBlockPrev(pFS, pSeg, fsPageToBlock(pFS, iOff), &iBlk);
  *piRes = fsLastPageOnBlock(pFS, iBlk) - iSub + (iOff - iStart + 1);
  return rc;
}

/*
** Add iAdd to database file offset iOff and set *piRes to the
** result. If doing so means passing the end of a block, follow the
** block pointer stored in the last 4 bytes of the block.
**
** Offset iOff is an absolute offset - not subject to any block redirection.
** However any block pointer followed is. Use pSeg->pRedirect in this case.
**
** Return LSM_OK if successful or an lsm error code if an error occurs.
*/
static int fsAddOffset(
  FileSystem *pFS, 
  Segment *pSeg,
  i64 iOff, 
  int iAdd, 
  i64 *piRes
){
  i64 iEob;
  int iBlk;
  int rc;

  assert( pFS->pCompress );

  iEob = fsLastPageOnPagesBlock(pFS, iOff);
  if( (iOff+iAdd)<=iEob ){
    *piRes = (iOff+iAdd);
    return LSM_OK;
  }

  rc = fsBlockNext(pFS, pSeg, fsPageToBlock(pFS, iOff), &iBlk);
  *piRes = fsFirstPageOnBlock(pFS, iBlk) + iAdd - (iEob - iOff + 1);
  return rc;
}

/*
** If it is not already allocated, allocate either the FileSystem.aOBuffer (if
** bWrite is true) or the FileSystem.aIBuffer (if bWrite is false). Return
** LSM_OK if successful if the attempt to allocate memory fails.
*/
static int fsAllocateBuffer(FileSystem *pFS, int bWrite){
  u8 **pp;                        /* Pointer to either aIBuffer or aOBuffer */

  assert( pFS->pCompress );

  /* If neither buffer has been allocated, figure out how large they
  ** should be. Store this value in FileSystem.nBuffer.  */
  if( pFS->nBuffer==0 ){
    assert( pFS->aIBuffer==0 && pFS->aOBuffer==0 );
    pFS->nBuffer = pFS->pCompress->xBound(pFS->pCompress->pCtx, pFS->nPagesize);
    if( pFS->nBuffer<(pFS->szSector+6) ){
      pFS->nBuffer = pFS->szSector+6;
    }
  }

  pp = (bWrite ? &pFS->aOBuffer : &pFS->aIBuffer);
  if( *pp==0 ){
    *pp = lsmMalloc(pFS->pEnv, LSM_MAX(pFS->nBuffer, pFS->nPagesize));
    if( *pp==0 ) return LSM_NOMEM_BKPT;
  }

  return LSM_OK;
}

/*
** This function is only called in compressed database mode. It reads and
** uncompresses the compressed data for page pPg from the database and
** populates the pPg->aData[] buffer and pPg->nCompress field.
**
** It is possible that instead of a page record, there is free space
** at offset pPg->iPgno. In this case no data is read from the file, but
** output variable *pnSpace is set to the total number of free bytes.
**
** LSM_OK is returned if successful, or an LSM error code otherwise.
*/
static int fsReadPagedata(
  FileSystem *pFS,                /* File-system handle */
  Segment *pSeg,                  /* pPg is part of this segment */
  Page *pPg,                      /* Page to read and uncompress data for */
  int *pnSpace                    /* OUT: Total bytes of free space */
){
  lsm_compress *p = pFS->pCompress;
  i64 iOff = pPg->iPg;
  u8 aSz[3];
  int rc;

  assert( p && pPg->nCompress==0 );

  if( fsAllocateBuffer(pFS, 0) ) return LSM_NOMEM;

  rc = fsReadData(pFS, pSeg, iOff, aSz, sizeof(aSz));

  if( rc==LSM_OK ){
    int bFree;
    if( aSz[0] & 0x80 ){
      pPg->nCompress = (int)getRecordSize(aSz, &bFree);
    }else{
      pPg->nCompress = (int)aSz[0] - sizeof(aSz)*2;
      bFree = 1;
    }
    if( bFree ){
      if( pnSpace ){
        *pnSpace = pPg->nCompress + sizeof(aSz)*2;
      }else{
        rc = LSM_CORRUPT_BKPT;
      }
    }else{
      rc = fsAddOffset(pFS, pSeg, iOff, 3, &iOff);
      if( rc==LSM_OK ){
        if( pPg->nCompress>pFS->nBuffer ){
          rc = LSM_CORRUPT_BKPT;
        }else{
          rc = fsReadData(pFS, pSeg, iOff, pFS->aIBuffer, pPg->nCompress);
        }
        if( rc==LSM_OK ){
          int n = pFS->nPagesize;
          rc = p->xUncompress(p->pCtx, 
              (char *)pPg->aData, &n, 
              (const char *)pFS->aIBuffer, pPg->nCompress
          );
          if( rc==LSM_OK && n!=pPg->pFS->nPagesize ){
            rc = LSM_CORRUPT_BKPT;
          }
        }
      }
    }
  }
  return rc;
}

/*
** Return a handle for a database page.
**
** If this file-system object is accessing a compressed database it may be
** that there is no page record at database file offset iPg. Instead, there
** may be a free space record. In this case, set *ppPg to NULL and *pnSpace
** to the total number of free bytes before returning.
**
** If no error occurs, LSM_OK is returned. Otherwise, an lsm error code.
*/
static int fsPageGet(
  FileSystem *pFS,                /* File-system handle */
  Segment *pSeg,                  /* Block redirection to use (or NULL) */
  LsmPgno iPg,                    /* Page id */
  int noContent,                  /* True to not load content from disk */
  Page **ppPg,                    /* OUT: New page handle */
  int *pnSpace                    /* OUT: Bytes of free space */
){
  Page *p;
  int iHash;
  int rc = LSM_OK;

  /* In most cases iReal is the same as iPg. Except, if pSeg->pRedirect is 
  ** not NULL, and the block containing iPg has been redirected, then iReal
  ** is the page number after redirection.  */
  LsmPgno iReal = lsmFsRedirectPage(pFS, (pSeg ? pSeg->pRedirect : 0), iPg);

  assert_lists_are_ok(pFS);
  assert( iPg>=fsFirstPageOnBlock(pFS, 1) );
  assert( iReal>=fsFirstPageOnBlock(pFS, 1) );
  *ppPg = 0;

  /* Search the hash-table for the page */
  p = fsPageFindInHash(pFS, iReal, &iHash);

  if( p ){
    assert( p->flags & PAGE_FREE );
    if( p->nRef==0 ) fsPageRemoveFromLru(pFS, p);
  }else{

    if( fsMmapPage(pFS, iReal) ){
      i64 iEnd = (i64)iReal * pFS->nPagesize;
      fsGrowMapping(pFS, iEnd, &rc);
      if( rc!=LSM_OK ) return rc;

      if( pFS->pFree ){
        p = pFS->pFree;
        pFS->pFree = p->pFreeNext;
        assert( p->nRef==0 );
      }else{
        p = lsmMallocZeroRc(pFS->pEnv, sizeof(Page), &rc);
        if( rc ) return rc;
        p->pFS = pFS;
      }
      p->aData = &((u8 *)pFS->pMap)[pFS->nPagesize * (iReal-1)];
      p->iPg = iReal;

      /* This page now carries a pointer to the mapping. Link it in to
      ** the FileSystem.pMapped list.  */
      assert( p->pMappedNext==0 );
      p->pMappedNext = pFS->pMapped;
      pFS->pMapped = p;

      assert( pFS->pCompress==0 );
      assert( (p->flags & PAGE_FREE)==0 );
    }else{
      rc = fsPageBuffer(pFS, &p);
      if( rc==LSM_OK ){
        int nSpace = 0;
        p->iPg = iReal;
        p->nRef = 0;
        p->pFS = pFS;
        assert( p->flags==0 || p->flags==PAGE_FREE );

#ifdef LSM_DEBUG
        memset(p->aData, 0x56, pFS->nPagesize);
#endif
        assert( p->pLruNext==0 && p->pLruPrev==0 );
        if( noContent==0 ){
          if( pFS->pCompress ){
            rc = fsReadPagedata(pFS, pSeg, p, &nSpace);
          }else{
            int nByte = pFS->nPagesize;
            i64 iOff = (i64)(iReal-1) * pFS->nPagesize;
            rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, p->aData, nByte);
          }
          pFS->nRead++;
        }

        /* If the xRead() call was successful (or not attempted), link the
        ** page into the page-cache hash-table. Otherwise, if it failed,
        ** free the buffer. */
        if( rc==LSM_OK && nSpace==0 ){
          p->pHashNext = pFS->apHash[iHash];
          pFS->apHash[iHash] = p;
        }else{
          fsPageBufferFree(p);
          p = 0;
          if( pnSpace ) *pnSpace = nSpace;
        }
      }
    }

    assert( (rc==LSM_OK && (p || (pnSpace && *pnSpace)))
         || (rc!=LSM_OK && p==0) 
    );
  }

  if( rc==LSM_OK && p ){
    if( pFS->pCompress==0 && (fsIsLast(pFS, iReal) || fsIsFirst(pFS, iReal)) ){
      p->nData = pFS->nPagesize - 4;
      if( fsIsFirst(pFS, iReal) && p->nRef==0 ){
        p->aData += 4;
        p->flags |= PAGE_HASPREV;
      }
    }else{
      p->nData = pFS->nPagesize;
    }
    pFS->nOut += (p->nRef==0);
    p->nRef++;
  }
  *ppPg = p;
  return rc;
}

/*
** Read the 64-bit checkpoint id of the checkpoint currently stored on meta
** page iMeta of the database file. If no error occurs, store the id value
** in *piVal and return LSM_OK. Otherwise, return an LSM error code and leave
** *piVal unmodified.
**
** If a checkpointer connection is currently updating meta-page iMeta, or an
** earlier checkpointer crashed while doing so, the value read into *piVal
** may be garbage. It is the callers responsibility to deal with this.
*/
static int lsmFsReadSyncedId(lsm_db *db, int iMeta, i64 *piVal){
  FileSystem *pFS = db->pFS;
  int rc = LSM_OK;

  assert( iMeta==1 || iMeta==2 );
  if( pFS->nMapLimit>0 ){
    fsGrowMapping(pFS, iMeta*LSM_META_PAGE_SIZE, &rc);
    if( rc==LSM_OK ){
      *piVal = (i64)lsmGetU64(&((u8 *)pFS->pMap)[(iMeta-1)*LSM_META_PAGE_SIZE]);
    }
  }else{
    MetaPage *pMeta = 0;
    rc = lsmFsMetaPageGet(pFS, 0, iMeta, &pMeta);
    if( rc==LSM_OK ){
      *piVal = (i64)lsmGetU64(pMeta->aData);
      lsmFsMetaPageRelease(pMeta);
    }
  }

  return rc;
}


/*
** Return true if the first or last page of segment pRun falls between iFirst
** and iLast, inclusive, and pRun is not equal to pIgnore.
*/
static int fsRunEndsBetween(
  Segment *pRun, 
  Segment *pIgnore, 
  LsmPgno iFirst, 
  LsmPgno iLast
){
  return (pRun!=pIgnore && (
        (pRun->iFirst>=iFirst && pRun->iFirst<=iLast)
     || (pRun->iLastPg>=iFirst && pRun->iLastPg<=iLast)
  ));
}

/*
** Return true if level pLevel contains a segment other than pIgnore for
** which the first or last page is between iFirst and iLast, inclusive.
*/
static int fsLevelEndsBetween(
  Level *pLevel, 
  Segment *pIgnore, 
  LsmPgno iFirst, 
  LsmPgno iLast
){
  int i;

  if( fsRunEndsBetween(&pLevel->lhs, pIgnore, iFirst, iLast) ){
    return 1;
  }
  for(i=0; i<pLevel->nRight; i++){
    if( fsRunEndsBetween(&pLevel->aRhs[i], pIgnore, iFirst, iLast) ){
      return 1;
    }
  }

  return 0;
}

/*
** Block iBlk is no longer in use by segment pIgnore. If it is not in use
** by any other segment, move it to the free block list.
*/
static int fsFreeBlock(
  FileSystem *pFS,                /* File system object */
  Snapshot *pSnapshot,            /* Worker snapshot */
  Segment *pIgnore,               /* Ignore this run when searching */
  int iBlk                        /* Block number of block to free */
){
  int rc = LSM_OK;                /* Return code */
  LsmPgno iFirst;                 /* First page on block iBlk */
  LsmPgno iLast;                  /* Last page on block iBlk */
  Level *pLevel;                  /* Used to iterate through levels */

  int iIn;                        /* Used to iterate through append points */
  int iOut = 0;                   /* Used to output append points */
  LsmPgno *aApp = pSnapshot->aiAppend;

  iFirst = fsFirstPageOnBlock(pFS, iBlk);
  iLast = fsLastPageOnBlock(pFS, iBlk);

  /* Check if any other run in the snapshot has a start or end page 
  ** within this block. If there is such a run, return early. */
  for(pLevel=lsmDbSnapshotLevel(pSnapshot); pLevel; pLevel=pLevel->pNext){
    if( fsLevelEndsBetween(pLevel, pIgnore, iFirst, iLast) ){
      return LSM_OK;
    }
  }

  /* Remove any entries that lie on this block from the append-list. */
  for(iIn=0; iIn<LSM_APPLIST_SZ; iIn++){
    if( aApp[iIn]<iFirst || aApp[iIn]>iLast ){
      aApp[iOut++] = aApp[iIn];
    }
  }
  while( iOut<LSM_APPLIST_SZ ) aApp[iOut++] = 0;

  if( rc==LSM_OK ){
    rc = lsmBlockFree(pFS->pDb, iBlk);
  }
  return rc;
}

/*
** Delete or otherwise recycle the blocks currently occupied by run pDel.
*/
static int lsmFsSortedDelete(
  FileSystem *pFS, 
  Snapshot *pSnapshot,
  int bZero,                      /* True to zero the Segment structure */
  Segment *pDel
){
  if( pDel->iFirst ){
    int rc = LSM_OK;

    int iBlk;
    int iLastBlk;

    iBlk = fsPageToBlock(pFS, pDel->iFirst);
    iLastBlk = fsPageToBlock(pFS, pDel->iLastPg);

    /* Mark all blocks currently used by this sorted run as free */
    while( iBlk && rc==LSM_OK ){
      int iNext = 0;
      if( iBlk!=iLastBlk ){
        rc = fsBlockNext(pFS, pDel, iBlk, &iNext);
      }else if( bZero==0 && pDel->iLastPg!=fsLastPageOnBlock(pFS, iLastBlk) ){
        break;
      }
      rc = fsFreeBlock(pFS, pSnapshot, pDel, iBlk);
      iBlk = iNext;
    }

    if( pDel->pRedirect ){
      assert( pDel->pRedirect==&pSnapshot->redirect );
      pSnapshot->redirect.n = 0;
    }

    if( bZero ) memset(pDel, 0, sizeof(Segment));
  }
  return LSM_OK;
}

/*
** aPgno is an array containing nPgno page numbers. Return the smallest page
** number from the array that falls on block iBlk. Or, if none of the pages
** in aPgno[] fall on block iBlk, return 0.
*/
static LsmPgno firstOnBlock(
  FileSystem *pFS, 
  int iBlk, 
  LsmPgno *aPgno, 
  int nPgno
){
  LsmPgno iRet = 0;
  int i;
  for(i=0; i<nPgno; i++){
    LsmPgno iPg = aPgno[i];
    if( fsPageToBlock(pFS, iPg)==iBlk && (iRet==0 || iPg<iRet) ){
      iRet = iPg;
    }
  }
  return iRet;
}

#ifndef NDEBUG
/*
** Return true if page iPg, which is a part of segment p, lies on
** a redirected block. 
*/
static int fsPageRedirects(FileSystem *pFS, Segment *p, LsmPgno iPg){
  return (iPg!=0 && iPg!=lsmFsRedirectPage(pFS, p->pRedirect, iPg));
}

/*
** Return true if the second argument is not NULL and any of the first
** last or root pages lie on a redirected block. 
*/
static int fsSegmentRedirects(FileSystem *pFS, Segment *p){
  return (p && (
      fsPageRedirects(pFS, p, p->iFirst)
   || fsPageRedirects(pFS, p, p->iRoot)
   || fsPageRedirects(pFS, p, p->iLastPg)
  ));
}
#endif

/*
** Argument aPgno is an array of nPgno page numbers. All pages belong to
** the segment pRun. This function gobbles from the start of the run to the
** first page that appears in aPgno[] (i.e. so that the aPgno[] entry is
** the new first page of the run).
*/
static void lsmFsGobble(
  lsm_db *pDb,
  Segment *pRun, 
  LsmPgno *aPgno,
  int nPgno
){
  int rc = LSM_OK;
  FileSystem *pFS = pDb->pFS;
  Snapshot *pSnapshot = pDb->pWorker;
  int iBlk;

  assert( pRun->nSize>0 );
  assert( 0==fsSegmentRedirects(pFS, pRun) );
  assert( nPgno>0 && 0==fsPageRedirects(pFS, pRun, aPgno[0]) );

  iBlk = fsPageToBlock(pFS, pRun->iFirst);
  pRun->nSize += (pRun->iFirst - fsFirstPageOnBlock(pFS, iBlk));

  while( rc==LSM_OK ){
    int iNext = 0;
    LsmPgno iFirst = firstOnBlock(pFS, iBlk, aPgno, nPgno);
    if( iFirst ){
      pRun->iFirst = iFirst;
      break;
    }
    rc = fsBlockNext(pFS, pRun, iBlk, &iNext);
    if( rc==LSM_OK ) rc = fsFreeBlock(pFS, pSnapshot, pRun, iBlk);
    pRun->nSize -= (
        1 + fsLastPageOnBlock(pFS, iBlk) - fsFirstPageOnBlock(pFS, iBlk)
    );
    iBlk = iNext;
  }

  pRun->nSize -= (pRun->iFirst - fsFirstPageOnBlock(pFS, iBlk));
  assert( pRun->nSize>0 );
}

/*
** This function is only used in compressed database mode.
**
** Argument iPg is the page number (byte offset) of a page within segment
** pSeg. The page record, including all headers, is nByte bytes in size.
** Before returning, set *piNext to the page number of the next page in
** the segment, or to zero if iPg is the last.
**
** In other words, do:
**
**   *piNext = iPg + nByte;
**
** But take block overflow and redirection into account.
*/
static int fsNextPageOffset(
  FileSystem *pFS,                /* File system object */
  Segment *pSeg,                  /* Segment to move within */
  LsmPgno iPg,                    /* Offset of current page */
  int nByte,                      /* Size of current page including headers */
  LsmPgno *piNext                 /* OUT: Offset of next page. Or zero (EOF) */
){
  LsmPgno iNext;
  int rc;

  assert( pFS->pCompress );

  rc = fsAddOffset(pFS, pSeg, iPg, nByte-1, &iNext);
  if( pSeg && iNext==pSeg->iLastPg ){
    iNext = 0;
  }else if( rc==LSM_OK ){
    rc = fsAddOffset(pFS, pSeg, iNext, 1, &iNext);
  }

  *piNext = iNext;
  return rc;
}

/*
** This function is only used in compressed database mode.
**
** Argument iPg is the page number of a pagethat appears in segment pSeg.
** This function determines the page number of the previous page in the
** same run. *piPrev is set to the previous page number before returning.
**
** LSM_OK is returned if no error occurs. Otherwise, an lsm error code.
** If any value other than LSM_OK is returned, then the final value of
** *piPrev is undefined.
*/
static int fsGetPageBefore(
  FileSystem *pFS, 
  Segment *pSeg, 
  LsmPgno iPg, 
  LsmPgno *piPrev
){
  u8 aSz[3];
  int rc;
  i64 iRead;

  assert( pFS->pCompress );

  rc = fsSubtractOffset(pFS, pSeg, iPg, sizeof(aSz), &iRead);
  if( rc==LSM_OK ) rc = fsReadData(pFS, pSeg, iRead, aSz, sizeof(aSz));

  if( rc==LSM_OK ){
    int bFree;
    int nSz;
    if( aSz[2] & 0x80 ){
      nSz = getRecordSize(aSz, &bFree) + sizeof(aSz)*2;
    }else{
      nSz = (int)(aSz[2] & 0x7F);
      bFree = 1;
    }
    rc = fsSubtractOffset(pFS, pSeg, iPg, nSz, piPrev);
  }

  return rc;
}

/*
** The first argument to this function is a valid reference to a database
** file page that is part of a sorted run. If parameter eDir is -1, this 
** function attempts to locate and load the previous page in the same run. 
** Or, if eDir is +1, it attempts to find the next page in the same run.
** The results of passing an eDir value other than positive or negative one
** are undefined.
**
** If parameter pRun is not NULL then it must point to the run that page
** pPg belongs to. In this case, if pPg is the first or last page of the
** run, and the request is for the previous or next page, respectively,
** *ppNext is set to NULL before returning LSM_OK. If pRun is NULL, then it
** is assumed that the next or previous page, as requested, exists.
**
** If the previous/next page does exist and is successfully loaded, *ppNext
** is set to point to it and LSM_OK is returned. Otherwise, if an error 
** occurs, *ppNext is set to NULL and and lsm error code returned.
**
** Page references returned by this function should be released by the 
** caller using lsmFsPageRelease().
*/
static int lsmFsDbPageNext(Segment *pRun, Page *pPg, int eDir, Page **ppNext){
  int rc = LSM_OK;
  FileSystem *pFS = pPg->pFS;
  LsmPgno iPg = pPg->iPg;

  assert( 0==fsSegmentRedirects(pFS, pRun) );
  if( pFS->pCompress ){
    int nSpace = pPg->nCompress + 2*3;

    do {
      if( eDir>0 ){
        rc = fsNextPageOffset(pFS, pRun, iPg, nSpace, &iPg);
      }else{
        if( iPg==pRun->iFirst ){
          iPg = 0;
        }else{
          rc = fsGetPageBefore(pFS, pRun, iPg, &iPg);
        }
      }

      nSpace = 0;
      if( iPg!=0 ){
        rc = fsPageGet(pFS, pRun, iPg, 0, ppNext, &nSpace);
        assert( (*ppNext==0)==(rc!=LSM_OK || nSpace>0) );
      }else{
        *ppNext = 0;
      }
    }while( nSpace>0 && rc==LSM_OK );

  }else{
    Redirect *pRedir = pRun ? pRun->pRedirect : 0;
    assert( eDir==1 || eDir==-1 );
    if( eDir<0 ){
      if( pRun && iPg==pRun->iFirst ){
        *ppNext = 0;
        return LSM_OK;
      }else if( fsIsFirst(pFS, iPg) ){
        assert( pPg->flags & PAGE_HASPREV );
        iPg = fsLastPageOnBlock(pFS, lsmGetU32(&pPg->aData[-4]));
      }else{
        iPg--;
      }
    }else{
      if( pRun ){
        if( iPg==pRun->iLastPg ){
          *ppNext = 0;
          return LSM_OK;
        }
      }

      if( fsIsLast(pFS, iPg) ){
        int iBlk = fsRedirectBlock(
            pRedir, lsmGetU32(&pPg->aData[pFS->nPagesize-4])
        );
        iPg = fsFirstPageOnBlock(pFS, iBlk);
      }else{
        iPg++;
      }
    }
    rc = fsPageGet(pFS, pRun, iPg, 0, ppNext, 0);
  }

  return rc;
}

/*
** This function is called when creating a new segment to determine if the
** first part of it can be written following an existing segment on an
** already allocated block. If it is possible, the page number of the first
** page to use for the new segment is returned. Otherwise zero.
**
** If argument pLvl is not NULL, then this function will not attempt to
** start the new segment immediately following any segment that is part
** of the right-hand-side of pLvl.
*/
static LsmPgno findAppendPoint(FileSystem *pFS, Level *pLvl){
  int i;
  LsmPgno *aiAppend = pFS->pDb->pWorker->aiAppend;
  LsmPgno iRet = 0;

  for(i=LSM_APPLIST_SZ-1; iRet==0 && i>=0; i--){
    if( (iRet = aiAppend[i]) ){
      if( pLvl ){
        int iBlk = fsPageToBlock(pFS, iRet);
        int j;
        for(j=0; iRet && j<pLvl->nRight; j++){
          if( fsPageToBlock(pFS, pLvl->aRhs[j].iLastPg)==iBlk ){
            iRet = 0;
          }
        }
      }
      if( iRet ) aiAppend[i] = 0;
    }
  }
  return iRet;
}

/*
** Append a page to the left-hand-side of pLvl. Set the ref-count to 1 and
** return a pointer to it. The page is writable until either 
** lsmFsPagePersist() is called on it or the ref-count drops to zero.
*/
static int lsmFsSortedAppend(
  FileSystem *pFS, 
  Snapshot *pSnapshot,
  Level *pLvl,
  int bDefer,
  Page **ppOut
){
  int rc = LSM_OK;
  Page *pPg = 0;
  LsmPgno iApp = 0;
  LsmPgno iNext = 0;
  Segment *p = &pLvl->lhs;
  LsmPgno iPrev = p->iLastPg;

  *ppOut = 0;
  assert( p->pRedirect==0 );

  if( pFS->pCompress || bDefer ){
    /* In compressed database mode the page is not assigned a page number
    ** or location in the database file at this point. This will be done
    ** by the lsmFsPagePersist() call.  */
    rc = fsPageBuffer(pFS, &pPg);
    if( rc==LSM_OK ){
      pPg->pFS = pFS;
      pPg->pSeg = p;
      pPg->iPg = 0;
      pPg->flags |= PAGE_DIRTY;
      pPg->nData = pFS->nPagesize;
      assert( pPg->aData );
      if( pFS->pCompress==0 ) pPg->nData -= 4;

      pPg->nRef = 1;
      pFS->nOut++;
    }
  }else{
    if( iPrev==0 ){
      iApp = findAppendPoint(pFS, pLvl);
    }else if( fsIsLast(pFS, iPrev) ){
      int iNext2;
      rc = fsBlockNext(pFS, 0, fsPageToBlock(pFS, iPrev), &iNext2);
      if( rc!=LSM_OK ) return rc;
      iApp = fsFirstPageOnBlock(pFS, iNext2);
    }else{
      iApp = iPrev + 1;
    }

    /* If this is the first page allocated, or if the page allocated is the
    ** last in the block, also allocate the next block here.  */
    if( iApp==0 || fsIsLast(pFS, iApp) ){
      int iNew;                     /* New block number */

      rc = lsmBlockAllocate(pFS->pDb, 0, &iNew);
      if( rc!=LSM_OK ) return rc;
      if( iApp==0 ){
        iApp = fsFirstPageOnBlock(pFS, iNew);
      }else{
        iNext = fsFirstPageOnBlock(pFS, iNew);
      }
    }

    /* Grab the new page. */
    pPg = 0;
    rc = fsPageGet(pFS, 0, iApp, 1, &pPg, 0);
    assert( rc==LSM_OK || pPg==0 );

    /* If this is the first or last page of a block, fill in the pointer 
     ** value at the end of the new page. */
    if( rc==LSM_OK ){
      p->nSize++;
      p->iLastPg = iApp;
      if( p->iFirst==0 ) p->iFirst = iApp;
      pPg->flags |= PAGE_DIRTY;

      if( fsIsLast(pFS, iApp) ){
        lsmPutU32(&pPg->aData[pFS->nPagesize-4], fsPageToBlock(pFS, iNext));
      }else if( fsIsFirst(pFS, iApp) ){
        lsmPutU32(&pPg->aData[-4], fsPageToBlock(pFS, iPrev));
      }
    }
  }

  *ppOut = pPg;
  return rc;
}

/*
** Mark the segment passed as the second argument as finished. Once a segment
** is marked as finished it is not possible to append any further pages to 
** it.
**
** Return LSM_OK if successful or an lsm error code if an error occurs.
*/
static int lsmFsSortedFinish(FileSystem *pFS, Segment *p){
  int rc = LSM_OK;
  if( p && p->iLastPg ){
    assert( p->pRedirect==0 );

    /* Check if the last page of this run happens to be the last of a block.
    ** If it is, then an extra block has already been allocated for this run.
    ** Shift this extra block back to the free-block list. 
    **
    ** Otherwise, add the first free page in the last block used by the run
    ** to the lAppend list.
    */
    if( fsLastPageOnPagesBlock(pFS, p->iLastPg)!=p->iLastPg ){
      int i;
      LsmPgno *aiAppend = pFS->pDb->pWorker->aiAppend;
      for(i=0; i<LSM_APPLIST_SZ; i++){
        if( aiAppend[i]==0 ){
          aiAppend[i] = p->iLastPg+1;
          break;
        }
      }
    }else if( pFS->pCompress==0 ){
      Page *pLast;
      rc = fsPageGet(pFS, 0, p->iLastPg, 0, &pLast, 0);
      if( rc==LSM_OK ){
        int iBlk = (int)lsmGetU32(&pLast->aData[pFS->nPagesize-4]);
        lsmBlockRefree(pFS->pDb, iBlk);
        lsmFsPageRelease(pLast);
      }
    }else{
      int iBlk = 0;
      rc = fsBlockNext(pFS, p, fsPageToBlock(pFS, p->iLastPg), &iBlk);
      if( rc==LSM_OK ){
        lsmBlockRefree(pFS->pDb, iBlk);
      }
    }
  }
  return rc;
}

/*
** Obtain a reference to page number iPg.
**
** Return LSM_OK if successful, or an lsm error code if an error occurs.
*/
static int lsmFsDbPageGet(FileSystem *pFS, Segment *pSeg, LsmPgno iPg, Page **ppPg){
  return fsPageGet(pFS, pSeg, iPg, 0, ppPg, 0);
}

/*
** Obtain a reference to the last page in the segment passed as the 
** second argument.
**
** Return LSM_OK if successful, or an lsm error code if an error occurs.
*/
static int lsmFsDbPageLast(FileSystem *pFS, Segment *pSeg, Page **ppPg){
  int rc;
  LsmPgno iPg = pSeg->iLastPg;
  if( pFS->pCompress ){
    int nSpace;
    iPg++;
    do {
      nSpace = 0;
      rc = fsGetPageBefore(pFS, pSeg, iPg, &iPg);
      if( rc==LSM_OK ){
        rc = fsPageGet(pFS, pSeg, iPg, 0, ppPg, &nSpace);
      }
    }while( rc==LSM_OK && nSpace>0 );

  }else{
    rc = fsPageGet(pFS, pSeg, iPg, 0, ppPg, 0);
  }
  return rc;
}

/*
** Return a reference to meta-page iPg. If successful, LSM_OK is returned
** and *ppPg populated with the new page reference. The reference should
** be released by the caller using lsmFsPageRelease().
**
** Otherwise, if an error occurs, *ppPg is set to NULL and an LSM error 
** code is returned.
*/
static int lsmFsMetaPageGet(
  FileSystem *pFS,                /* File-system connection */
  int bWrite,                     /* True for write access, false for read */
  int iPg,                        /* Either 1 or 2 */
  MetaPage **ppPg                 /* OUT: Pointer to MetaPage object */
){
  int rc = LSM_OK;
  MetaPage *pPg;
  assert( iPg==1 || iPg==2 );

  pPg = lsmMallocZeroRc(pFS->pEnv, sizeof(Page), &rc);

  if( pPg ){
    i64 iOff = (iPg-1) * pFS->nMetasize;
    if( pFS->nMapLimit>0 ){
      fsGrowMapping(pFS, 2*pFS->nMetasize, &rc);
      pPg->aData = (u8 *)(pFS->pMap) + iOff;
    }else{
      pPg->aData = lsmMallocRc(pFS->pEnv, pFS->nMetasize, &rc);
      if( rc==LSM_OK && bWrite==0 ){
        rc = lsmEnvRead(
            pFS->pEnv, pFS->fdDb, iOff, pPg->aData, pFS->nMetaRwSize
        );
      }
#ifndef NDEBUG
      /* pPg->aData causes an uninitialized access via a downstreadm write().
         After discussion on this list, this memory should not, for performance
         reasons, be memset. However, tracking down "real" misuse is more
         difficult with this "false" positive, so it is set when NDEBUG.
      */
      else if( rc==LSM_OK ){
        memset( pPg->aData, 0x77, pFS->nMetasize );
      }
#endif
    }

    if( rc!=LSM_OK ){
      if( pFS->nMapLimit==0 ) lsmFree(pFS->pEnv, pPg->aData);
      lsmFree(pFS->pEnv, pPg);
      pPg = 0;
    }else{
      pPg->iPg = iPg;
      pPg->bWrite = bWrite;
      pPg->pFS = pFS;
    }
  }

  *ppPg = pPg;
  return rc;
}

/*
** Release a meta-page reference obtained via a call to lsmFsMetaPageGet().
*/
static int lsmFsMetaPageRelease(MetaPage *pPg){
  int rc = LSM_OK;
  if( pPg ){
    FileSystem *pFS = pPg->pFS;

    if( pFS->nMapLimit==0 ){
      if( pPg->bWrite ){
        i64 iOff = (pPg->iPg==2 ? pFS->nMetasize : 0);
        int nWrite = pFS->nMetaRwSize;
        rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iOff, pPg->aData, nWrite);
      }
      lsmFree(pFS->pEnv, pPg->aData);
    }

    lsmFree(pFS->pEnv, pPg);
  }
  return rc;
}

/*
** Return a pointer to a buffer containing the data associated with the
** meta-page passed as the first argument. If parameter pnData is not NULL,
** set *pnData to the size of the meta-page in bytes before returning.
*/
static u8 *lsmFsMetaPageData(MetaPage *pPg, int *pnData){
  if( pnData ) *pnData = pPg->pFS->nMetaRwSize;
  return pPg->aData;
}

/*
** Return true if page is currently writable. This is used in assert() 
** statements only.
*/
#ifndef NDEBUG
static int lsmFsPageWritable(Page *pPg){
  return (pPg->flags & PAGE_DIRTY) ? 1 : 0;
}
#endif

/*
** This is called when block iFrom is being redirected to iTo. If page 
** number (*piPg) lies on block iFrom, then calculate the equivalent
** page on block iTo and set *piPg to this value before returning.
*/
static void fsMovePage(
  FileSystem *pFS,                /* File system object */
  int iTo,                        /* Destination block */
  int iFrom,                      /* Source block */
  LsmPgno *piPg                   /* IN/OUT: Page number */
){
  LsmPgno iPg = *piPg;
  if( iFrom==fsPageToBlock(pFS, iPg) ){
    const int nPagePerBlock = (
        pFS->pCompress ? pFS ->nBlocksize : (pFS->nBlocksize / pFS->nPagesize)
    );
    *piPg = iPg - (LsmPgno)(iFrom - iTo) * nPagePerBlock;
  }
}

/*
** Copy the contents of block iFrom to block iTo. 
**
** It is safe to assume that there are no outstanding references to pages 
** on block iTo. And that block iFrom is not currently being written. In
** other words, the data can be read and written directly.
*/
static int lsmFsMoveBlock(FileSystem *pFS, Segment *pSeg, int iTo, int iFrom){
  Snapshot *p = pFS->pDb->pWorker;
  int rc = LSM_OK;
  int i;
  i64 nMap;

  i64 iFromOff = (i64)(iFrom-1) * pFS->nBlocksize;
  i64 iToOff = (i64)(iTo-1) * pFS->nBlocksize;
  
  assert( iTo!=1 );
  assert( iFrom>iTo );

  /* Grow the mapping as required. */
  nMap = LSM_MIN(pFS->nMapLimit, (i64)iFrom * pFS->nBlocksize);
  fsGrowMapping(pFS, nMap, &rc);

  if( rc==LSM_OK ){
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    int nSz = pFS->nPagesize;
    u8 *aBuf = 0;
    u8 *aData = 0;

    for(i=0; rc==LSM_OK && i<nPagePerBlock; i++){
      i64 iOff = iFromOff + i*nSz;

      /* Set aData to point to a buffer containing the from page */
      if( (iOff+nSz)<=pFS->nMapLimit ){
        u8 *aMap = (u8 *)(pFS->pMap);
        aData = &aMap[iOff];
      }else{
        if( aBuf==0 ){
          aBuf = (u8 *)lsmMallocRc(pFS->pEnv, nSz, &rc);
          if( aBuf==0 ) break;
        }
        aData = aBuf;
        rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, aData, nSz);
      }

      /* Copy aData to the to page */
      if( rc==LSM_OK ){
        iOff = iToOff + i*nSz;
        if( (iOff+nSz)<=pFS->nMapLimit ){
          u8 *aMap = (u8 *)(pFS->pMap);
          memcpy(&aMap[iOff], aData, nSz);
        }else{
          rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iOff, aData, nSz);
        }
      }
    }
    lsmFree(pFS->pEnv, aBuf);
    lsmFsPurgeCache(pFS);
  }

  /* Update append-point list if necessary */
  for(i=0; i<LSM_APPLIST_SZ; i++){
    fsMovePage(pFS, iTo, iFrom, &p->aiAppend[i]);
  }

  /* Update the Segment structure itself */
  fsMovePage(pFS, iTo, iFrom, &pSeg->iFirst);
  fsMovePage(pFS, iTo, iFrom, &pSeg->iLastPg);
  fsMovePage(pFS, iTo, iFrom, &pSeg->iRoot);

  return rc;
}

/*
** Append raw data to a segment. Return the database file offset that the
** data is written to (this may be used as the page number if the data
** being appended is a new page record).
**
** This function is only used in compressed database mode.
*/
static LsmPgno fsAppendData(
  FileSystem *pFS,                /* File-system handle */
  Segment *pSeg,                  /* Segment to append to */
  const u8 *aData,                /* Buffer containing data to write */
  int nData,                      /* Size of buffer aData[] in bytes */
  int *pRc                        /* IN/OUT: Error code */
){
  LsmPgno iRet = 0;
  int rc = *pRc;
  assert( pFS->pCompress );
  if( rc==LSM_OK ){
    int nRem = 0;
    int nWrite = 0;
    LsmPgno iLastOnBlock;
    LsmPgno iApp = pSeg->iLastPg+1;

    /* If this is the first data written into the segment, find an append-point
    ** or allocate a new block.  */
    if( iApp==1 ){
      pSeg->iFirst = iApp = findAppendPoint(pFS, 0);
      if( iApp==0 ){
        int iBlk;
        rc = lsmBlockAllocate(pFS->pDb, 0, &iBlk);
        pSeg->iFirst = iApp = fsFirstPageOnBlock(pFS, iBlk);
      }
    }
    iRet = iApp;

    /* Write as much data as is possible at iApp (usually all of it). */
    iLastOnBlock = fsLastPageOnPagesBlock(pFS, iApp);
    if( rc==LSM_OK ){
      int nSpace = (int)(iLastOnBlock - iApp + 1);
      nWrite = LSM_MIN(nData, nSpace);
      nRem = nData - nWrite;
      assert( nWrite>=0 );
      if( nWrite!=0 ){
        rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iApp, aData, nWrite);
      }
      iApp += nWrite;
    }

    /* If required, allocate a new block and write the rest of the data
    ** into it. Set the next and previous block pointers to link the new
    ** block to the old.  */
    assert( nRem<=0 || (iApp-1)==iLastOnBlock );
    if( rc==LSM_OK && (iApp-1)==iLastOnBlock ){
      u8 aPtr[4];                 /* Space to serialize a u32 */
      int iBlk;                   /* New block number */

      if( nWrite>0 ){
        /* Allocate a new block. */
        rc = lsmBlockAllocate(pFS->pDb, 0, &iBlk);

        /* Set the "next" pointer on the old block */
        if( rc==LSM_OK ){
          assert( iApp==(fsPageToBlock(pFS, iApp)*pFS->nBlocksize)-4 );
          lsmPutU32(aPtr, iBlk);
          rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iApp, aPtr, sizeof(aPtr));
        }

        /* Set the "prev" pointer on the new block */
        if( rc==LSM_OK ){
          LsmPgno iWrite;
          lsmPutU32(aPtr, fsPageToBlock(pFS, iApp));
          iWrite = fsFirstPageOnBlock(pFS, iBlk);
          rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iWrite-4, aPtr, sizeof(aPtr));
          if( nRem>0 ) iApp = iWrite;
        }
      }else{
        /* The next block is already allocated. */
        assert( nRem>0 );
        assert( pSeg->pRedirect==0 );
        rc = fsBlockNext(pFS, 0, fsPageToBlock(pFS, iApp), &iBlk);
        iRet = iApp = fsFirstPageOnBlock(pFS, iBlk);
      }

      /* Write the remaining data into the new block */
      if( rc==LSM_OK && nRem>0 ){
        rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iApp, &aData[nWrite], nRem);
        iApp += nRem;
      }
    }

    pSeg->iLastPg = iApp-1;
    *pRc = rc;
  }

  return iRet;
}

/*
** This function is only called in compressed database mode. It 
** compresses the contents of page pPg and writes the result to the 
** buffer at pFS->aOBuffer. The size of the compressed data is stored in
** pPg->nCompress.
**
** If buffer pFS->aOBuffer[] has not been allocated then this function
** allocates it. If this fails, LSM_NOMEM is returned. Otherwise, LSM_OK.
*/
static int fsCompressIntoBuffer(FileSystem *pFS, Page *pPg){
  lsm_compress *p = pFS->pCompress;

  if( fsAllocateBuffer(pFS, 1) ) return LSM_NOMEM;
  assert( pPg->nData==pFS->nPagesize );

  pPg->nCompress = pFS->nBuffer;
  return p->xCompress(p->pCtx, 
      (char *)pFS->aOBuffer, &pPg->nCompress, 
      (const char *)pPg->aData, pPg->nData
  );
}

/*
** Append a new page to segment pSeg. Set output variable *piNew to the
** page number of the new page before returning.
**
** If the new page is the last on its block, then the 'next' block that
** will be used by the segment is allocated here too. In this case output
** variable *piNext is set to the block number of the next block.
**
** If the new page is the first on its block but not the first in the
** entire segment, set output variable *piPrev to the block number of
** the previous block in the segment.
**
** LSM_OK is returned if successful, or an lsm error code otherwise. If
** any value other than LSM_OK is returned, then the final value of all
** output variables is undefined.
*/
static int fsAppendPage(
  FileSystem *pFS, 
  Segment *pSeg,
  LsmPgno *piNew,
  int *piPrev,
  int *piNext
){
  LsmPgno iPrev = pSeg->iLastPg;
  int rc;
  assert( iPrev!=0 );

  *piPrev = 0;
  *piNext = 0;

  if( fsIsLast(pFS, iPrev) ){
    /* Grab the first page on the next block (which has already be
    ** allocated). In this case set *piPrev to tell the caller to set
    ** the "previous block" pointer in the first 4 bytes of the page.
    */
    int iNext;
    int iBlk = fsPageToBlock(pFS, iPrev);
    assert( pSeg->pRedirect==0 );
    rc = fsBlockNext(pFS, 0, iBlk, &iNext);
    if( rc!=LSM_OK ) return rc;
    *piNew = fsFirstPageOnBlock(pFS, iNext);
    *piPrev = iBlk;
  }else{
    *piNew = iPrev+1;
    if( fsIsLast(pFS, *piNew) ){
      /* Allocate the next block here. */
      int iBlk;
      rc = lsmBlockAllocate(pFS->pDb, 0, &iBlk);
      if( rc!=LSM_OK ) return rc;
      *piNext = iBlk;
    }
  }

  pSeg->nSize++;
  pSeg->iLastPg = *piNew;
  return LSM_OK;
}

/*
** Flush all pages in the FileSystem.pWaiting list to disk.
*/
static void lsmFsFlushWaiting(FileSystem *pFS, int *pRc){
  int rc = *pRc;
  Page *pPg;

  pPg = pFS->pWaiting;
  pFS->pWaiting = 0;

  while( pPg ){
    Page *pNext = pPg->pWaitingNext;
    if( rc==LSM_OK ) rc = lsmFsPagePersist(pPg);
    assert( pPg->nRef==1 );
    lsmFsPageRelease(pPg);
    pPg = pNext;
  }
  *pRc = rc;
}

/*
** If there exists a hash-table entry associated with page iPg, remove it.
*/
static void fsRemoveHashEntry(FileSystem *pFS, LsmPgno iPg){
  Page *p;
  int iHash = fsHashKey(pFS->nHash, iPg);

  for(p=pFS->apHash[iHash]; p && p->iPg!=iPg; p=p->pHashNext);

  if( p ){
    assert( p->nRef==0 || (p->flags & PAGE_FREE)==0 );
    fsPageRemoveFromHash(pFS, p);
    p->iPg = 0;
    iHash = fsHashKey(pFS->nHash, 0);
    p->pHashNext = pFS->apHash[iHash];
    pFS->apHash[iHash] = p;
  }
}

/*
** If the page passed as an argument is dirty, update the database file
** (or mapping of the database file) with its current contents and mark
** the page as clean.
**
** Return LSM_OK if the operation is a success, or an LSM error code
** otherwise.
*/
static int lsmFsPagePersist(Page *pPg){
  int rc = LSM_OK;
  if( pPg && (pPg->flags & PAGE_DIRTY) ){
    FileSystem *pFS = pPg->pFS;

    if( pFS->pCompress ){
      int iHash;                  /* Hash key of assigned page number */
      u8 aSz[3];                  /* pPg->nCompress as a 24-bit big-endian */
      assert( pPg->pSeg && pPg->iPg==0 && pPg->nCompress==0 );

      /* Compress the page image. */
      rc = fsCompressIntoBuffer(pFS, pPg);

      /* Serialize the compressed size into buffer aSz[] */
      putRecordSize(aSz, pPg->nCompress, 0);

      /* Write the serialized page record into the database file. */
      pPg->iPg = fsAppendData(pFS, pPg->pSeg, aSz, sizeof(aSz), &rc);
      fsAppendData(pFS, pPg->pSeg, pFS->aOBuffer, pPg->nCompress, &rc);
      fsAppendData(pFS, pPg->pSeg, aSz, sizeof(aSz), &rc);

      /* Now that it has a page number, insert the page into the hash table */
      iHash = fsHashKey(pFS->nHash, pPg->iPg);
      pPg->pHashNext = pFS->apHash[iHash];
      pFS->apHash[iHash] = pPg;

      pPg->pSeg->nSize += (sizeof(aSz) * 2) + pPg->nCompress;

      pPg->flags &= ~PAGE_DIRTY;
      pFS->nWrite++;
    }else{

      if( pPg->iPg==0 ){
        /* No page number has been assigned yet. This occurs with pages used
        ** in the b-tree hierarchy. They were not assigned page numbers when
        ** they were created as doing so would cause this call to
        ** lsmFsPagePersist() to write an out-of-order page. Instead a page 
        ** number is assigned here so that the page data will be appended
        ** to the current segment.
        */
        Page **pp;
        int iPrev = 0;
        int iNext = 0;
        int iHash;

        assert( pPg->pSeg->iFirst );
        assert( pPg->flags & PAGE_FREE );
        assert( (pPg->flags & PAGE_HASPREV)==0 );
        assert( pPg->nData==pFS->nPagesize-4 );

        rc = fsAppendPage(pFS, pPg->pSeg, &pPg->iPg, &iPrev, &iNext);
        if( rc!=LSM_OK ) return rc;

        assert( pPg->flags & PAGE_FREE );
        iHash = fsHashKey(pFS->nHash, pPg->iPg);
        fsRemoveHashEntry(pFS, pPg->iPg);
        pPg->pHashNext = pFS->apHash[iHash];
        pFS->apHash[iHash] = pPg;
        assert( pPg->pHashNext==0 || pPg->pHashNext->iPg!=pPg->iPg );

        if( iPrev ){
          assert( iNext==0 );
          memmove(&pPg->aData[4], pPg->aData, pPg->nData);
          lsmPutU32(pPg->aData, iPrev);
          pPg->flags |= PAGE_HASPREV;
          pPg->aData += 4;
        }else if( iNext ){
          assert( iPrev==0 );
          lsmPutU32(&pPg->aData[pPg->nData], iNext);
        }else{
          int nData = pPg->nData;
          pPg->nData += 4;
          lsmSortedExpandBtreePage(pPg, nData);
        }

        pPg->nRef++;
        for(pp=&pFS->pWaiting; *pp; pp=&(*pp)->pWaitingNext);
        *pp = pPg;
        assert( pPg->pWaitingNext==0 );

      }else{
        i64 iOff;                   /* Offset to write within database file */

        iOff = (i64)pFS->nPagesize * (i64)(pPg->iPg-1);
        if( fsMmapPage(pFS, pPg->iPg)==0 ){
          u8 *aData = pPg->aData - (pPg->flags & PAGE_HASPREV);
          rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iOff, aData, pFS->nPagesize);
        }else if( pPg->flags & PAGE_FREE ){
          fsGrowMapping(pFS, iOff + pFS->nPagesize, &rc);
          if( rc==LSM_OK ){
            u8 *aTo = &((u8 *)(pFS->pMap))[iOff];
            u8 *aFrom = pPg->aData - (pPg->flags & PAGE_HASPREV);
            memcpy(aTo, aFrom, pFS->nPagesize);
            lsmFree(pFS->pEnv, aFrom);
            pFS->nCacheAlloc--;
            pPg->aData = aTo + (pPg->flags & PAGE_HASPREV);
            pPg->flags &= ~PAGE_FREE;
            fsPageRemoveFromHash(pFS, pPg);
            pPg->pMappedNext = pFS->pMapped;
            pFS->pMapped = pPg;
          }
        }

        lsmFsFlushWaiting(pFS, &rc);
        pPg->flags &= ~PAGE_DIRTY;
        pFS->nWrite++;
      }
    }
  }

  return rc;
}

/*
** For non-compressed databases, this function is a no-op. For compressed
** databases, it adds a padding record to the segment passed as the third
** argument.
**
** The size of the padding records is selected so that the last byte 
** written is the last byte of a disk sector. This means that if a 
** snapshot is taken and checkpointed, subsequent worker processes will
** not write to any sector that contains checkpointed data.
*/
static int lsmFsSortedPadding(
  FileSystem *pFS, 
  Snapshot *pSnapshot,
  Segment *pSeg
){
  int rc = LSM_OK;
  if( pFS->pCompress && pSeg->iFirst ){
    LsmPgno iLast2;
    LsmPgno iLast = pSeg->iLastPg;  /* Current last page of segment */
    int nPad;                       /* Bytes of padding required */
    u8 aSz[3];

    iLast2 = (1 + iLast/pFS->szSector) * pFS->szSector - 1;
    assert( fsPageToBlock(pFS, iLast)==fsPageToBlock(pFS, iLast2) );
    nPad = (int)(iLast2 - iLast);

    if( iLast2>fsLastPageOnPagesBlock(pFS, iLast) ){
      nPad -= 4;
    }
    assert( nPad>=0 );

    if( nPad>=6 ){
      pSeg->nSize += nPad;
      nPad -= 6;
      putRecordSize(aSz, nPad, 1);
      fsAppendData(pFS, pSeg, aSz, sizeof(aSz), &rc);
      memset(pFS->aOBuffer, 0, nPad);
      fsAppendData(pFS, pSeg, pFS->aOBuffer, nPad, &rc);
      fsAppendData(pFS, pSeg, aSz, sizeof(aSz), &rc);
    }else if( nPad>0 ){
      u8 aBuf[5] = {0,0,0,0,0};
      aBuf[0] = (u8)nPad;
      aBuf[nPad-1] = (u8)nPad;
      fsAppendData(pFS, pSeg, aBuf, nPad, &rc);
    }

    assert( rc!=LSM_OK 
        || pSeg->iLastPg==fsLastPageOnPagesBlock(pFS, pSeg->iLastPg)
        || ((pSeg->iLastPg + 1) % pFS->szSector)==0
    );
  }

  return rc;
}


/*
** Increment the reference count on the page object passed as the first
** argument.
*/
static void lsmFsPageRef(Page *pPg){
  if( pPg ){
    pPg->nRef++;
  }
}

/*
** Release a page-reference obtained using fsPageGet().
*/
static int lsmFsPageRelease(Page *pPg){
  int rc = LSM_OK;
  if( pPg ){
    assert( pPg->nRef>0 );
    pPg->nRef--;
    if( pPg->nRef==0 ){
      FileSystem *pFS = pPg->pFS;
      rc = lsmFsPagePersist(pPg);
      pFS->nOut--;

      assert( pPg->pFS->pCompress 
           || fsIsFirst(pPg->pFS, pPg->iPg)==0 
           || (pPg->flags & PAGE_HASPREV)
      );
      pPg->aData -= (pPg->flags & PAGE_HASPREV);
      pPg->flags &= ~PAGE_HASPREV;

      if( (pPg->flags & PAGE_FREE)==0 ){
        /* Removed from mapped list */
        Page **pp;
        for(pp=&pFS->pMapped; (*pp)!=pPg; pp=&(*pp)->pMappedNext);
        *pp = pPg->pMappedNext;
        pPg->pMappedNext = 0;

        /* Add to free list */
        pPg->pFreeNext = pFS->pFree;
        pFS->pFree = pPg;
      }else{
        fsPageAddToLru(pFS, pPg);
      }
    }
  }

  return rc;
}

/*
** Return the total number of pages read from the database file.
*/
static int lsmFsNRead(FileSystem *pFS){ return pFS->nRead; }

/*
** Return the total number of pages written to the database file.
*/
static int lsmFsNWrite(FileSystem *pFS){ return pFS->nWrite; }

/*
** Return a copy of the environment pointer used by the file-system object.
*/
lsm_env *lsmFsEnv(FileSystem *pFS){ 
  return pFS->pEnv; 
}

/*
** Return a copy of the environment pointer used by the file-system object
** to which this page belongs.
*/
lsm_env *lsmPageEnv(Page *pPg) { 
  return pPg->pFS->pEnv; 
}

/*
** Return a pointer to the file-system object associated with the Page
** passed as the only argument.
*/
static FileSystem *lsmPageFS(Page *pPg){
  return pPg->pFS;
}

/*
** Return the sector-size as reported by the log file handle.
*/
static int lsmFsSectorSize(FileSystem *pFS){
  return pFS->szSector;
}

/*
** Helper function for lsmInfoArrayStructure().
*/
static Segment *startsWith(Segment *pRun, LsmPgno iFirst){
  return (iFirst==pRun->iFirst) ? pRun : 0;
}

/*
** Return the segment that starts with page iFirst, if any. If no such segment
** can be found, return NULL.
*/
static Segment *findSegment(Snapshot *pWorker, LsmPgno iFirst){
  Level *pLvl;                    /* Used to iterate through db levels */
  Segment *pSeg = 0;              /* Pointer to segment to return */

  for(pLvl=lsmDbSnapshotLevel(pWorker); pLvl && pSeg==0; pLvl=pLvl->pNext){
    if( 0==(pSeg = startsWith(&pLvl->lhs, iFirst)) ){
      int i;
      for(i=0; i<pLvl->nRight; i++){
        if( (pSeg = startsWith(&pLvl->aRhs[i], iFirst)) ) break;
      }
    }
  }

  return pSeg;
}

/*
** This function implements the lsm_info(LSM_INFO_ARRAY_STRUCTURE) request.
** If successful, *pzOut is set to point to a nul-terminated string 
** containing the array structure and LSM_OK is returned. The caller should
** eventually free the string using lsmFree().
**
** If an error occurs, *pzOut is set to NULL and an LSM error code returned.
*/
static int lsmInfoArrayStructure(
  lsm_db *pDb, 
  int bBlock,                     /* True for block numbers only */
  LsmPgno iFirst,
  char **pzOut
){
  int rc = LSM_OK;
  Snapshot *pWorker;              /* Worker snapshot */
  Segment *pArray = 0;            /* Array to report on */
  int bUnlock = 0;

  *pzOut = 0;
  if( iFirst==0 ) return LSM_ERROR;

  /* Obtain the worker snapshot */
  pWorker = pDb->pWorker;
  if( !pWorker ){
    rc = lsmBeginWork(pDb);
    if( rc!=LSM_OK ) return rc;
    pWorker = pDb->pWorker;
    bUnlock = 1;
  }

  /* Search for the array that starts on page iFirst */
  pArray = findSegment(pWorker, iFirst);

  if( pArray==0 ){
    /* Could not find the requested array. This is an error. */
    rc = LSM_ERROR;
  }else{
    FileSystem *pFS = pDb->pFS;
    LsmString str;
    int iBlk;
    int iLastBlk;
   
    iBlk = fsPageToBlock(pFS, pArray->iFirst);
    iLastBlk = fsPageToBlock(pFS, pArray->iLastPg);

    lsmStringInit(&str, pDb->pEnv);
    if( bBlock ){
      lsmStringAppendf(&str, "%d", iBlk);
      while( iBlk!=iLastBlk ){
        fsBlockNext(pFS, pArray, iBlk, &iBlk);
        lsmStringAppendf(&str, " %d", iBlk);
      }
    }else{
      lsmStringAppendf(&str, "%d", pArray->iFirst);
      while( iBlk!=iLastBlk ){
        lsmStringAppendf(&str, " %d", fsLastPageOnBlock(pFS, iBlk));
        fsBlockNext(pFS, pArray, iBlk, &iBlk);
        lsmStringAppendf(&str, " %d", fsFirstPageOnBlock(pFS, iBlk));
      }
      lsmStringAppendf(&str, " %d", pArray->iLastPg);
    }

    *pzOut = str.z;
  }

  if( bUnlock ){
    int rcwork = LSM_BUSY;
    lsmFinishWork(pDb, 0, &rcwork);
  }
  return rc;
}

static int lsmFsSegmentContainsPg(
  FileSystem *pFS, 
  Segment *pSeg, 
  LsmPgno iPg, 
  int *pbRes
){
  Redirect *pRedir = pSeg->pRedirect;
  int rc = LSM_OK;
  int iBlk;
  int iLastBlk;
  int iPgBlock;                   /* Block containing page iPg */

  iPgBlock = fsPageToBlock(pFS, pSeg->iFirst);
  iBlk = fsRedirectBlock(pRedir, fsPageToBlock(pFS, pSeg->iFirst));
  iLastBlk = fsRedirectBlock(pRedir, fsPageToBlock(pFS, pSeg->iLastPg));

  while( iBlk!=iLastBlk && iBlk!=iPgBlock && rc==LSM_OK ){
    rc = fsBlockNext(pFS, pSeg, iBlk, &iBlk);
  }

  *pbRes = (iBlk==iPgBlock);
  return rc;
}

/*
** This function implements the lsm_info(LSM_INFO_ARRAY_PAGES) request.
** If successful, *pzOut is set to point to a nul-terminated string 
** containing the array structure and LSM_OK is returned. The caller should
** eventually free the string using lsmFree().
**
** If an error occurs, *pzOut is set to NULL and an LSM error code returned.
*/
static int lsmInfoArrayPages(lsm_db *pDb, LsmPgno iFirst, char **pzOut){
  int rc = LSM_OK;
  Snapshot *pWorker;              /* Worker snapshot */
  Segment *pSeg = 0;              /* Array to report on */
  int bUnlock = 0;

  *pzOut = 0;
  if( iFirst==0 ) return LSM_ERROR;

  /* Obtain the worker snapshot */
  pWorker = pDb->pWorker;
  if( !pWorker ){
    rc = lsmBeginWork(pDb);
    if( rc!=LSM_OK ) return rc;
    pWorker = pDb->pWorker;
    bUnlock = 1;
  }

  /* Search for the array that starts on page iFirst */
  pSeg = findSegment(pWorker, iFirst);

  if( pSeg==0 ){
    /* Could not find the requested array. This is an error. */
    rc = LSM_ERROR;
  }else{
    Page *pPg = 0;
    FileSystem *pFS = pDb->pFS;
    LsmString str;

    lsmStringInit(&str, pDb->pEnv);
    rc = lsmFsDbPageGet(pFS, pSeg, iFirst, &pPg);
    while( rc==LSM_OK && pPg ){
      Page *pNext = 0;
      lsmStringAppendf(&str, " %lld", lsmFsPageNumber(pPg));
      rc = lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
      lsmFsPageRelease(pPg);
      pPg = pNext;
    }

    if( rc!=LSM_OK ){
      lsmFree(pDb->pEnv, str.z);
    }else{
      *pzOut = str.z;
    }
  }

  if( bUnlock ){
    int rcwork = LSM_BUSY;
    lsmFinishWork(pDb, 0, &rcwork);
  }
  return rc;
}

/*
** The following macros are used by the integrity-check code. Associated with
** each block in the database is an 8-bit bit mask (the entry in the aUsed[]
** array). As the integrity-check meanders through the database, it sets the
** following bits to indicate how each block is used.
**
** INTEGRITY_CHECK_FIRST_PG:
**   First page of block is in use by sorted run.
**
** INTEGRITY_CHECK_LAST_PG:
**   Last page of block is in use by sorted run.
**
** INTEGRITY_CHECK_USED:
**   At least one page of the block is in use by a sorted run.
**
** INTEGRITY_CHECK_FREE:
**   The free block list contains an entry corresponding to this block.
*/
#define INTEGRITY_CHECK_FIRST_PG 0x01
#define INTEGRITY_CHECK_LAST_PG  0x02
#define INTEGRITY_CHECK_USED     0x04
#define INTEGRITY_CHECK_FREE     0x08

/*
** Helper function for lsmFsIntegrityCheck()
*/
static void checkBlocks(
  FileSystem *pFS, 
  Segment *pSeg,
  int bExtra,                     /* If true, count the "next" block if any */
  int nUsed,
  u8 *aUsed
){
  if( pSeg ){
    if( pSeg && pSeg->nSize>0 ){
      int rc;
      int iBlk;                   /* Current block (during iteration) */
      int iLastBlk;               /* Last block of segment */
      int iFirstBlk;              /* First block of segment */
      int bLastIsLastOnBlock;     /* True iLast is the last on its block */

      assert( 0==fsSegmentRedirects(pFS, pSeg) );
      iBlk = iFirstBlk = fsPageToBlock(pFS, pSeg->iFirst);
      iLastBlk = fsPageToBlock(pFS, pSeg->iLastPg);

      bLastIsLastOnBlock = (fsLastPageOnBlock(pFS, iLastBlk)==pSeg->iLastPg);
      assert( iBlk>0 );

      do {
        /* iBlk is a part of this sorted run. */
        aUsed[iBlk-1] |= INTEGRITY_CHECK_USED;

        /* If the first page of this block is also part of the segment,
        ** set the flag to indicate that the first page of iBlk is in use.  
        */
        if( fsFirstPageOnBlock(pFS, iBlk)==pSeg->iFirst || iBlk!=iFirstBlk ){
          assert( (aUsed[iBlk-1] & INTEGRITY_CHECK_FIRST_PG)==0 );
          aUsed[iBlk-1] |= INTEGRITY_CHECK_FIRST_PG;
        }

        /* Unless the sorted run finishes before the last page on this block, 
        ** the last page of this block is also in use.  */
        if( iBlk!=iLastBlk || bLastIsLastOnBlock ){
          assert( (aUsed[iBlk-1] & INTEGRITY_CHECK_LAST_PG)==0 );
          aUsed[iBlk-1] |= INTEGRITY_CHECK_LAST_PG;
        }

        /* Special case. The sorted run being scanned is the output run of
        ** a level currently undergoing an incremental merge. The sorted
        ** run ends on the last page of iBlk, but the next block has already
        ** been allocated. So mark it as in use as well.  */
        if( iBlk==iLastBlk && bLastIsLastOnBlock && bExtra ){
          int iExtra = 0;
          rc = fsBlockNext(pFS, pSeg, iBlk, &iExtra);
          assert( rc==LSM_OK );

          assert( aUsed[iExtra-1]==0 );
          aUsed[iExtra-1] |= INTEGRITY_CHECK_USED;
          aUsed[iExtra-1] |= INTEGRITY_CHECK_FIRST_PG;
          aUsed[iExtra-1] |= INTEGRITY_CHECK_LAST_PG;
        }

        /* Move on to the next block in the sorted run. Or set iBlk to zero
        ** in order to break out of the loop if this was the last block in
        ** the run.  */
        if( iBlk==iLastBlk ){
          iBlk = 0;
        }else{
          rc = fsBlockNext(pFS, pSeg, iBlk, &iBlk);
          assert( rc==LSM_OK );
        }
      }while( iBlk );
    }
  }
}

typedef struct CheckFreelistCtx CheckFreelistCtx;
struct CheckFreelistCtx {
  u8 *aUsed;
  int nBlock;
};
static int checkFreelistCb(void *pCtx, int iBlk, i64 iSnapshot){
  CheckFreelistCtx *p = (CheckFreelistCtx *)pCtx;

  assert( iBlk>=1 );
  assert( iBlk<=p->nBlock );
  assert( p->aUsed[iBlk-1]==0 );
  p->aUsed[iBlk-1] = INTEGRITY_CHECK_FREE;
  return 0;
}

/*
** This function checks that all blocks in the database file are accounted
** for. For each block, exactly one of the following must be true:
**
**   + the block is part of a sorted run, or
**   + the block is on the free-block list
**
** This function also checks that there are no references to blocks with
** out-of-range block numbers.
**
** If no errors are found, non-zero is returned. If an error is found, an
** assert() fails.
*/
static int lsmFsIntegrityCheck(lsm_db *pDb){
  CheckFreelistCtx ctx;
  FileSystem *pFS = pDb->pFS;
  int i;
  int rc;
  Freelist freelist = {0, 0, 0};
  u8 *aUsed;
  Level *pLevel;
  Snapshot *pWorker = pDb->pWorker;
  int nBlock = pWorker->nBlock;

#if 0 
  static int nCall = 0;
  nCall++;
  printf("%d calls\n", nCall);
#endif

  aUsed = lsmMallocZero(pDb->pEnv, nBlock);
  if( aUsed==0 ){
    /* Malloc has failed. Since this function is only called within debug
    ** builds, this probably means the user is running an OOM injection test.
    ** Regardless, it will not be possible to run the integrity-check at this
    ** time, so assume the database is Ok and return non-zero. */
    return 1;
  }

  for(pLevel=pWorker->pLevel; pLevel; pLevel=pLevel->pNext){
    int j;
    checkBlocks(pFS, &pLevel->lhs, (pLevel->nRight!=0), nBlock, aUsed);
    for(j=0; j<pLevel->nRight; j++){
      checkBlocks(pFS, &pLevel->aRhs[j], 0, nBlock, aUsed);
    }
  }

  /* Mark all blocks in the free-list as used */
  ctx.aUsed = aUsed;
  ctx.nBlock = nBlock;
  rc = lsmWalkFreelist(pDb, 0, checkFreelistCb, (void *)&ctx);

  if( rc==LSM_OK ){
    for(i=0; i<nBlock; i++) assert( aUsed[i]!=0 );
  }

  lsmFree(pDb->pEnv, aUsed);
  lsmFree(pDb->pEnv, freelist.aEntry);

  return 1;
}

#ifndef NDEBUG
/*
** Return true if pPg happens to be the last page in segment pSeg. Or false
** otherwise. This function is only invoked as part of assert() conditions.
*/
static int lsmFsDbPageIsLast(Segment *pSeg, Page *pPg){
  if( pPg->pFS->pCompress ){
    LsmPgno iNext = 0;
    int rc;
    rc = fsNextPageOffset(pPg->pFS, pSeg, pPg->iPg, pPg->nCompress+6, &iNext);
    return (rc!=LSM_OK || iNext==0);
  }
  return (pPg->iPg==pSeg->iLastPg);
}
#endif

#line 1 "lsm_log.c"
/*
** 2011-08-13
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains the implementation of LSM database logging. Logging
** has one purpose in LSM - to make transactions durable.
**
** When data is written to an LSM database, it is initially stored in an
** in-memory tree structure. Since this structure is in volatile memory,
** if a power failure or application crash occurs it may be lost. To
** prevent loss of data in this case, each time a record is written to the
** in-memory tree an equivalent record is appended to the log on disk.
** If a power failure or application crash does occur, data can be recovered
** by reading the log.
**
** A log file consists of the following types of records representing data
** written into the database:
**
**   LOG_WRITE:  A key-value pair written to the database.
**   LOG_DELETE: A delete key issued to the database.
**   LOG_COMMIT: A transaction commit.
**
** And the following types of records for ancillary purposes..
**
**   LOG_EOF:    A record indicating the end of a log file.
**   LOG_PAD1:   A single byte padding record.
**   LOG_PAD2:   An N byte padding record (N>1).
**   LOG_JUMP:   A pointer to another offset within the log file.
**
** Each transaction written to the log contains one or more LOG_WRITE and/or
** LOG_DELETE records, followed by a LOG_COMMIT record. The LOG_COMMIT record
** contains an 8-byte checksum based on all previous data written to the
** log file.
**
** LOG CHECKSUMS & RECOVERY
**
**   Checksums are found in two types of log records: LOG_COMMIT and
**   LOG_CKSUM records. In order to recover content from a log, a client
**   reads each record from the start of the log, calculating a checksum as
**   it does. Each time a LOG_COMMIT or LOG_CKSUM is encountered, the 
**   recovery process verifies that the checksum stored in the log 
**   matches the calculated checksum. If it does not, the recovery process
**   can stop reading the log.
**
**   If a recovery process reads records (other than COMMIT or CKSUM) 
**   consisting of at least LSM_CKSUM_MAXDATA bytes, then the next record in
**   the log must be either a LOG_CKSUM or LOG_COMMIT record. If it is
**   not, the recovery process also stops reading the log.
**
**   To recover the log file, it must be read twice. The first time to 
**   determine the location of the last valid commit record. And the second
**   time to load data into the in-memory tree.
**
**   Todo: Surely there is a better way...
**
** LOG WRAPPING
**
**   If the log file were never deleted or wrapped, it would be possible to
**   read it from start to end each time is required recovery (i.e each time
**   the number of database clients changes from 0 to 1). Effectively reading
**   the entire history of the database each time. This would quickly become 
**   inefficient. Additionally, since the log file would grow without bound,
**   it wastes storage space.
**
**   Instead, part of each checkpoint written into the database file contains 
**   a log offset (and other information required to read the log starting at
**   at this offset) at which to begin recovery. Offset $O.
**
**   Once a checkpoint has been written and synced into the database file, it
**   is guaranteed that no recovery process will need to read any data before
**   offset $O of the log file. It is therefore safe to begin overwriting
**   any data that occurs before offset $O.
**
**   This implementation separates the log into three regions mapped into
**   the log file - regions 0, 1 and 2. During recovery, regions are read
**   in ascending order (i.e. 0, then 1, then 2). Each region is zero or
**   more bytes in size.
**
**     |---1---|..|--0--|.|--2--|....
**
**   New records are always appended to the end of region 2.
**
**   Initially (when it is empty), all three regions are zero bytes in size.
**   Each of them are located at the beginning of the file. As records are
**   added to the log, region 2 grows, so that the log consists of a zero
**   byte region 1, followed by a zero byte region 0, followed by an N byte
**   region 2. After one or more checkpoints have been written to disk, 
**   the start point of region 2 is moved to $O. For example:
**
**     A) ||.........|--2--|....
**   
**   (both regions 0 and 1 are 0 bytes in size at offset 0).
**
**   Eventually, the log wraps around to write new records into the start.
**   At this point, region 2 is renamed to region 0. Region 0 is renamed
**   to region 2. After appending a few records to the new region 2, the
**   log file looks like this:
**
**     B) ||--2--|...|--0--|....
**
**   (region 1 is still 0 bytes in size, located at offset 0).
**
**   Any checkpoints made at this point may reduce the size of region 0.
**   However, if they do not, and region 2 expands so that it is about to
**   overwrite the start of region 0, then region 2 is renamed to region 1,
**   and a new region 2 created at the end of the file following the existing
**   region 0.
**
**     C) |---1---|..|--0--|.|-2-|
**
**   In this state records are appended to region 2 until checkpoints have
**   contracted regions 0 AND 1 UNTil they are both zero bytes in size. They 
**   are then shifted to the start of the log file, leaving the system in 
**   the equivalent of state A above.
**
**   Alternatively, state B may transition directly to state A if the size
**   of region 0 is reduced to zero bytes before region 2 threatens to 
**   encroach upon it.
**
** LOG_PAD1 & LOG_PAD2 RECORDS
**
**   PAD1 and PAD2 records may appear in a log file at any point. They allow
**   a process writing the log file align the beginning of transactions with 
**   the beginning of disk sectors, which increases robustness.
**
** RECORD FORMATS:
**
**   LOG_EOF:    * A single 0x00 byte.
**
**   LOG_PAD1:   * A single 0x01 byte.
**
**   LOG_PAD2:   * A single 0x02 byte, followed by
**               * The number of unused bytes (N) as a varint,
**               * An N byte block of unused space.
**
**   LOG_COMMIT: * A single 0x03 byte.
**               * An 8-byte checksum.
**
**   LOG_JUMP:   * A single 0x04 byte.
**               * Absolute file offset to jump to, encoded as a varint.
**
**   LOG_WRITE:  * A single 0x06 or 0x07 byte, 
**               * The number of bytes in the key, encoded as a varint, 
**               * The number of bytes in the value, encoded as a varint, 
**               * If the first byte was 0x07, an 8 byte checksum.
**               * The key data,
**               * The value data.
**
**   LOG_DELETE: * A single 0x08 or 0x09 byte, 
**               * The number of bytes in the key, encoded as a varint, 
**               * If the first byte was 0x09, an 8 byte checksum.
**               * The key data.
**
**   Varints are as described in lsm_varint.c (SQLite 4 format).
**
** CHECKSUMS:
**
**   The checksum is calculated using two 32-bit unsigned integers, s0 and
**   s1. The initial value for both is 42. It is updated each time a record
**   is written into the log file by treating the encoded (binary) record as 
**   an array of 32-bit little-endian integers. Then, if x[] is the integer
**   array, updating the checksum accumulators as follows:
**
**     for i from 0 to n-1 step 2:
**       s0 += x[i] + s1;
**       s1 += x[i+1] + s0;
**     endfor
**
**   If the record is not an even multiple of 8-bytes in size it is padded
**   with zeroes to make it so before the checksum is updated.
**
**   The checksum stored in a COMMIT, WRITE or DELETE is based on all bytes
**   up to the start of the 8-byte checksum itself, including the COMMIT,
**   WRITE or DELETE fields that appear before the checksum in the record.
**
** VARINT FORMAT
**
** See lsm_varint.c.
*/

#ifndef _LSM_INT_H
/* # include "lsmInt.h" */
#endif

/* Log record types */
#define LSM_LOG_EOF          0x00
#define LSM_LOG_PAD1         0x01
#define LSM_LOG_PAD2         0x02
#define LSM_LOG_COMMIT       0x03
#define LSM_LOG_JUMP         0x04

#define LSM_LOG_WRITE        0x06
#define LSM_LOG_WRITE_CKSUM  0x07

#define LSM_LOG_DELETE       0x08
#define LSM_LOG_DELETE_CKSUM 0x09

#define LSM_LOG_DRANGE       0x0A
#define LSM_LOG_DRANGE_CKSUM 0x0B

/* Require a checksum every 32KB. */
#define LSM_CKSUM_MAXDATA (32*1024)

/* Do not wrap a log file smaller than this in bytes. */
#define LSM_MIN_LOGWRAP      (128*1024)

/*
** szSector:
**   Commit records must be aligned to end on szSector boundaries. If
**   the safety-mode is set to NORMAL or OFF, this value is 1. Otherwise,
**   if the safety-mode is set to FULL, it is the size of the file-system
**   sectors as reported by lsmFsSectorSize().
*/
struct LogWriter {
  u32 cksum0;                     /* Checksum 0 at offset iOff */
  u32 cksum1;                     /* Checksum 1 at offset iOff */
  int iCksumBuf;                  /* Bytes of buf that have been checksummed */
  i64 iOff;                       /* Offset at start of buffer buf */
  int szSector;                   /* Sector size for this transaction */
  LogRegion jump;                 /* Avoid writing to this region */
  i64 iRegion1End;                /* End of first region written by trans */
  i64 iRegion2Start;              /* Start of second regions written by trans */
  LsmString buf;                  /* Buffer containing data not yet written */
};

/*
** Return the result of interpreting the first 4 bytes in buffer aIn as 
** a 32-bit unsigned little-endian integer.
*/
static u32 getU32le(u8 *aIn){
  return ((u32)aIn[3] << 24) 
       + ((u32)aIn[2] << 16) 
       + ((u32)aIn[1] << 8) 
       + ((u32)aIn[0]);
}


/*
** This function is the same as logCksum(), except that pointer "a" need
** not be aligned to an 8-byte boundary or padded with zero bytes. This
** version is slower, but sometimes more convenient to use.
*/
static void logCksumUnaligned(
  char *z,                        /* Input buffer */
  int n,                          /* Size of input buffer in bytes */
  u32 *pCksum0,                   /* IN/OUT: Checksum value 1 */
  u32 *pCksum1                    /* IN/OUT: Checksum value 2 */
){
  u8 *a = (u8 *)z;
  u32 cksum0 = *pCksum0;
  u32 cksum1 = *pCksum1;
  int nIn = (n/8) * 8;
  int i;

  assert( n>0 );
  for(i=0; i<nIn; i+=8){
    cksum0 += getU32le(&a[i]) + cksum1;
    cksum1 += getU32le(&a[i+4]) + cksum0;
  }

  if( nIn!=n ){
    u8 aBuf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    assert( (n-nIn)<8 && n>nIn );
    memcpy(aBuf, &a[nIn], n-nIn);
    cksum0 += getU32le(aBuf) + cksum1;
    cksum1 += getU32le(&aBuf[4]) + cksum0;
  }

  *pCksum0 = cksum0;
  *pCksum1 = cksum1;
}

/*
** Update pLog->cksum0 and pLog->cksum1 so that the first nBuf bytes in the 
** write buffer (pLog->buf) are included in the checksum.
*/
static void logUpdateCksum(LogWriter *pLog, int nBuf){
  assert( (pLog->iCksumBuf % 8)==0 );
  assert( pLog->iCksumBuf<=nBuf );
  assert( (nBuf % 8)==0 || nBuf==pLog->buf.n );
  if( nBuf>pLog->iCksumBuf ){
    logCksumUnaligned(
        &pLog->buf.z[pLog->iCksumBuf], nBuf-pLog->iCksumBuf, 
        &pLog->cksum0, &pLog->cksum1
    );
  }
  pLog->iCksumBuf = nBuf;
}

static i64 firstByteOnSector(LogWriter *pLog, i64 iOff){
  return (iOff / pLog->szSector) * pLog->szSector;
}
static i64 lastByteOnSector(LogWriter *pLog, i64 iOff){
  return firstByteOnSector(pLog, iOff) + pLog->szSector - 1;
}

/*
** If possible, reclaim log file space. Log file space is reclaimed after
** a snapshot that points to the same data in the database file is synced
** into the db header.
*/
static int logReclaimSpace(lsm_db *pDb){
  int rc;
  int iMeta;
  int bRotrans;                   /* True if there exists some ro-trans */

  /* Test if there exists some other connection with a read-only transaction
  ** open. If there does, then log file space may not be reclaimed.  */
  rc = lsmDetectRoTrans(pDb, &bRotrans);
  if( rc!=LSM_OK || bRotrans ) return rc;

  iMeta = (int)pDb->pShmhdr->iMetaPage;
  if( iMeta==1 || iMeta==2 ){
    DbLog *pLog = &pDb->treehdr.log;
    i64 iSyncedId;

    /* Read the snapshot-id of the snapshot stored on meta-page iMeta. Note
    ** that in theory, the value read is untrustworthy (due to a race 
    ** condition - see comments above lsmFsReadSyncedId()). So it is only 
    ** ever used to conclude that no log space can be reclaimed. If it seems
    ** to indicate that it may be possible to reclaim log space, a
    ** second call to lsmCheckpointSynced() (which does return trustworthy
    ** values) is made below to confirm.  */
    rc = lsmFsReadSyncedId(pDb, iMeta, &iSyncedId);

    if( rc==LSM_OK && pLog->iSnapshotId!=iSyncedId ){
      i64 iSnapshotId = 0;
      i64 iOff = 0;
      rc = lsmCheckpointSynced(pDb, &iSnapshotId, &iOff, 0);
      if( rc==LSM_OK && pLog->iSnapshotId<iSnapshotId ){
        int iRegion;
        for(iRegion=0; iRegion<3; iRegion++){
          LogRegion *p = &pLog->aRegion[iRegion];
          if( iOff>=p->iStart && iOff<=p->iEnd ) break;
          p->iStart = 0;
          p->iEnd = 0;
        }
        assert( iRegion<3 );
        pLog->aRegion[iRegion].iStart = iOff;
        pLog->iSnapshotId = iSnapshotId;
      }
    }
  }
  return rc;
}

/*
** This function is called when a write-transaction is first opened. It
** is assumed that the caller is holding the client-mutex when it is 
** called.
**
** Before returning, this function allocates the LogWriter object that
** will be used to write to the log file during the write transaction.
** LSM_OK is returned if no error occurs, otherwise an LSM error code.
*/
static int lsmLogBegin(lsm_db *pDb){
  int rc = LSM_OK;
  LogWriter *pNew;
  LogRegion *aReg;

  if( pDb->bUseLog==0 ) return LSM_OK;

  /* If the log file has not yet been opened, open it now. Also allocate
  ** the LogWriter structure, if it has not already been allocated.  */
  rc = lsmFsOpenLog(pDb, 0);
  if( pDb->pLogWriter==0 ){
    pNew = lsmMallocZeroRc(pDb->pEnv, sizeof(LogWriter), &rc);
    if( pNew ){
      lsmStringInit(&pNew->buf, pDb->pEnv);
      rc = lsmStringExtend(&pNew->buf, 2);
    }
    pDb->pLogWriter = pNew;
  }else{
    pNew = pDb->pLogWriter;
    assert( (u8 *)(&pNew[1])==(u8 *)(&((&pNew->buf)[1])) );
    memset(pNew, 0, ((u8 *)&pNew->buf) - (u8 *)pNew);
    pNew->buf.n = 0;
  }

  if( rc==LSM_OK ){
    /* The following call detects whether or not a new snapshot has been 
    ** synced into the database file. If so, it updates the contents of
    ** the pDb->treehdr.log structure to reclaim any space in the log
    ** file that is no longer required. 
    **
    ** TODO: Calling this every transaction is overkill. And since the 
    ** call has to read and checksum a snapshot from the database file,
    ** it is expensive. It would be better to figure out a way so that
    ** this is only called occasionally - say for every 32KB written to 
    ** the log file.
    */
    rc = logReclaimSpace(pDb);
  }
  if( rc!=LSM_OK ){
    lsmLogClose(pDb);
    return rc;
  }

  /* Set the effective sector-size for this transaction. Sectors are assumed
  ** to be one byte in size if the safety-mode is OFF or NORMAL, or as
  ** reported by lsmFsSectorSize if it is FULL.  */
  if( pDb->eSafety==LSM_SAFETY_FULL ){
    pNew->szSector = lsmFsSectorSize(pDb->pFS);
    assert( pNew->szSector>0 );
  }else{
    pNew->szSector = 1;
  }

  /* There are now three scenarios:
  **
  **   1) Regions 0 and 1 are both zero bytes in size and region 2 begins
  **      at a file offset greater than LSM_MIN_LOGWRAP. In this case, wrap
  **      around to the start and write data into the start of the log file. 
  **
  **   2) Region 1 is zero bytes in size and region 2 occurs earlier in the 
  **      file than region 0. In this case, append data to region 2, but
  **      remember to jump over region 1 if required.
  **
  **   3) Region 2 is the last in the file. Append to it.
  */
  aReg = &pDb->treehdr.log.aRegion[0];

  assert( aReg[0].iEnd==0 || aReg[0].iEnd>aReg[0].iStart );
  assert( aReg[1].iEnd==0 || aReg[1].iEnd>aReg[1].iStart );

  pNew->cksum0 = pDb->treehdr.log.cksum0;
  pNew->cksum1 = pDb->treehdr.log.cksum1;

  if( aReg[0].iEnd==0 && aReg[1].iEnd==0 && aReg[2].iStart>=LSM_MIN_LOGWRAP ){
    /* Case 1. Wrap around to the start of the file. Write an LSM_LOG_JUMP 
    ** into the log file in this case. Pad it out to 8 bytes using a PAD2
    ** record so that the checksums can be updated immediately.  */
    u8 aJump[] = { 
      LSM_LOG_PAD2, 0x04, 0x00, 0x00, 0x00, 0x00, LSM_LOG_JUMP, 0x00 
    };

    lsmStringBinAppend(&pNew->buf, aJump, sizeof(aJump));
    logUpdateCksum(pNew, pNew->buf.n);
    rc = lsmFsWriteLog(pDb->pFS, aReg[2].iEnd, &pNew->buf);
    pNew->iCksumBuf = pNew->buf.n = 0;

    aReg[2].iEnd += 8;
    pNew->jump = aReg[0] = aReg[2];
    aReg[2].iStart = aReg[2].iEnd = 0;
  }else if( aReg[1].iEnd==0 && aReg[2].iEnd<aReg[0].iEnd ){
    /* Case 2. */
    pNew->iOff = aReg[2].iEnd;
    pNew->jump = aReg[0];
  }else{
    /* Case 3. */
    assert( aReg[2].iStart>=aReg[0].iEnd && aReg[2].iStart>=aReg[1].iEnd );
    pNew->iOff = aReg[2].iEnd;
  }

  if( pNew->jump.iStart ){
    i64 iRound;
    assert( pNew->jump.iStart>pNew->iOff );

    iRound = firstByteOnSector(pNew, pNew->jump.iStart);
    if( iRound>pNew->iOff ) pNew->jump.iStart = iRound;
    pNew->jump.iEnd = lastByteOnSector(pNew, pNew->jump.iEnd);
  }

  assert( pDb->pLogWriter==pNew );
  return rc;
}

/*
** This function is called when a write-transaction is being closed.
** Parameter bCommit is true if the transaction is being committed,
** or false otherwise. The caller must hold the client-mutex to call
** this function.
**
** A call to this function deletes the LogWriter object allocated by
** lsmLogBegin(). If the transaction is being committed, the shared state
** in *pLog is updated before returning.
*/
static void lsmLogEnd(lsm_db *pDb, int bCommit){
  DbLog *pLog;
  LogWriter *p;
  p = pDb->pLogWriter;

  if( p==0 ) return;
  pLog = &pDb->treehdr.log;

  if( bCommit ){
    pLog->aRegion[2].iEnd = p->iOff;
    pLog->cksum0 = p->cksum0;
    pLog->cksum1 = p->cksum1;
    if( p->iRegion1End ){
      /* This happens when the transaction had to jump over some other
      ** part of the log.  */
      assert( pLog->aRegion[1].iEnd==0 );
      assert( pLog->aRegion[2].iStart<p->iRegion1End );
      pLog->aRegion[1].iStart = pLog->aRegion[2].iStart;
      pLog->aRegion[1].iEnd = p->iRegion1End;
      pLog->aRegion[2].iStart = p->iRegion2Start;
    }
  }
}

static int jumpIfRequired(
  lsm_db *pDb,
  LogWriter *pLog,
  int nReq,
  int *pbJump
){
  /* Determine if it is necessary to add an LSM_LOG_JUMP to jump over the
  ** jump region before writing the LSM_LOG_WRITE or DELETE record. This
  ** is necessary if there is insufficient room between the current offset
  ** and the jump region to fit the new WRITE/DELETE record and the largest
  ** possible JUMP record with up to 7 bytes of padding (a total of 17 
  ** bytes).  */
  if( (pLog->jump.iStart > (pLog->iOff + pLog->buf.n))
   && (pLog->jump.iStart < (pLog->iOff + pLog->buf.n + (nReq + 17))) 
  ){
    int rc;                       /* Return code */
    i64 iJump;                    /* Offset to jump to */
    u8 aJump[10];                 /* Encoded jump record */
    int nJump;                    /* Valid bytes in aJump[] */
    int nPad;                     /* Bytes of padding required */

    /* Serialize the JUMP record */
    iJump = pLog->jump.iEnd+1;
    aJump[0] = LSM_LOG_JUMP;
    nJump = 1 + lsmVarintPut64(&aJump[1], iJump);

    /* Adding padding to the contents of the buffer so that it will be a 
    ** multiple of 8 bytes in size after the JUMP record is appended. This
    ** is not strictly required, it just makes the keeping the running 
    ** checksum up to date in this file a little simpler.  */
    nPad = (pLog->buf.n + nJump) % 8;
    if( nPad ){
      u8 aPad[7] = {0,0,0,0,0,0,0};
      nPad = 8-nPad;
      if( nPad==1 ){
        aPad[0] = LSM_LOG_PAD1;
      }else{
        aPad[0] = LSM_LOG_PAD2;
        aPad[1] = (u8)(nPad-2);
      }
      rc = lsmStringBinAppend(&pLog->buf, aPad, nPad);
      if( rc!=LSM_OK ) return rc;
    }

    /* Append the JUMP record to the buffer. Then flush the buffer to disk
    ** and update the checksums. The next write to the log file (assuming
    ** there is no transaction rollback) will be to offset iJump (just past
    ** the jump region).  */
    rc = lsmStringBinAppend(&pLog->buf, aJump, nJump);
    if( rc!=LSM_OK ) return rc;
    assert( (pLog->buf.n % 8)==0 );
    rc = lsmFsWriteLog(pDb->pFS, pLog->iOff, &pLog->buf);
    if( rc!=LSM_OK ) return rc;
    logUpdateCksum(pLog, pLog->buf.n);
    pLog->iRegion1End = (pLog->iOff + pLog->buf.n);
    pLog->iRegion2Start = iJump;
    pLog->iOff = iJump;
    pLog->iCksumBuf = pLog->buf.n = 0;
    if( pbJump ) *pbJump = 1;
  }

  return LSM_OK;
}

static int logCksumAndFlush(lsm_db *pDb){
  int rc;                         /* Return code */
  LogWriter *pLog = pDb->pLogWriter;

  /* Calculate the checksum value. Append it to the buffer. */
  logUpdateCksum(pLog, pLog->buf.n);
  lsmPutU32((u8 *)&pLog->buf.z[pLog->buf.n], pLog->cksum0);
  pLog->buf.n += 4;
  lsmPutU32((u8 *)&pLog->buf.z[pLog->buf.n], pLog->cksum1);
  pLog->buf.n += 4;

  /* Write the contents of the buffer to disk. */
  rc = lsmFsWriteLog(pDb->pFS, pLog->iOff, &pLog->buf);
  pLog->iOff += pLog->buf.n;
  pLog->iCksumBuf = pLog->buf.n = 0;

  return rc;
}

/*
** Write the contents of the log-buffer to disk. Then write either a CKSUM
** or COMMIT record, depending on the value of parameter eType.
*/
static int logFlush(lsm_db *pDb, int eType){
  int rc;
  int nReq;
  LogWriter *pLog = pDb->pLogWriter;
  
  assert( eType==LSM_LOG_COMMIT );
  assert( pLog );

  /* Commit record is always 9 bytes in size. */
  nReq = 9;
  if( eType==LSM_LOG_COMMIT && pLog->szSector>1 ) nReq += pLog->szSector + 17;
  rc = jumpIfRequired(pDb, pLog, nReq, 0);

  /* If this is a COMMIT, add padding to the log so that the COMMIT record
  ** is aligned against the end of a disk sector. In other words, add padding
  ** so that the first byte following the COMMIT record lies on a different
  ** sector.  */
  if( eType==LSM_LOG_COMMIT && pLog->szSector>1 ){
    int nPad;                     /* Bytes of padding to add */

    /* Determine the value of nPad. */
    nPad = ((pLog->iOff + pLog->buf.n + 9) % pLog->szSector);
    if( nPad ) nPad = pLog->szSector - nPad;
    rc = lsmStringExtend(&pLog->buf, nPad);
    if( rc!=LSM_OK ) return rc;

    while( nPad ){
      if( nPad==1 ){
        pLog->buf.z[pLog->buf.n++] = LSM_LOG_PAD1;
        nPad = 0;
      }else{
        int n = LSM_MIN(200, nPad-2);
        pLog->buf.z[pLog->buf.n++] = LSM_LOG_PAD2;
        pLog->buf.z[pLog->buf.n++] = (char)n;
        nPad -= 2;
        memset(&pLog->buf.z[pLog->buf.n], 0x2B, n);
        pLog->buf.n += n;
        nPad -= n;
      }
    }
  }

  /* Make sure there is room in the log-buffer to add the CKSUM or COMMIT
  ** record. Then add the first byte of it.  */
  rc = lsmStringExtend(&pLog->buf, 9);
  if( rc!=LSM_OK ) return rc;
  pLog->buf.z[pLog->buf.n++] = (char)eType;
  memset(&pLog->buf.z[pLog->buf.n], 0, 8);

  rc = logCksumAndFlush(pDb);

  /* If this is a commit and synchronous=full, sync the log to disk. */
  if( rc==LSM_OK && eType==LSM_LOG_COMMIT && pDb->eSafety==LSM_SAFETY_FULL ){
    rc = lsmFsSyncLog(pDb->pFS);
  }
  return rc;
}

/*
** Append an LSM_LOG_WRITE (if nVal>=0) or LSM_LOG_DELETE (if nVal<0) 
** record to the database log.
*/
static int lsmLogWrite(
  lsm_db *pDb,                    /* Database handle */
  int eType,
  void *pKey, int nKey,           /* Database key to write to log */
  void *pVal, int nVal            /* Database value (or nVal<0) to write */
){
  int rc = LSM_OK;
  LogWriter *pLog;                /* Log object to write to */
  int nReq;                       /* Bytes of space required in log */
  int bCksum = 0;                 /* True to embed a checksum in this record */

  assert( eType==LSM_WRITE || eType==LSM_DELETE || eType==LSM_DRANGE );
  assert( LSM_LOG_WRITE==LSM_WRITE );
  assert( LSM_LOG_DELETE==LSM_DELETE );
  assert( LSM_LOG_DRANGE==LSM_DRANGE );
  assert( (eType==LSM_LOG_DELETE)==(nVal<0) );

  if( pDb->bUseLog==0 ) return LSM_OK;
  pLog = pDb->pLogWriter;

  /* Determine how many bytes of space are required, assuming that a checksum
  ** will be embedded in this record (even though it may not be).  */
  nReq = 1 + lsmVarintLen32(nKey) + 8 + nKey;
  if( eType!=LSM_LOG_DELETE ) nReq += lsmVarintLen32(nVal) + nVal;

  /* Jump over the jump region if required. Set bCksum to true to tell the
  ** code below to include a checksum in the record if either (a) writing
  ** this record would mean that more than LSM_CKSUM_MAXDATA bytes of data
  ** have been written to the log since the last checksum, or (b) the jump
  ** is taken.  */
  rc = jumpIfRequired(pDb, pLog, nReq, &bCksum);
  if( (pLog->buf.n+nReq) > LSM_CKSUM_MAXDATA ) bCksum = 1;

  if( rc==LSM_OK ){
    rc = lsmStringExtend(&pLog->buf, nReq);
  }
  if( rc==LSM_OK ){
    u8 *a = (u8 *)&pLog->buf.z[pLog->buf.n];
    
    /* Write the record header - the type byte followed by either 1 (for
    ** DELETE) or 2 (for WRITE) varints.  */
    assert( LSM_LOG_WRITE_CKSUM == (LSM_LOG_WRITE | 0x0001) );
    assert( LSM_LOG_DELETE_CKSUM == (LSM_LOG_DELETE | 0x0001) );
    assert( LSM_LOG_DRANGE_CKSUM == (LSM_LOG_DRANGE | 0x0001) );
    *(a++) = (u8)eType | (u8)bCksum;
    a += lsmVarintPut32(a, nKey);
    if( eType!=LSM_LOG_DELETE ) a += lsmVarintPut32(a, nVal);

    if( bCksum ){
      pLog->buf.n = (a - (u8 *)pLog->buf.z);
      rc = logCksumAndFlush(pDb);
      a = (u8 *)&pLog->buf.z[pLog->buf.n];
    }

    memcpy(a, pKey, nKey);
    a += nKey;
    if( eType!=LSM_LOG_DELETE ){
      memcpy(a, pVal, nVal);
      a += nVal;
    }
    pLog->buf.n = a - (u8 *)pLog->buf.z;
    assert( pLog->buf.n<=pLog->buf.nAlloc );
  }

  return rc;
}

/*
** Append an LSM_LOG_COMMIT record to the database log.
*/
static int lsmLogCommit(lsm_db *pDb){
  if( pDb->bUseLog==0 ) return LSM_OK;
  return logFlush(pDb, LSM_LOG_COMMIT);
}

/*
** Store the current offset and other checksum related information in the
** structure *pMark. Later, *pMark can be passed to lsmLogSeek() to "rewind"
** the LogWriter object to the current log file offset. This is used when
** rolling back savepoint transactions.
*/
static void lsmLogTell(
  lsm_db *pDb,                    /* Database handle */
  LogMark *pMark                  /* Populate this object with current offset */
){
  LogWriter *pLog;
  int nCksum;

  if( pDb->bUseLog==0 ) return;
  pLog = pDb->pLogWriter;
  nCksum = pLog->buf.n & 0xFFFFFFF8;
  logUpdateCksum(pLog, nCksum);
  assert( pLog->iCksumBuf==nCksum );
  pMark->nBuf = pLog->buf.n - nCksum;
  memcpy(pMark->aBuf, &pLog->buf.z[nCksum], pMark->nBuf);

  pMark->iOff = pLog->iOff + pLog->buf.n;
  pMark->cksum0 = pLog->cksum0;
  pMark->cksum1 = pLog->cksum1;
}

/*
** Seek (rewind) back to the log file offset stored by an ealier call to
** lsmLogTell() in *pMark.
*/
static void lsmLogSeek(
  lsm_db *pDb,                    /* Database handle */
  LogMark *pMark                  /* Object containing log offset to seek to */
){
  LogWriter *pLog;

  if( pDb->bUseLog==0 ) return;
  pLog = pDb->pLogWriter;

  assert( pMark->iOff<=pLog->iOff+pLog->buf.n );
  if( (pMark->iOff & 0xFFFFFFF8)>=pLog->iOff ){
    pLog->buf.n = (int)(pMark->iOff - pLog->iOff);
    pLog->iCksumBuf = (pLog->buf.n & 0xFFFFFFF8);
  }else{
    pLog->buf.n = pMark->nBuf;
    memcpy(pLog->buf.z, pMark->aBuf, pMark->nBuf);
    pLog->iCksumBuf = 0;
    pLog->iOff = pMark->iOff - pMark->nBuf;
  }
  pLog->cksum0 = pMark->cksum0;
  pLog->cksum1 = pMark->cksum1;

  if( pMark->iOff > pLog->iRegion1End ) pLog->iRegion1End = 0;
  if( pMark->iOff > pLog->iRegion2Start ) pLog->iRegion2Start = 0;
}

/*
** This function does the work for an lsm_info(LOG_STRUCTURE) request.
*/
static int lsmInfoLogStructure(lsm_db *pDb, char **pzVal){
  int rc = LSM_OK;
  char *zVal = 0;

  /* If there is no read or write transaction open, read the latest 
  ** tree-header from shared-memory to report on. If necessary, update
  ** it based on the contents of the database header.  
  **
  ** No locks are taken here - these are passive read operations only.
  */
  if( pDb->pCsr==0 && pDb->nTransOpen==0 ){
    rc = lsmTreeLoadHeader(pDb, 0);
    if( rc==LSM_OK ) rc = logReclaimSpace(pDb);
  }

  if( rc==LSM_OK ){
    DbLog *pLog = &pDb->treehdr.log;
    zVal = lsmMallocPrintf(pDb->pEnv, 
        "%d %d %d %d %d %d", 
        (int)pLog->aRegion[0].iStart, (int)pLog->aRegion[0].iEnd,
        (int)pLog->aRegion[1].iStart, (int)pLog->aRegion[1].iEnd,
        (int)pLog->aRegion[2].iStart, (int)pLog->aRegion[2].iEnd
    );
    if( !zVal ) rc = LSM_NOMEM_BKPT;
  }

  *pzVal = zVal;
  return rc;
}

/*************************************************************************
** Begin code for log recovery.
*/

typedef struct LogReader LogReader;
struct LogReader {
  FileSystem *pFS;                /* File system to read from */
  i64 iOff;                       /* File offset at end of buf content */
  int iBuf;                       /* Current read offset in buf */
  LsmString buf;                  /* Buffer containing file content */

  int iCksumBuf;                  /* Offset in buf corresponding to cksum[01] */
  u32 cksum0;                     /* Checksum 0 at offset iCksumBuf */
  u32 cksum1;                     /* Checksum 1 at offset iCksumBuf */
};

static void logReaderBlob(
  LogReader *p,                   /* Log reader object */
  LsmString *pBuf,                /* Dynamic storage, if required */
  int nBlob,                      /* Number of bytes to read */
  u8 **ppBlob,                    /* OUT: Pointer to blob read */
  int *pRc                        /* IN/OUT: Error code */
){
  static const int LOG_READ_SIZE = 512;
  int rc = *pRc;                  /* Return code */
  int nReq = nBlob;               /* Bytes required */

  while( rc==LSM_OK && nReq>0 ){
    int nAvail;                   /* Bytes of data available in p->buf */
    if( p->buf.n==p->iBuf ){
      int nCksum;                 /* Total bytes requiring checksum */
      int nCarry = 0;             /* Total bytes requiring checksum */

      nCksum = p->iBuf - p->iCksumBuf;
      if( nCksum>0 ){
        nCarry = nCksum % 8;
        nCksum = ((nCksum / 8) * 8);
        if( nCksum>0 ){
          logCksumUnaligned(
              &p->buf.z[p->iCksumBuf], nCksum, &p->cksum0, &p->cksum1
          );
        }
      }
      if( nCarry>0 ) memcpy(p->buf.z, &p->buf.z[p->iBuf-nCarry], nCarry);
      p->buf.n = nCarry;
      p->iBuf = nCarry;

      rc = lsmFsReadLog(p->pFS, p->iOff, LOG_READ_SIZE, &p->buf);
      if( rc!=LSM_OK ) break;
      p->iCksumBuf = 0;
      p->iOff += LOG_READ_SIZE;
    }

    nAvail = p->buf.n - p->iBuf;
    if( ppBlob && nReq==nBlob && nBlob<=nAvail ){
      *ppBlob = (u8 *)&p->buf.z[p->iBuf];
      p->iBuf += nBlob;
      nReq = 0;
    }else{
      int nCopy = LSM_MIN(nAvail, nReq);
      if( nBlob==nReq ){
        pBuf->n = 0;
      }
      rc = lsmStringBinAppend(pBuf, (u8 *)&p->buf.z[p->iBuf], nCopy);
      nReq -= nCopy;
      p->iBuf += nCopy;
      if( nReq==0 && ppBlob ){
        *ppBlob = (u8*)pBuf->z;
      }
    }
  }

  *pRc = rc;
}

static void logReaderVarint(
  LogReader *p, 
  LsmString *pBuf,
  int *piVal,                     /* OUT: Value read from log */
  int *pRc                        /* IN/OUT: Error code */
){
  if( *pRc==LSM_OK ){
    u8 *aVarint;
    if( p->buf.n==p->iBuf ){
      logReaderBlob(p, 0, 10, &aVarint, pRc);
      if( LSM_OK==*pRc ) p->iBuf -= (10 - lsmVarintGet32(aVarint, piVal));
    }else{
      logReaderBlob(p, pBuf, lsmVarintSize(p->buf.z[p->iBuf]), &aVarint, pRc);
      if( LSM_OK==*pRc ) lsmVarintGet32(aVarint, piVal);
    }
  }
}

static void logReaderByte(LogReader *p, u8 *pByte, int *pRc){
  u8 *pPtr = 0;
  logReaderBlob(p, 0, 1, &pPtr, pRc);
  if( pPtr ) *pByte = *pPtr;
}

static void logReaderCksum(LogReader *p, LsmString *pBuf, int *pbEof, int *pRc){
  if( *pRc==LSM_OK ){
    u8 *pPtr = 0;
    u32 cksum0, cksum1;
    int nCksum = p->iBuf - p->iCksumBuf;

    /* Update in-memory (expected) checksums */
    assert( nCksum>=0 );
    logCksumUnaligned(&p->buf.z[p->iCksumBuf], nCksum, &p->cksum0, &p->cksum1);
    p->iCksumBuf = p->iBuf + 8;
    logReaderBlob(p, pBuf, 8, &pPtr, pRc);
    assert( pPtr || *pRc );

    /* Read the checksums from the log file. Set *pbEof if they do not match. */
    if( pPtr ){
      cksum0 = lsmGetU32(pPtr);
      cksum1 = lsmGetU32(&pPtr[4]);
      *pbEof = (cksum0!=p->cksum0 || cksum1!=p->cksum1);
      p->iCksumBuf = p->iBuf;
    }
  }
}

static void logReaderInit(
  lsm_db *pDb,                    /* Database handle */
  DbLog *pLog,                    /* Log object associated with pDb */
  int bInitBuf,                   /* True if p->buf is uninitialized */
  LogReader *p                    /* Initialize this LogReader object */
){
  p->pFS = pDb->pFS;
  p->iOff = pLog->aRegion[2].iStart;
  p->cksum0 = pLog->cksum0;
  p->cksum1 = pLog->cksum1;
  if( bInitBuf ){ lsmStringInit(&p->buf, pDb->pEnv); }
  p->buf.n = 0;
  p->iCksumBuf = 0;
  p->iBuf = 0;
}

/*
** This function is called after reading the header of a LOG_DELETE or
** LOG_WRITE record. Parameter nByte is the total size of the key and
** value that follow the header just read. Return true if the size and
** position of the record indicate that it should contain a checksum.
*/
static int logRequireCksum(LogReader *p, int nByte){
  return ((p->iBuf + nByte - p->iCksumBuf) > LSM_CKSUM_MAXDATA);
}

/*
** Recover the contents of the log file.
*/
static int lsmLogRecover(lsm_db *pDb){
  LsmString buf1;                 /* Key buffer */
  LsmString buf2;                 /* Value buffer */
  LogReader reader;               /* Log reader object */
  int rc = LSM_OK;                /* Return code */
  int nCommit = 0;                /* Number of transactions to recover */
  int iPass;
  int nJump = 0;                  /* Number of LSM_LOG_JUMP records in pass 0 */
  DbLog *pLog;
  int bOpen;

  rc = lsmFsOpenLog(pDb, &bOpen);
  if( rc!=LSM_OK ) return rc;

  rc = lsmTreeInit(pDb);
  if( rc!=LSM_OK ) return rc;

  pLog = &pDb->treehdr.log;
  lsmCheckpointLogoffset(pDb->pShmhdr->aSnap2, pLog);

  logReaderInit(pDb, pLog, 1, &reader);
  lsmStringInit(&buf1, pDb->pEnv);
  lsmStringInit(&buf2, pDb->pEnv);

  /* The outer for() loop runs at most twice. The first iteration is to 
  ** count the number of committed transactions in the log. The second 
  ** iterates through those transactions and updates the in-memory tree 
  ** structure with their contents.  */
  if( bOpen ){
    for(iPass=0; iPass<2 && rc==LSM_OK; iPass++){
      int bEof = 0;

      while( rc==LSM_OK && !bEof ){
        u8 eType = 0;
        logReaderByte(&reader, &eType, &rc);

        switch( eType ){
          case LSM_LOG_PAD1:
            break;

          case LSM_LOG_PAD2: {
            int nPad;
            logReaderVarint(&reader, &buf1, &nPad, &rc);
            logReaderBlob(&reader, &buf1, nPad, 0, &rc);
            break;
          }

          case LSM_LOG_DRANGE:
          case LSM_LOG_DRANGE_CKSUM:
          case LSM_LOG_WRITE:
          case LSM_LOG_WRITE_CKSUM: {
            int nKey;
            int nVal;
            u8 *aVal;
            logReaderVarint(&reader, &buf1, &nKey, &rc);
            logReaderVarint(&reader, &buf2, &nVal, &rc);

            if( eType==LSM_LOG_WRITE_CKSUM || eType==LSM_LOG_DRANGE_CKSUM ){
              logReaderCksum(&reader, &buf1, &bEof, &rc);
            }else{
              bEof = logRequireCksum(&reader, nKey+nVal);
            }
            if( bEof ) break;

            logReaderBlob(&reader, &buf1, nKey, 0, &rc);
            logReaderBlob(&reader, &buf2, nVal, &aVal, &rc);
            if( iPass==1 && rc==LSM_OK ){ 
              if( eType==LSM_LOG_WRITE || eType==LSM_LOG_WRITE_CKSUM ){
                rc = lsmTreeInsert(pDb, (u8 *)buf1.z, nKey, aVal, nVal);
              }else{
                rc = lsmTreeDelete(pDb, (u8 *)buf1.z, nKey, aVal, nVal);
              }
            }
            break;
          }

          case LSM_LOG_DELETE:
          case LSM_LOG_DELETE_CKSUM: {
            int nKey; u8 *aKey;
            logReaderVarint(&reader, &buf1, &nKey, &rc);

            if( eType==LSM_LOG_DELETE_CKSUM ){
              logReaderCksum(&reader, &buf1, &bEof, &rc);
            }else{
              bEof = logRequireCksum(&reader, nKey);
            }
            if( bEof ) break;

            logReaderBlob(&reader, &buf1, nKey, &aKey, &rc);
            if( iPass==1 && rc==LSM_OK ){ 
              rc = lsmTreeInsert(pDb, aKey, nKey, NULL, -1);
            }
            break;
          }

          case LSM_LOG_COMMIT:
            logReaderCksum(&reader, &buf1, &bEof, &rc);
            if( bEof==0 ){
              nCommit++;
              assert( nCommit>0 || iPass==1 );
              if( nCommit==0 ) bEof = 1;
            }
            break;

          case LSM_LOG_JUMP: {
            int iOff = 0;
            logReaderVarint(&reader, &buf1, &iOff, &rc);
            if( rc==LSM_OK ){
              if( iPass==1 ){
                if( pLog->aRegion[2].iStart==0 ){
                  assert( pLog->aRegion[1].iStart==0 );
                  pLog->aRegion[1].iEnd = reader.iOff;
                }else{
                  assert( pLog->aRegion[0].iStart==0 );
                  pLog->aRegion[0].iStart = pLog->aRegion[2].iStart;
                  pLog->aRegion[0].iEnd = reader.iOff-reader.buf.n+reader.iBuf;
                }
                pLog->aRegion[2].iStart = iOff;
              }else{
                if( (nJump++)==2 ){
                  bEof = 1;
                }
              }

              reader.iOff = iOff;
              reader.buf.n = reader.iBuf;
            }
            break;
          }

          default:
            /* Including LSM_LOG_EOF */
            bEof = 1;
            break;
        }
      }

      if( rc==LSM_OK && iPass==0 ){
        if( nCommit==0 ){
          if( pLog->aRegion[2].iStart==0 ){
            iPass = 1;
          }else{
            pLog->aRegion[2].iStart = 0;
            iPass = -1;
            lsmCheckpointZeroLogoffset(pDb);
          }
        }
        logReaderInit(pDb, pLog, 0, &reader);
        nCommit = nCommit * -1;
      }
    }
  }

  /* Initialize DbLog object */
  if( rc==LSM_OK ){
    pLog->aRegion[2].iEnd = reader.iOff - reader.buf.n + reader.iBuf;
    pLog->cksum0 = reader.cksum0;
    pLog->cksum1 = reader.cksum1;
  }

  if( rc==LSM_OK ){
    rc = lsmFinishRecovery(pDb);
  }else{
    lsmFinishRecovery(pDb);
  }

  if( pDb->bRoTrans ){
    lsmFsCloseLog(pDb);
  }

  lsmStringClear(&buf1);
  lsmStringClear(&buf2);
  lsmStringClear(&reader.buf);
  return rc;
}

static void lsmLogClose(lsm_db *db){
  if( db->pLogWriter ){
    lsmFree(db->pEnv, db->pLogWriter->buf.z);
    lsmFree(db->pEnv, db->pLogWriter);
    db->pLogWriter = 0;
  }
}

#line 1 "lsm_main.c"
/*
** 2011-08-18
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** The main interface to the LSM module.
*/
/* #include "lsmInt.h" */


#ifdef LSM_DEBUG
/*
** This function returns a copy of its only argument.
**
** When the library is built with LSM_DEBUG defined, this function is called
** whenever an error code is generated (not propagated - generated). So
** if the library is mysteriously returning (say) LSM_IOERR, a breakpoint
** may be set in this function to determine why.
*/
static int lsmErrorBkpt(int rc){
  /* Set breakpoint here! */
  return rc;
}

/*
** This function contains various assert() statements that test that the
** lsm_db structure passed as an argument is internally consistent.
*/
static void assert_db_state(lsm_db *pDb){

  /* If there is at least one cursor or a write transaction open, the database
  ** handle must be holding a pointer to a client snapshot. And the reverse 
  ** - if there are no open cursors and no write transactions then there must 
  ** not be a client snapshot.  */
  
  assert( (pDb->pCsr!=0||pDb->nTransOpen>0)==(pDb->iReader>=0||pDb->bRoTrans) );

  assert( (pDb->iReader<0 && pDb->bRoTrans==0) || pDb->pClient!=0 );

  assert( pDb->nTransOpen>=0 );
}
#else
# define assert_db_state(x) 
#endif

/*
** The default key-compare function.
*/
static int xCmp(void *p1, int n1, void *p2, int n2){
  int res;
  res = memcmp(p1, p2, LSM_MIN(n1, n2));
  if( res==0 ) res = (n1-n2);
  return res;
}

static void xLog(void *pCtx, int rc, const char *z){
  (void)(rc);
  (void)(pCtx);
  fprintf(stderr, "%s\n", z);
  fflush(stderr);
}

/*
** Allocate a new db handle.
*/
int lsm_new(lsm_env *pEnv, lsm_db **ppDb){
  lsm_db *pDb;

  /* If the user did not provide an environment, use the default. */
  if( pEnv==0 ) pEnv = lsm_default_env();
  assert( pEnv );

  /* Allocate the new database handle */
  *ppDb = pDb = (lsm_db *)lsmMallocZero(pEnv, sizeof(lsm_db));
  if( pDb==0 ) return LSM_NOMEM_BKPT;

  /* Initialize the new object */
  pDb->pEnv = pEnv;
  pDb->nTreeLimit = LSM_DFLT_AUTOFLUSH;
  pDb->nAutockpt = LSM_DFLT_AUTOCHECKPOINT;
  pDb->bAutowork = LSM_DFLT_AUTOWORK;
  pDb->eSafety = LSM_DFLT_SAFETY;
  pDb->xCmp = xCmp;
  pDb->nDfltPgsz = LSM_DFLT_PAGE_SIZE;
  pDb->nDfltBlksz = LSM_DFLT_BLOCK_SIZE;
  pDb->nMerge = LSM_DFLT_AUTOMERGE;
  pDb->nMaxFreelist = LSM_MAX_FREELIST_ENTRIES;
  pDb->bUseLog = LSM_DFLT_USE_LOG;
  pDb->iReader = -1;
  pDb->iRwclient = -1;
  pDb->bMultiProc = LSM_DFLT_MULTIPLE_PROCESSES;
  pDb->iMmap = LSM_DFLT_MMAP;
  pDb->xLog = xLog;
  pDb->compress.iId = LSM_COMPRESSION_NONE;
  return LSM_OK;
}

lsm_env *lsm_get_env(lsm_db *pDb){
  assert( pDb->pEnv );
  return pDb->pEnv;
}

/*
** If database handle pDb is currently holding a client snapshot, but does
** not have any open cursors or write transactions, release it.
*/
static void dbReleaseClientSnapshot(lsm_db *pDb){
  if( pDb->nTransOpen==0 && pDb->pCsr==0 ){
    lsmFinishReadTrans(pDb);
  }
}

static int getFullpathname(
  lsm_env *pEnv, 
  const char *zRel,
  char **pzAbs
){
  int nAlloc = 0;
  char *zAlloc = 0;
  int nReq = 0;
  int rc;

  do{
    nAlloc = nReq;
    rc = pEnv->xFullpath(pEnv, zRel, zAlloc, &nReq);
    if( nReq>nAlloc ){
      zAlloc = lsmReallocOrFreeRc(pEnv, zAlloc, nReq, &rc);
    }
  }while( nReq>nAlloc && rc==LSM_OK );

  if( rc!=LSM_OK ){
    lsmFree(pEnv, zAlloc);
    zAlloc = 0;
  }
  *pzAbs = zAlloc;
  return rc;
}

/*
** Check that the bits in the db->mLock mask are consistent with the
** value stored in db->iRwclient. An assert shall fail otherwise.
*/
static void assertRwclientLockValue(lsm_db *db){
#ifndef NDEBUG
  u64 msk;                        /* Mask of mLock bits for RWCLIENT locks */
  u64 rwclient = 0;               /* Bit corresponding to db->iRwclient */

  if( db->iRwclient>=0 ){
    rwclient = ((u64)1 << (LSM_LOCK_RWCLIENT(db->iRwclient)-1));
  }
  msk  = ((u64)1 << (LSM_LOCK_RWCLIENT(LSM_LOCK_NRWCLIENT)-1)) - 1;
  msk -= (((u64)1 << (LSM_LOCK_RWCLIENT(0)-1)) - 1);

  assert( (db->mLock & msk)==rwclient );
#endif
}

/*
** Open a new connection to database zFilename.
*/
int lsm_open(lsm_db *pDb, const char *zFilename){
  int rc;

  if( pDb->pDatabase ){
    rc = LSM_MISUSE;
  }else{
    char *zFull;

    /* Translate the possibly relative pathname supplied by the user into
    ** an absolute pathname. This is required because the supplied path
    ** is used (either directly or with "-log" appended to it) for more 
    ** than one purpose - to open both the database and log files, and 
    ** perhaps to unlink the log file during disconnection. An absolute
    ** path is required to ensure that the correct files are operated
    ** on even if the application changes the cwd.  */
    rc = getFullpathname(pDb->pEnv, zFilename, &zFull);
    assert( rc==LSM_OK || zFull==0 );

    /* Connect to the database. */
    if( rc==LSM_OK ){
      rc = lsmDbDatabaseConnect(pDb, zFull);
    }

    if( pDb->bReadonly==0 ){
      /* Configure the file-system connection with the page-size and block-size
      ** of this database. Even if the database file is zero bytes in size
      ** on disk, these values have been set in shared-memory by now, and so 
      ** are guaranteed not to change during the lifetime of this connection.  
      */
      if( rc==LSM_OK && LSM_OK==(rc = lsmCheckpointLoad(pDb, 0)) ){
        lsmFsSetPageSize(pDb->pFS, lsmCheckpointPgsz(pDb->aSnapshot));
        lsmFsSetBlockSize(pDb->pFS, lsmCheckpointBlksz(pDb->aSnapshot));
      }
    }

    lsmFree(pDb->pEnv, zFull);
    assertRwclientLockValue(pDb);
  }

  assert( pDb->bReadonly==0 || pDb->bReadonly==1 );
  assert( rc!=LSM_OK || (pDb->pShmhdr==0)==(pDb->bReadonly==1) );

  return rc;
}

int lsm_close(lsm_db *pDb){
  int rc = LSM_OK;
  if( pDb ){
    assert_db_state(pDb);
    if( pDb->pCsr || pDb->nTransOpen ){
      rc = LSM_MISUSE_BKPT;
    }else{
      lsmMCursorFreeCache(pDb);
      lsmFreeSnapshot(pDb->pEnv, pDb->pClient);
      pDb->pClient = 0;

      assertRwclientLockValue(pDb);

      lsmDbDatabaseRelease(pDb);
      lsmLogClose(pDb);
      lsmFsClose(pDb->pFS);
      /* assert( pDb->mLock==0 ); */
      
      /* Invoke any destructors registered for the compression or 
      ** compression factory callbacks.  */
      if( pDb->factory.xFree ) pDb->factory.xFree(pDb->factory.pCtx);
      if( pDb->compress.xFree ) pDb->compress.xFree(pDb->compress.pCtx);

      lsmFree(pDb->pEnv, pDb->rollback.aArray);
      lsmFree(pDb->pEnv, pDb->aTrans);
      lsmFree(pDb->pEnv, pDb->apShm);
      lsmFree(pDb->pEnv, pDb);
    }
  }
  return rc;
}

int lsm_config(lsm_db *pDb, int eParam, ...){
  int rc = LSM_OK;
  va_list ap;
  va_start(ap, eParam);

  switch( eParam ){
    case LSM_CONFIG_AUTOFLUSH: {
      /* This parameter is read and written in KB. But all internal 
      ** processing is done in bytes.  */
      int *piVal = va_arg(ap, int *);
      int iVal = *piVal;
      if( iVal>=0 && iVal<=(1024*1024) ){
        pDb->nTreeLimit = iVal*1024;
      }
      *piVal = (pDb->nTreeLimit / 1024);
      break;
    }

    case LSM_CONFIG_AUTOWORK: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>=0 ){
        pDb->bAutowork = *piVal;
      }
      *piVal = pDb->bAutowork;
      break;
    }

    case LSM_CONFIG_AUTOCHECKPOINT: {
      /* This parameter is read and written in KB. But all internal processing
      ** (including the lsm_db.nAutockpt variable) is done in bytes.  */
      int *piVal = va_arg(ap, int *);
      if( *piVal>=0 ){
        int iVal = *piVal;
        pDb->nAutockpt = (i64)iVal * 1024;
      }
      *piVal = (int)(pDb->nAutockpt / 1024);
      break;
    }

    case LSM_CONFIG_PAGE_SIZE: {
      int *piVal = va_arg(ap, int *);
      if( pDb->pDatabase ){
        /* If lsm_open() has been called, this is a read-only parameter. 
        ** Set the output variable to the page-size according to the 
        ** FileSystem object.  */
        *piVal = lsmFsPageSize(pDb->pFS);
      }else{
        if( *piVal>=256 && *piVal<=65536 && ((*piVal-1) & *piVal)==0 ){
          pDb->nDfltPgsz = *piVal;
        }else{
          *piVal = pDb->nDfltPgsz;
        }
      }
      break;
    }

    case LSM_CONFIG_BLOCK_SIZE: {
      /* This parameter is read and written in KB. But all internal 
      ** processing is done in bytes.  */
      int *piVal = va_arg(ap, int *);
      if( pDb->pDatabase ){
        /* If lsm_open() has been called, this is a read-only parameter. 
        ** Set the output variable to the block-size in KB according to the 
        ** FileSystem object.  */
        *piVal = lsmFsBlockSize(pDb->pFS) / 1024;
      }else{
        int iVal = *piVal;
        if( iVal>=64 && iVal<=65536 && ((iVal-1) & iVal)==0 ){
          pDb->nDfltBlksz = iVal * 1024;
        }else{
          *piVal = pDb->nDfltBlksz / 1024;
        }
      }
      break;
    }

    case LSM_CONFIG_SAFETY: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>=0 && *piVal<=2 ){
        pDb->eSafety = *piVal;
      }
      *piVal = pDb->eSafety;
      break;
    }

    case LSM_CONFIG_MMAP: {
      int *piVal = va_arg(ap, int *);
      if( pDb->iReader<0 && *piVal>=0 ){
        pDb->iMmap = *piVal;
        rc = lsmFsConfigure(pDb);
      }
      *piVal = pDb->iMmap;
      break;
    }

    case LSM_CONFIG_USE_LOG: {
      int *piVal = va_arg(ap, int *);
      if( pDb->nTransOpen==0 && (*piVal==0 || *piVal==1) ){
        pDb->bUseLog = *piVal;
      }
      *piVal = pDb->bUseLog;
      break;
    }

    case LSM_CONFIG_AUTOMERGE: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>1 ) pDb->nMerge = *piVal;
      *piVal = pDb->nMerge;
      break;
    }

    case LSM_CONFIG_MAX_FREELIST: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>=2 && *piVal<=LSM_MAX_FREELIST_ENTRIES ){
        pDb->nMaxFreelist = *piVal;
      }
      *piVal = pDb->nMaxFreelist;
      break;
    }

    case LSM_CONFIG_MULTIPLE_PROCESSES: {
      int *piVal = va_arg(ap, int *);
      if( pDb->pDatabase ){
        /* If lsm_open() has been called, this is a read-only parameter. 
        ** Set the output variable to true if this connection is currently
        ** in multi-process mode.  */
        *piVal = lsmDbMultiProc(pDb);
      }else{
        pDb->bMultiProc = *piVal = (*piVal!=0);
      }
      break;
    }

    case LSM_CONFIG_READONLY: {
      int *piVal = va_arg(ap, int *);
      /* If lsm_open() has been called, this is a read-only parameter. */
      if( pDb->pDatabase==0 && *piVal>=0 ){
        pDb->bReadonly = *piVal = (*piVal!=0);
      }
      *piVal = pDb->bReadonly;
      break;
    }

    case LSM_CONFIG_SET_COMPRESSION: {
      lsm_compress *p = va_arg(ap, lsm_compress *);
      if( pDb->iReader>=0 && pDb->bInFactory==0 ){
        /* May not change compression schemes with an open transaction */
        rc = LSM_MISUSE_BKPT;
      }else{
        if( pDb->compress.xFree ){
          /* Invoke any destructor belonging to the current compression. */
          pDb->compress.xFree(pDb->compress.pCtx);
        }
        if( p->xBound==0 ){
          memset(&pDb->compress, 0, sizeof(lsm_compress));
          pDb->compress.iId = LSM_COMPRESSION_NONE;
        }else{
          memcpy(&pDb->compress, p, sizeof(lsm_compress));
        }
        rc = lsmFsConfigure(pDb);
      }
      break;
    }

    case LSM_CONFIG_SET_COMPRESSION_FACTORY: {
      lsm_compress_factory *p = va_arg(ap, lsm_compress_factory *);
      if( pDb->factory.xFree ){
        /* Invoke any destructor belonging to the current factory. */
        pDb->factory.xFree(pDb->factory.pCtx);
      }
      memcpy(&pDb->factory, p, sizeof(lsm_compress_factory));
      break;
    }

    case LSM_CONFIG_GET_COMPRESSION: {
      lsm_compress *p = va_arg(ap, lsm_compress *);
      memcpy(p, &pDb->compress, sizeof(lsm_compress));
      break;
    }

    default:
      rc = LSM_MISUSE;
      break;
  }

  va_end(ap);
  return rc;
}

static void lsmAppendSegmentList(LsmString *pStr, char *zPre, Segment *pSeg){
  lsmStringAppendf(pStr, "%s{%lld %lld %lld %lld}", zPre, 
        pSeg->iFirst, pSeg->iLastPg, pSeg->iRoot, pSeg->nSize
  );
}

static int infoGetWorker(lsm_db *pDb, Snapshot **pp, int *pbUnlock){
  int rc = LSM_OK;

  assert( *pbUnlock==0 );
  if( !pDb->pWorker ){
    rc = lsmBeginWork(pDb);
    if( rc!=LSM_OK ) return rc;
    *pbUnlock = 1;
  }
  if( pp ) *pp = pDb->pWorker;
  return rc;
}

static void infoFreeWorker(lsm_db *pDb, int bUnlock){
  if( bUnlock ){
    int rcdummy = LSM_BUSY;
    lsmFinishWork(pDb, 0, &rcdummy);
  }
}

static int lsmStructList(
  lsm_db *pDb,                    /* Database handle */
  char **pzOut                    /* OUT: Nul-terminated string (tcl list) */
){
  Level *pTopLevel = 0;           /* Top level of snapshot to report on */
  int rc = LSM_OK;
  Level *p;
  LsmString s;
  Snapshot *pWorker;              /* Worker snapshot */
  int bUnlock = 0;

  /* Obtain the worker snapshot */
  rc = infoGetWorker(pDb, &pWorker, &bUnlock);
  if( rc!=LSM_OK ) return rc;

  /* Format the contents of the snapshot as text */
  pTopLevel = lsmDbSnapshotLevel(pWorker);
  lsmStringInit(&s, pDb->pEnv);
  for(p=pTopLevel; rc==LSM_OK && p; p=p->pNext){
    int i;
    lsmStringAppendf(&s, "%s{%d", (s.n ? " " : ""), (int)p->iAge);
    lsmAppendSegmentList(&s, " ", &p->lhs);
    for(i=0; rc==LSM_OK && i<p->nRight; i++){
      lsmAppendSegmentList(&s, " ", &p->aRhs[i]);
    }
    lsmStringAppend(&s, "}", 1);
  }
  rc = s.n>=0 ? LSM_OK : LSM_NOMEM;

  /* Release the snapshot and return */
  infoFreeWorker(pDb, bUnlock);
  *pzOut = s.z;
  return rc;
}

static int infoFreelistCb(void *pCtx, int iBlk, i64 iSnapshot){
  LsmString *pStr = (LsmString *)pCtx;
  lsmStringAppendf(pStr, "%s{%d %lld}", (pStr->n?" ":""), iBlk, iSnapshot);
  return 0;
}

static int lsmInfoFreelist(lsm_db *pDb, char **pzOut){
  Snapshot *pWorker;              /* Worker snapshot */
  int bUnlock = 0;
  LsmString s;
  int rc;

  /* Obtain the worker snapshot */
  rc = infoGetWorker(pDb, &pWorker, &bUnlock);
  if( rc!=LSM_OK ) return rc;

  lsmStringInit(&s, pDb->pEnv);
  rc = lsmWalkFreelist(pDb, 0, infoFreelistCb, &s);
  if( rc!=LSM_OK ){
    lsmFree(pDb->pEnv, s.z);
  }else{
    *pzOut = s.z;
  }

  /* Release the snapshot and return */
  infoFreeWorker(pDb, bUnlock);
  return rc;
}

static int infoTreeSize(lsm_db *db, int *pnOldKB, int *pnNewKB){
  ShmHeader *pShm = db->pShmhdr;
  TreeHeader *p = &pShm->hdr1;

  /* The following code suffers from two race conditions, as it accesses and
  ** trusts the contents of shared memory without verifying checksums:
  **
  **   * The two values read - TreeHeader.root.nByte and oldroot.nByte - are 
  **     32-bit fields. It is assumed that reading from one of these
  **     is atomic - that it is not possible to read a partially written
  **     garbage value. However the two values may be mutually inconsistent. 
  **
  **   * TreeHeader.iLogOff is a 64-bit value. And lsmCheckpointLogOffset()
  **     reads a 64-bit value from a snapshot stored in shared memory. It
  **     is assumed that in each case it is possible to read a partially
  **     written garbage value. If this occurs, then the value returned
  **     for the size of the "old" tree may reflect the size of an "old"
  **     tree that was recently flushed to disk.
  **
  ** Given the context in which this function is called (as a result of an
  ** lsm_info(LSM_INFO_TREE_SIZE) request), neither of these are considered to
  ** be problems.
  */
  *pnNewKB = ((int)p->root.nByte + 1023) / 1024;
  if( p->iOldShmid ){
    if( p->iOldLog==lsmCheckpointLogOffset(pShm->aSnap1) ){
      *pnOldKB = 0;
    }else{
      *pnOldKB = ((int)p->oldroot.nByte + 1023) / 1024;
    }
  }else{
    *pnOldKB = 0;
  }

  return LSM_OK;
}

int lsm_info(lsm_db *pDb, int eParam, ...){
  int rc = LSM_OK;
  va_list ap;
  va_start(ap, eParam);

  switch( eParam ){
    case LSM_INFO_NWRITE: {
      int *piVal = va_arg(ap, int *);
      *piVal = lsmFsNWrite(pDb->pFS);
      break;
    }

    case LSM_INFO_NREAD: {
      int *piVal = va_arg(ap, int *);
      *piVal = lsmFsNRead(pDb->pFS);
      break;
    }

    case LSM_INFO_DB_STRUCTURE: {
      char **pzVal = va_arg(ap, char **);
      rc = lsmStructList(pDb, pzVal);
      break;
    }

    case LSM_INFO_ARRAY_STRUCTURE: {
      LsmPgno pgno = va_arg(ap, LsmPgno);
      char **pzVal = va_arg(ap, char **);
      rc = lsmInfoArrayStructure(pDb, 0, pgno, pzVal);
      break;
    }

    case LSM_INFO_ARRAY_PAGES: {
      LsmPgno pgno = va_arg(ap, LsmPgno);
      char **pzVal = va_arg(ap, char **);
      rc = lsmInfoArrayPages(pDb, pgno, pzVal);
      break;
    }

    case LSM_INFO_PAGE_HEX_DUMP:
    case LSM_INFO_PAGE_ASCII_DUMP: {
      LsmPgno pgno = va_arg(ap, LsmPgno);
      char **pzVal = va_arg(ap, char **);
      int bUnlock = 0;
      rc = infoGetWorker(pDb, 0, &bUnlock);
      if( rc==LSM_OK ){
        int bHex = (eParam==LSM_INFO_PAGE_HEX_DUMP);
        rc = lsmInfoPageDump(pDb, pgno, bHex, pzVal);
      }
      infoFreeWorker(pDb, bUnlock);
      break;
    }

    case LSM_INFO_LOG_STRUCTURE: {
      char **pzVal = va_arg(ap, char **);
      rc = lsmInfoLogStructure(pDb, pzVal);
      break;
    }

    case LSM_INFO_FREELIST: {
      char **pzVal = va_arg(ap, char **);
      rc = lsmInfoFreelist(pDb, pzVal);
      break;
    }

    case LSM_INFO_CHECKPOINT_SIZE: {
      int *pnKB = va_arg(ap, int *);
      rc = lsmCheckpointSize(pDb, pnKB);
      break;
    }

    case LSM_INFO_TREE_SIZE: {
      int *pnOld = va_arg(ap, int *);
      int *pnNew = va_arg(ap, int *);
      rc = infoTreeSize(pDb, pnOld, pnNew);
      break;
    }

    case LSM_INFO_COMPRESSION_ID: {
      unsigned int *piOut = va_arg(ap, unsigned int *);
      if( pDb->pClient ){
        *piOut = pDb->pClient->iCmpId;
      }else{
        rc = lsmInfoCompressionId(pDb, piOut);
      }
      break;
    }

    default:
      rc = LSM_MISUSE;
      break;
  }

  va_end(ap);
  return rc;
}

static int doWriteOp(
  lsm_db *pDb,
  int bDeleteRange,
  const void *pKey, int nKey,     /* Key to write or delete */
  const void *pVal, int nVal      /* Value to write. Or nVal==-1 for a delete */
){
  int rc = LSM_OK;                /* Return code */
  int bCommit = 0;                /* True to commit before returning */

  if( pDb->nTransOpen==0 ){
    bCommit = 1;
    rc = lsm_begin(pDb, 1);
  }

  if( rc==LSM_OK ){
    int eType = (bDeleteRange ? LSM_DRANGE : (nVal>=0?LSM_WRITE:LSM_DELETE));
    rc = lsmLogWrite(pDb, eType, (void *)pKey, nKey, (void *)pVal, nVal);
  }

  lsmSortedSaveTreeCursors(pDb);

  if( rc==LSM_OK ){
    int pgsz = lsmFsPageSize(pDb->pFS);
    int nQuant = LSM_AUTOWORK_QUANT * pgsz;
    int nBefore;
    int nAfter;
    int nDiff;

    if( nQuant>pDb->nTreeLimit ){
      nQuant = LSM_MAX(pDb->nTreeLimit, pgsz);
    }

    nBefore = lsmTreeSize(pDb);
    if( bDeleteRange ){
      rc = lsmTreeDelete(pDb, (void *)pKey, nKey, (void *)pVal, nVal);
    }else{
      rc = lsmTreeInsert(pDb, (void *)pKey, nKey, (void *)pVal, nVal);
    }

    nAfter = lsmTreeSize(pDb);
    nDiff = (nAfter/nQuant) - (nBefore/nQuant);
    if( rc==LSM_OK && pDb->bAutowork && nDiff!=0 ){
      rc = lsmSortedAutoWork(pDb, nDiff * LSM_AUTOWORK_QUANT);
    }
  }

  /* If a transaction was opened at the start of this function, commit it. 
  ** Or, if an error has occurred, roll it back.  */
  if( bCommit ){
    if( rc==LSM_OK ){
      rc = lsm_commit(pDb, 0);
    }else{
      lsm_rollback(pDb, 0);
    }
  }

  return rc;
}

/* 
** Write a new value into the database.
*/
int lsm_insert(
  lsm_db *db,                     /* Database connection */
  const void *pKey, int nKey,     /* Key to write or delete */
  const void *pVal, int nVal      /* Value to write. Or nVal==-1 for a delete */
){
  return doWriteOp(db, 0, pKey, nKey, pVal, nVal);
}

/*
** Delete a value from the database. 
*/
int lsm_delete(lsm_db *db, const void *pKey, int nKey){
  return doWriteOp(db, 0, pKey, nKey, 0, -1);
}

/*
** Delete a range of database keys.
*/
int lsm_delete_range(
  lsm_db *db,                     /* Database handle */
  const void *pKey1, int nKey1,   /* Lower bound of range to delete */
  const void *pKey2, int nKey2    /* Upper bound of range to delete */
){
  int rc = LSM_OK;
  if( db->xCmp((void *)pKey1, nKey1, (void *)pKey2, nKey2)<0 ){
    rc = doWriteOp(db, 1, pKey1, nKey1, pKey2, nKey2);
  }
  return rc;
}

/*
** Open a new cursor handle. 
**
** If there are currently no other open cursor handles, and no open write
** transaction, open a read transaction here.
*/
int lsm_csr_open(lsm_db *pDb, lsm_cursor **ppCsr){
  int rc = LSM_OK;                /* Return code */
  MultiCursor *pCsr = 0;          /* New cursor object */

  /* Open a read transaction if one is not already open. */
  assert_db_state(pDb);

  if( pDb->pShmhdr==0 ){
    assert( pDb->bReadonly );
    rc = lsmBeginRoTrans(pDb);
  }else if( pDb->iReader<0 ){
    rc = lsmBeginReadTrans(pDb);
  }

  /* Allocate the multi-cursor. */
  if( rc==LSM_OK ){
    rc = lsmMCursorNew(pDb, &pCsr);
  }

  /* If an error has occured, set the output to NULL and delete any partially
  ** allocated cursor. If this means there are no open cursors, release the
  ** client snapshot.  */
  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr, 0);
    dbReleaseClientSnapshot(pDb);
  }

  assert_db_state(pDb);
  *ppCsr = (lsm_cursor *)pCsr;
  return rc;
}

/*
** Close a cursor opened using lsm_csr_open().
*/
int lsm_csr_close(lsm_cursor *p){
  if( p ){
    lsm_db *pDb = lsmMCursorDb((MultiCursor *)p);
    assert_db_state(pDb);
    lsmMCursorClose((MultiCursor *)p, 1);
    dbReleaseClientSnapshot(pDb);
    assert_db_state(pDb);
  }
  return LSM_OK;
}

/*
** Attempt to seek the cursor to the database entry specified by pKey/nKey.
** If an error occurs (e.g. an OOM or IO error), return an LSM error code.
** Otherwise, return LSM_OK.
*/
int lsm_csr_seek(lsm_cursor *pCsr, const void *pKey, int nKey, int eSeek){
  return lsmMCursorSeek((MultiCursor *)pCsr, 0, (void *)pKey, nKey, eSeek);
}

int lsm_csr_next(lsm_cursor *pCsr){
  return lsmMCursorNext((MultiCursor *)pCsr);
}

int lsm_csr_prev(lsm_cursor *pCsr){
  return lsmMCursorPrev((MultiCursor *)pCsr);
}

int lsm_csr_first(lsm_cursor *pCsr){
  return lsmMCursorFirst((MultiCursor *)pCsr);
}

int lsm_csr_last(lsm_cursor *pCsr){
  return lsmMCursorLast((MultiCursor *)pCsr);
}

int lsm_csr_valid(lsm_cursor *pCsr){
  return lsmMCursorValid((MultiCursor *)pCsr);
}

int lsm_csr_key(lsm_cursor *pCsr, const void **ppKey, int *pnKey){
  return lsmMCursorKey((MultiCursor *)pCsr, (void **)ppKey, pnKey);
}

int lsm_csr_value(lsm_cursor *pCsr, const void **ppVal, int *pnVal){
  return lsmMCursorValue((MultiCursor *)pCsr, (void **)ppVal, pnVal);
}

void lsm_config_log(
  lsm_db *pDb, 
  void (*xLog)(void *, int, const char *), 
  void *pCtx
){
  pDb->xLog = xLog;
  pDb->pLogCtx = pCtx;
}

void lsm_config_work_hook(
  lsm_db *pDb, 
  void (*xWork)(lsm_db *, void *), 
  void *pCtx
){
  pDb->xWork = xWork;
  pDb->pWorkCtx = pCtx;
}

static void lsmLogMessage(lsm_db *pDb, int rc, const char *zFormat, ...){
  if( pDb->xLog ){
    LsmString s;
    va_list ap, ap2;
    lsmStringInit(&s, pDb->pEnv);
    va_start(ap, zFormat);
    va_start(ap2, zFormat);
    lsmStringVAppendf(&s, zFormat, ap, ap2);
    va_end(ap);
    va_end(ap2);
    pDb->xLog(pDb->pLogCtx, rc, s.z);
    lsmStringClear(&s);
  }
}

int lsm_begin(lsm_db *pDb, int iLevel){
  int rc;

  assert_db_state( pDb );
  rc = (pDb->bReadonly ? LSM_READONLY : LSM_OK);

  /* A value less than zero means open one more transaction. */
  if( iLevel<0 ) iLevel = pDb->nTransOpen + 1;
  if( iLevel>pDb->nTransOpen ){
    int i;

    /* Extend the pDb->aTrans[] array if required. */
    if( rc==LSM_OK && pDb->nTransAlloc<iLevel ){
      TransMark *aNew;            /* New allocation */
      int nByte = sizeof(TransMark) * (iLevel+1);
      aNew = (TransMark *)lsmRealloc(pDb->pEnv, pDb->aTrans, nByte);
      if( !aNew ){
        rc = LSM_NOMEM;
      }else{
        nByte = sizeof(TransMark) * (iLevel+1 - pDb->nTransAlloc);
        memset(&aNew[pDb->nTransAlloc], 0, nByte);
        pDb->nTransAlloc = iLevel+1;
        pDb->aTrans = aNew;
      }
    }

    if( rc==LSM_OK && pDb->nTransOpen==0 ){
      rc = lsmBeginWriteTrans(pDb);
    }

    if( rc==LSM_OK ){
      for(i=pDb->nTransOpen; i<iLevel; i++){
        lsmTreeMark(pDb, &pDb->aTrans[i].tree);
        lsmLogTell(pDb, &pDb->aTrans[i].log);
      }
      pDb->nTransOpen = iLevel;
    }
  }

  return rc;
}

int lsm_commit(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;

  assert_db_state( pDb );

  /* A value less than zero means close the innermost nested transaction. */
  if( iLevel<0 ) iLevel = LSM_MAX(0, pDb->nTransOpen - 1);

  if( iLevel<pDb->nTransOpen ){
    if( iLevel==0 ){
      int rc2;
      /* Commit the transaction to disk. */
      if( rc==LSM_OK ) rc = lsmLogCommit(pDb);
      if( rc==LSM_OK && pDb->eSafety==LSM_SAFETY_FULL ){
        rc = lsmFsSyncLog(pDb->pFS);
      }
      rc2 = lsmFinishWriteTrans(pDb, (rc==LSM_OK));
      if( rc==LSM_OK ) rc = rc2;
    }
    pDb->nTransOpen = iLevel;
  }
  dbReleaseClientSnapshot(pDb);
  return rc;
}

int lsm_rollback(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;
  assert_db_state( pDb );

  if( pDb->nTransOpen ){
    /* A value less than zero means close the innermost nested transaction. */
    if( iLevel<0 ) iLevel = LSM_MAX(0, pDb->nTransOpen - 1);

    if( iLevel<=pDb->nTransOpen ){
      TransMark *pMark = &pDb->aTrans[(iLevel==0 ? 0 : iLevel-1)];
      lsmTreeRollback(pDb, &pMark->tree);
      if( iLevel ) lsmLogSeek(pDb, &pMark->log);
      pDb->nTransOpen = iLevel;
    }

    if( pDb->nTransOpen==0 ){
      lsmFinishWriteTrans(pDb, 0);
    }
    dbReleaseClientSnapshot(pDb);
  }

  return rc;
}

int lsm_get_user_version(lsm_db *pDb, unsigned int *piUsr){
  int rc = LSM_OK;                /* Return code */

  /* Open a read transaction if one is not already open. */
  assert_db_state(pDb);
  if( pDb->pShmhdr==0 ){
    assert( pDb->bReadonly );
    rc = lsmBeginRoTrans(pDb);
  }else if( pDb->iReader<0 ){
    rc = lsmBeginReadTrans(pDb);
  }

  /* Allocate the multi-cursor. */
  if( rc==LSM_OK ){
    *piUsr = pDb->treehdr.iUsrVersion;
  }

  dbReleaseClientSnapshot(pDb);
  assert_db_state(pDb);
  return rc;
}

int lsm_set_user_version(lsm_db *pDb, unsigned int iUsr){
  int rc = LSM_OK;                /* Return code */
  int bCommit = 0;                /* True to commit before returning */

  if( pDb->nTransOpen==0 ){
    bCommit = 1;
    rc = lsm_begin(pDb, 1);
  }

  if( rc==LSM_OK ){
    pDb->treehdr.iUsrVersion = iUsr;
  }

  /* If a transaction was opened at the start of this function, commit it. 
  ** Or, if an error has occurred, roll it back.  */
  if( bCommit ){
    if( rc==LSM_OK ){
      rc = lsm_commit(pDb, 0);
    }else{
      lsm_rollback(pDb, 0);
    }
  }

  return rc;
}

#line 1 "lsm_mem.c"
/*
** 2011-08-18
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Helper routines for memory allocation.
*/
/* #include "lsmInt.h" */

/*
** The following routines are called internally by LSM sub-routines. In
** this case a valid environment pointer must be supplied.
*/
static void *lsmMalloc(lsm_env *pEnv, size_t N){
  assert( pEnv );
  return pEnv->xMalloc(pEnv, N);
}
static void lsmFree(lsm_env *pEnv, void *p){
  assert( pEnv );
  pEnv->xFree(pEnv, p);
}
static void *lsmRealloc(lsm_env *pEnv, void *p, size_t N){
  assert( pEnv );
  return pEnv->xRealloc(pEnv, p, N);
}

/*
** Core memory allocation routines for LSM.
*/
void *lsm_malloc(lsm_env *pEnv, size_t N){
  return lsmMalloc(pEnv ? pEnv : lsm_default_env(), N);
}
void lsm_free(lsm_env *pEnv, void *p){
  lsmFree(pEnv ? pEnv : lsm_default_env(), p);
}
void *lsm_realloc(lsm_env *pEnv, void *p, size_t N){
  return lsmRealloc(pEnv ? pEnv : lsm_default_env(), p, N);
}

static void *lsmMallocZero(lsm_env *pEnv, size_t N){
  void *pRet;
  assert( pEnv );
  pRet = lsmMalloc(pEnv, N);
  if( pRet ) memset(pRet, 0, N);
  return pRet;
}

static void *lsmMallocRc(lsm_env *pEnv, size_t N, int *pRc){
  void *pRet = 0;
  if( *pRc==LSM_OK ){
    pRet = lsmMalloc(pEnv, N);
    if( pRet==0 ){
      *pRc = LSM_NOMEM_BKPT;
    }
  }
  return pRet;
}

static void *lsmMallocZeroRc(lsm_env *pEnv, size_t N, int *pRc){
  void *pRet = 0;
  if( *pRc==LSM_OK ){
    pRet = lsmMallocZero(pEnv, N);
    if( pRet==0 ){
      *pRc = LSM_NOMEM_BKPT;
    }
  }
  return pRet;
}

static void *lsmReallocOrFree(lsm_env *pEnv, void *p, size_t N){
  void *pNew;
  pNew = lsm_realloc(pEnv, p, N);
  if( !pNew ) lsm_free(pEnv, p);
  return pNew;
}

static void *lsmReallocOrFreeRc(lsm_env *pEnv, void *p, size_t N, int *pRc){
  void *pRet = 0;
  if( *pRc ){
    lsmFree(pEnv, p);
  }else{
    pRet = lsmReallocOrFree(pEnv, p, N);
    if( !pRet ) *pRc = LSM_NOMEM_BKPT;
  }
  return pRet;
}

static char *lsmMallocStrdup(lsm_env *pEnv, const char *zIn){
  int nByte;
  char *zRet;
  nByte = strlen(zIn);
  zRet = lsmMalloc(pEnv, nByte+1);
  if( zRet ){
    memcpy(zRet, zIn, nByte+1);
  }
  return zRet;
}

#line 1 "lsm_mutex.c"
/*
** 2012-01-30
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Mutex functions for LSM.
*/
/* #include "lsmInt.h" */

/*
** Allocate a new mutex.
*/
static int lsmMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  return pEnv->xMutexNew(pEnv, ppNew);
}

/*
** Return a handle for one of the static mutexes.
*/
static int lsmMutexStatic(lsm_env *pEnv, int iMutex, lsm_mutex **ppStatic){
  return pEnv->xMutexStatic(pEnv, iMutex, ppStatic);
}

/*
** Free a mutex allocated by lsmMutexNew().
*/
static void lsmMutexDel(lsm_env *pEnv, lsm_mutex *pMutex){
  if( pMutex ) pEnv->xMutexDel(pMutex);
}

/*
** Enter a mutex.
*/
static void lsmMutexEnter(lsm_env *pEnv, lsm_mutex *pMutex){
  pEnv->xMutexEnter(pMutex);
}

/*
** Attempt to enter a mutex, but do not block. If successful, return zero.
** Otherwise, if the mutex is already held by some other thread and is not
** entered, return non zero.
**
** Each successful call to this function must be matched by a call to
** lsmMutexLeave().
*/
static int lsmMutexTry(lsm_env *pEnv, lsm_mutex *pMutex){
  return pEnv->xMutexTry(pMutex);
}

/*
** Leave a mutex.
*/
static void lsmMutexLeave(lsm_env *pEnv, lsm_mutex *pMutex){
  pEnv->xMutexLeave(pMutex);
}

#ifndef NDEBUG
/*
** Return non-zero if the mutex passed as the second argument is held
** by the calling thread, or zero otherwise. If the implementation is not 
** able to tell if the mutex is held by the caller, it should return
** non-zero.
**
** This function is only used as part of assert() statements.
*/
static int lsmMutexHeld(lsm_env *pEnv, lsm_mutex *pMutex){
  return pEnv->xMutexHeld ? pEnv->xMutexHeld(pMutex) : 1;
}

/*
** Return non-zero if the mutex passed as the second argument is not 
** held by the calling thread, or zero otherwise. If the implementation 
** is not able to tell if the mutex is held by the caller, it should 
** return non-zero.
**
** This function is only used as part of assert() statements.
*/
static int lsmMutexNotHeld(lsm_env *pEnv, lsm_mutex *pMutex){
  return pEnv->xMutexNotHeld ? pEnv->xMutexNotHeld(pMutex) : 1;
}
#endif

#line 1 "lsm_shared.c"
/*
** 2012-01-23
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Utilities used to help multiple LSM clients to coexist within the
** same process space.
*/
/* #include "lsmInt.h" */

/*
** Global data. All global variables used by code in this file are grouped
** into the following structure instance.
**
** pDatabase:
**   Linked list of all Database objects allocated within this process.
**   This list may not be traversed without holding the global mutex (see
**   functions enterGlobalMutex() and leaveGlobalMutex()).
*/
static struct SharedData {
  Database *pDatabase;            /* Linked list of all Database objects */
} gShared;

/*
** Database structure. There is one such structure for each distinct 
** database accessed by this process. They are stored in the singly linked 
** list starting at global variable gShared.pDatabase. Database objects are 
** reference counted. Once the number of connections to the associated
** database drops to zero, they are removed from the linked list and deleted.
**
** pFile:
**   In multi-process mode, this file descriptor is used to obtain locks 
**   and to access shared-memory. In single process mode, its only job is
**   to hold the exclusive lock on the file.
**   
*/
struct Database {
  /* Protected by the global mutex (enterGlobalMutex/leaveGlobalMutex): */
  char *zName;                    /* Canonical path to database file */
  int nName;                      /* strlen(zName) */
  int nDbRef;                     /* Number of associated lsm_db handles */
  Database *pDbNext;              /* Next Database structure in global list */

  /* Protected by the local mutex (pClientMutex) */
  int bReadonly;                  /* True if Database.pFile is read-only */
  int bMultiProc;                 /* True if running in multi-process mode */
  lsm_file *pFile;                /* Used for locks/shm in multi-proc mode */
  LsmFile *pLsmFile;              /* List of deferred closes */
  lsm_mutex *pClientMutex;        /* Protects the apShmChunk[] and pConn */
  int nShmChunk;                  /* Number of entries in apShmChunk[] array */
  void **apShmChunk;              /* Array of "shared" memory regions */
  lsm_db *pConn;                  /* List of connections to this db. */
};

/*
** Functions to enter and leave the global mutex. This mutex is used
** to protect the global linked-list headed at gShared.pDatabase.
*/
static int enterGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  int rc = lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  if( rc==LSM_OK ) lsmMutexEnter(pEnv, p);
  return rc;
}
static void leaveGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  lsmMutexLeave(pEnv, p);
}

#ifdef LSM_DEBUG
static int holdingGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  return lsmMutexHeld(pEnv, p);
}
#endif

#if 0
static void assertNotInFreelist(Freelist *p, int iBlk){
  int i; 
  for(i=0; i<p->nEntry; i++){
    assert( p->aEntry[i].iBlk!=iBlk );
  }
}
#else
# define assertNotInFreelist(x,y)
#endif

/*
** Append an entry to the free-list. If (iId==-1), this is a delete.
*/
int freelistAppend(lsm_db *db, u32 iBlk, i64 iId){
  lsm_env *pEnv = db->pEnv;
  Freelist *p;
  int i; 

  assert( iId==-1 || iId>=0 );
  p = db->bUseFreelist ? db->pFreelist : &db->pWorker->freelist;

  /* Extend the space allocated for the freelist, if required */
  assert( p->nAlloc>=p->nEntry );
  if( p->nAlloc==p->nEntry ){
    int nNew; 
    int nByte; 
    FreelistEntry *aNew;

    nNew = (p->nAlloc==0 ? 4 : p->nAlloc*2);
    nByte = sizeof(FreelistEntry) * nNew;
    aNew = (FreelistEntry *)lsmRealloc(pEnv, p->aEntry, nByte);
    if( !aNew ) return LSM_NOMEM_BKPT;
    p->nAlloc = nNew;
    p->aEntry = aNew;
  }

  for(i=0; i<p->nEntry; i++){
    assert( i==0 || p->aEntry[i].iBlk > p->aEntry[i-1].iBlk );
    if( p->aEntry[i].iBlk>=iBlk ) break;
  }

  if( i<p->nEntry && p->aEntry[i].iBlk==iBlk ){
    /* Clobber an existing entry */
    p->aEntry[i].iId = iId;
  }else{
    /* Insert a new entry into the list */
    int nByte = sizeof(FreelistEntry)*(p->nEntry-i);
    memmove(&p->aEntry[i+1], &p->aEntry[i], nByte);
    p->aEntry[i].iBlk = iBlk;
    p->aEntry[i].iId = iId;
    p->nEntry++;
  }

  return LSM_OK;
}

/*
** This function frees all resources held by the Database structure passed
** as the only argument.
*/
static void freeDatabase(lsm_env *pEnv, Database *p){
  assert( holdingGlobalMutex(pEnv) );
  if( p ){
    /* Free the mutexes */
    lsmMutexDel(pEnv, p->pClientMutex);

    if( p->pFile ){
      lsmEnvClose(pEnv, p->pFile);
    }

    /* Free the array of shm pointers */
    lsmFree(pEnv, p->apShmChunk);

    /* Free the memory allocated for the Database struct itself */
    lsmFree(pEnv, p);
  }
}

typedef struct DbTruncateCtx DbTruncateCtx;
struct DbTruncateCtx {
  int nBlock;
  i64 iInUse;
};

static int dbTruncateCb(void *pCtx, int iBlk, i64 iSnapshot){
  DbTruncateCtx *p = (DbTruncateCtx *)pCtx;
  if( iBlk!=p->nBlock || (p->iInUse>=0 && iSnapshot>=p->iInUse) ) return 1;
  p->nBlock--;
  return 0;
}

static int dbTruncate(lsm_db *pDb, i64 iInUse){
  int rc = LSM_OK;
#if 0
  int i;
  DbTruncateCtx ctx;

  assert( pDb->pWorker );
  ctx.nBlock = pDb->pWorker->nBlock;
  ctx.iInUse = iInUse;

  rc = lsmWalkFreelist(pDb, 1, dbTruncateCb, (void *)&ctx);
  for(i=ctx.nBlock+1; rc==LSM_OK && i<=pDb->pWorker->nBlock; i++){
    rc = freelistAppend(pDb, i, -1);
  }

  if( rc==LSM_OK ){
#ifdef LSM_LOG_FREELIST
    if( ctx.nBlock!=pDb->pWorker->nBlock ){
      lsmLogMessage(pDb, 0, 
          "dbTruncate(): truncated db to %d blocks",ctx.nBlock
      );
    }
#endif
    pDb->pWorker->nBlock = ctx.nBlock;
  }
#endif
  return rc;
}


/*
** This function is called during database shutdown (when the number of
** connections drops from one to zero). It truncates the database file
** to as small a size as possible without truncating away any blocks that
** contain data.
*/
static int dbTruncateFile(lsm_db *pDb){
  int rc;

  assert( pDb->pWorker==0 );
  assert( lsmShmAssertLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_EXCL) );
  rc = lsmCheckpointLoadWorker(pDb);

  if( rc==LSM_OK ){
    DbTruncateCtx ctx;

    /* Walk the database free-block-list in reverse order. Set ctx.nBlock
    ** to the block number of the last block in the database that actually
    ** contains data. */
    ctx.nBlock = pDb->pWorker->nBlock;
    ctx.iInUse = -1;
    rc = lsmWalkFreelist(pDb, 1, dbTruncateCb, (void *)&ctx);

    /* If the last block that contains data is not already the last block in
    ** the database file, truncate the database file so that it is. */
    if( rc==LSM_OK ){
      rc = lsmFsTruncateDb(
          pDb->pFS, (i64)ctx.nBlock*lsmFsBlockSize(pDb->pFS)
      );
    }
  }

  lsmFreeSnapshot(pDb->pEnv, pDb->pWorker);
  pDb->pWorker = 0;
  return rc;
}

static void doDbDisconnect(lsm_db *pDb){
  int rc;

  if( pDb->bReadonly ){
    lsmShmLock(pDb, LSM_LOCK_DMS3, LSM_LOCK_UNLOCK, 0);
  }else{
    /* Block for an exclusive lock on DMS1. This lock serializes all calls
    ** to doDbConnect() and doDbDisconnect() across all processes.  */
    rc = lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_EXCL, 1);
    if( rc==LSM_OK ){

      lsmShmLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_UNLOCK, 0);

      /* Try an exclusive lock on DMS2. If successful, this is the last
      ** connection to the database. In this case flush the contents of the
      ** in-memory tree to disk and write a checkpoint.  */
      rc = lsmShmTestLock(pDb, LSM_LOCK_DMS2, 1, LSM_LOCK_EXCL);
      if( rc==LSM_OK ){
        rc = lsmShmTestLock(pDb, LSM_LOCK_CHECKPOINTER, 1, LSM_LOCK_EXCL);
      }
      if( rc==LSM_OK ){
        int bReadonly = 0;        /* True if there exist read-only conns. */

        /* Flush the in-memory tree, if required. If there is data to flush,
        ** this will create a new client snapshot in Database.pClient. The
        ** checkpoint (serialization) of this snapshot may be written to disk
        ** by the following block.  
        **
        ** There is no need to take a WRITER lock here. That there are no 
        ** other locks on DMS2 guarantees that there are no other read-write
        ** connections at this time (and the lock on DMS1 guarantees that
        ** no new ones may appear).
        */
        rc = lsmTreeLoadHeader(pDb, 0);
        if( rc==LSM_OK && (lsmTreeHasOld(pDb) || lsmTreeSize(pDb)>0) ){
          rc = lsmFlushTreeToDisk(pDb);
        }

        /* Now check if there are any read-only connections. If there are,
        ** then do not truncate the db file or unlink the shared-memory 
        ** region.  */
        if( rc==LSM_OK ){
          rc = lsmShmTestLock(pDb, LSM_LOCK_DMS3, 1, LSM_LOCK_EXCL);
          if( rc==LSM_BUSY ){
            bReadonly = 1;
            rc = LSM_OK;
          }
        }

        /* Write a checkpoint to disk. */
        if( rc==LSM_OK ){
          rc = lsmCheckpointWrite(pDb, 0);
        }

        /* If the checkpoint was written successfully, delete the log file
        ** and, if possible, truncate the database file.  */
        if( rc==LSM_OK ){
          int bRotrans = 0;
          Database *p = pDb->pDatabase;

          /* The log file may only be deleted if there are no clients 
          ** read-only clients running rotrans transactions.  */
          rc = lsmDetectRoTrans(pDb, &bRotrans);
          if( rc==LSM_OK && bRotrans==0 ){
            lsmFsCloseAndDeleteLog(pDb->pFS);
          }

          /* The database may only be truncated if there exist no read-only
          ** clients - either connected or running rotrans transactions. */
          if( bReadonly==0 && bRotrans==0 ){
            lsmFsUnmap(pDb->pFS);
            dbTruncateFile(pDb);
            if( p->pFile && p->bMultiProc ){
              lsmEnvShmUnmap(pDb->pEnv, p->pFile, 1);
            }
          }
        }
      }
    }

    if( pDb->iRwclient>=0 ){
      lsmShmLock(pDb, LSM_LOCK_RWCLIENT(pDb->iRwclient), LSM_LOCK_UNLOCK, 0);
      pDb->iRwclient = -1;
    }

    lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_UNLOCK, 0);
  }
  pDb->pShmhdr = 0;
}

static int doDbConnect(lsm_db *pDb){
  const int nUsMax = 100000;      /* Max value for nUs */
  int nUs = 1000;                 /* us to wait between DMS1 attempts */
  int rc;

  /* Obtain a pointer to the shared-memory header */
  assert( pDb->pShmhdr==0 );
  assert( pDb->bReadonly==0 );

  /* Block for an exclusive lock on DMS1. This lock serializes all calls
  ** to doDbConnect() and doDbDisconnect() across all processes.  */
  while( 1 ){
    rc = lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_EXCL, 1);
    if( rc!=LSM_BUSY ) break;
    lsmEnvSleep(pDb->pEnv, nUs);
    nUs = nUs * 2;
    if( nUs>nUsMax ) nUs = nUsMax;
  }
  if( rc==LSM_OK ){
    rc = lsmShmCacheChunks(pDb, 1);
  }
  if( rc!=LSM_OK ) return rc;
  pDb->pShmhdr = (ShmHeader *)pDb->apShm[0];

  /* Try an exclusive lock on DMS2/DMS3. If successful, this is the first 
  ** and only connection to the database. In this case initialize the 
  ** shared-memory and run log file recovery.  */
  assert( LSM_LOCK_DMS3==1+LSM_LOCK_DMS2 );
  rc = lsmShmTestLock(pDb, LSM_LOCK_DMS2, 2, LSM_LOCK_EXCL);
  if( rc==LSM_OK ){
    memset(pDb->pShmhdr, 0, sizeof(ShmHeader));
    rc = lsmCheckpointRecover(pDb);
    if( rc==LSM_OK ){
      rc = lsmLogRecover(pDb);
    }
    if( rc==LSM_OK ){
      ShmHeader *pShm = pDb->pShmhdr;
      pShm->aReader[0].iLsmId = lsmCheckpointId(pShm->aSnap1, 0);
      pShm->aReader[0].iTreeId = pDb->treehdr.iUsedShmid;
    }
  }else if( rc==LSM_BUSY ){
    rc = LSM_OK;
  }

  /* Take a shared lock on DMS2. In multi-process mode this lock "cannot" 
  ** fail, as connections may only hold an exclusive lock on DMS2 if they 
  ** first hold an exclusive lock on DMS1. And this connection is currently 
  ** holding the exclusive lock on DSM1. 
  **
  ** However, if some other connection has the database open in single-process
  ** mode, this operation will fail. In this case, return the error to the
  ** caller - the attempt to connect to the db has failed.
  */
  if( rc==LSM_OK ){
    rc = lsmShmLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_SHARED, 0);
  }

  /* If anything went wrong, unlock DMS2. Otherwise, try to take an exclusive
  ** lock on one of the LSM_LOCK_RWCLIENT() locks. Unlock DMS1 in any case. */
  if( rc!=LSM_OK ){
    pDb->pShmhdr = 0;
  }else{
    int i;
    for(i=0; i<LSM_LOCK_NRWCLIENT; i++){
      int rc2 = lsmShmLock(pDb, LSM_LOCK_RWCLIENT(i), LSM_LOCK_EXCL, 0);
      if( rc2==LSM_OK ) pDb->iRwclient = i;
      if( rc2!=LSM_BUSY ){
        rc = rc2;
        break;
      }
    }
  }
  lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_UNLOCK, 0);

  return rc;
}

static int dbOpenSharedFd(lsm_env *pEnv, Database *p, int bRoOk){
  int rc;

  rc = lsmEnvOpen(pEnv, p->zName, 0, &p->pFile);
  if( rc==LSM_IOERR && bRoOk ){
    rc = lsmEnvOpen(pEnv, p->zName, LSM_OPEN_READONLY, &p->pFile);
    p->bReadonly = 1;
  }

  return rc;
}

/*
** Return a reference to the shared Database handle for the database 
** identified by canonical path zName. If this is the first connection to
** the named database, a new Database object is allocated. Otherwise, a
** pointer to an existing object is returned.
**
** If successful, *ppDatabase is set to point to the shared Database 
** structure and LSM_OK returned. Otherwise, *ppDatabase is set to NULL
** and and LSM error code returned.
**
** Each successful call to this function should be (eventually) matched
** by a call to lsmDbDatabaseRelease().
*/
static int lsmDbDatabaseConnect(
  lsm_db *pDb,                    /* Database handle */
  const char *zName               /* Full-path to db file */
){
  lsm_env *pEnv = pDb->pEnv;
  int rc;                         /* Return code */
  Database *p = 0;                /* Pointer returned via *ppDatabase */
  int nName = lsmStrlen(zName);

  assert( pDb->pDatabase==0 );
  rc = enterGlobalMutex(pEnv);
  if( rc==LSM_OK ){

    /* Search the global list for an existing object. TODO: Need something
    ** better than the memcmp() below to figure out if a given Database
    ** object represents the requested file.  */
    for(p=gShared.pDatabase; p; p=p->pDbNext){
      if( nName==p->nName && 0==memcmp(zName, p->zName, nName) ) break;
    }

    /* If no suitable Database object was found, allocate a new one. */
    if( p==0 ){
      p = (Database *)lsmMallocZeroRc(pEnv, sizeof(Database)+nName+1, &rc);

      /* If the allocation was successful, fill in other fields and
      ** allocate the client mutex. */ 
      if( rc==LSM_OK ){
        p->bMultiProc = pDb->bMultiProc;
        p->zName = (char *)&p[1];
        p->nName = nName;
        memcpy((void *)p->zName, zName, nName+1);
        rc = lsmMutexNew(pEnv, &p->pClientMutex);
      }

      /* If nothing has gone wrong so far, open the shared fd. And if that
      ** succeeds and this connection requested single-process mode, 
      ** attempt to take the exclusive lock on DMS2.  */
      if( rc==LSM_OK ){
        int bReadonly = (pDb->bReadonly && pDb->bMultiProc);
        rc = dbOpenSharedFd(pDb->pEnv, p, bReadonly);
      }

      if( rc==LSM_OK && p->bMultiProc==0 ){
        /* Hold an exclusive lock DMS1 while grabbing DMS2. This ensures
        ** that any ongoing call to doDbDisconnect() (even one in another
        ** process) is finished before proceeding.  */
        assert( p->bReadonly==0 );
        rc = lsmEnvLock(pDb->pEnv, p->pFile, LSM_LOCK_DMS1, LSM_LOCK_EXCL);
        if( rc==LSM_OK ){
          rc = lsmEnvLock(pDb->pEnv, p->pFile, LSM_LOCK_DMS2, LSM_LOCK_EXCL);
          lsmEnvLock(pDb->pEnv, p->pFile, LSM_LOCK_DMS1, LSM_LOCK_UNLOCK);
        }
      }

      if( rc==LSM_OK ){
        p->pDbNext = gShared.pDatabase;
        gShared.pDatabase = p;
      }else{
        freeDatabase(pEnv, p);
        p = 0;
      }
    }

    if( p ){
      p->nDbRef++;
    }
    leaveGlobalMutex(pEnv);

    if( p ){
      lsmMutexEnter(pDb->pEnv, p->pClientMutex);
      pDb->pNext = p->pConn;
      p->pConn = pDb;
      lsmMutexLeave(pDb->pEnv, p->pClientMutex);
    }
  }

  pDb->pDatabase = p;
  if( rc==LSM_OK ){
    assert( p );
    rc = lsmFsOpen(pDb, zName, p->bReadonly);
  }

  /* If the db handle is read-write, then connect to the system now. Run
  ** recovery as necessary. Or, if this is a read-only database handle,
  ** defer attempting to connect to the system until a read-transaction
  ** is opened.  */
  if( rc==LSM_OK ){
    rc = lsmFsConfigure(pDb);
  }
  if( rc==LSM_OK && pDb->bReadonly==0 ){
    rc = doDbConnect(pDb);
  }

  return rc;
}

static void dbDeferClose(lsm_db *pDb){
  if( pDb->pFS ){
    LsmFile *pLsmFile;
    Database *p = pDb->pDatabase;
    pLsmFile = lsmFsDeferClose(pDb->pFS);
    pLsmFile->pNext = p->pLsmFile;
    p->pLsmFile = pLsmFile;
  }
}

static LsmFile *lsmDbRecycleFd(lsm_db *db){
  LsmFile *pRet;
  Database *p = db->pDatabase;
  lsmMutexEnter(db->pEnv, p->pClientMutex);
  if( (pRet = p->pLsmFile)!=0 ){
    p->pLsmFile = pRet->pNext;
  }
  lsmMutexLeave(db->pEnv, p->pClientMutex);
  return pRet;
}

/*
** Release a reference to a Database object obtained from 
** lsmDbDatabaseConnect(). There should be exactly one call to this function 
** for each successful call to Find().
*/
static void lsmDbDatabaseRelease(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  if( p ){
    lsm_db **ppDb;

    if( pDb->pShmhdr ){
      doDbDisconnect(pDb);
    }

    lsmFsUnmap(pDb->pFS);
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    for(ppDb=&p->pConn; *ppDb!=pDb; ppDb=&((*ppDb)->pNext));
    *ppDb = pDb->pNext;
    dbDeferClose(pDb);
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);

    enterGlobalMutex(pDb->pEnv);
    p->nDbRef--;
    if( p->nDbRef==0 ){
      LsmFile *pIter;
      LsmFile *pNext;
      Database **pp;

      /* Remove the Database structure from the linked list. */
      for(pp=&gShared.pDatabase; *pp!=p; pp=&((*pp)->pDbNext));
      *pp = p->pDbNext;

      /* If they were allocated from the heap, free the shared memory chunks */
      if( p->bMultiProc==0 ){
        int i;
        for(i=0; i<p->nShmChunk; i++){
          lsmFree(pDb->pEnv, p->apShmChunk[i]);
        }
      }

      /* Close any outstanding file descriptors */
      for(pIter=p->pLsmFile; pIter; pIter=pNext){
        pNext = pIter->pNext;
        lsmEnvClose(pDb->pEnv, pIter->pFile);
        lsmFree(pDb->pEnv, pIter);
      }
      freeDatabase(pDb->pEnv, p);
    }
    leaveGlobalMutex(pDb->pEnv);
  }
}

static Level *lsmDbSnapshotLevel(Snapshot *pSnapshot){
  return pSnapshot->pLevel;
}

static void lsmDbSnapshotSetLevel(Snapshot *pSnap, Level *pLevel){
  pSnap->pLevel = pLevel;
}

/* TODO: Shuffle things around to get rid of this */
static int firstSnapshotInUse(lsm_db *, i64 *);

/* 
** Context object used by the lsmWalkFreelist() utility. 
*/
typedef struct WalkFreelistCtx WalkFreelistCtx;
struct WalkFreelistCtx {
  lsm_db *pDb;
  int bReverse;
  Freelist *pFreelist;
  int iFree;
  int (*xUsr)(void *, int, i64);  /* User callback function */
  void *pUsrctx;                  /* User callback context */
  int bDone;                      /* Set to true after xUsr() returns true */
};

/* 
** Callback used by lsmWalkFreelist().
*/
static int walkFreelistCb(void *pCtx, int iBlk, i64 iSnapshot){
  WalkFreelistCtx *p = (WalkFreelistCtx *)pCtx;
  const int iDir = (p->bReverse ? -1 : 1);
  Freelist *pFree = p->pFreelist;

  assert( p->bDone==0 );
  assert( iBlk>=0 );
  if( pFree ){
    while( (p->iFree < pFree->nEntry) && p->iFree>=0 ){
      FreelistEntry *pEntry = &pFree->aEntry[p->iFree];
      if( (p->bReverse==0 && pEntry->iBlk>(u32)iBlk)
       || (p->bReverse!=0 && pEntry->iBlk<(u32)iBlk)
      ){
        break;
      }else{
        p->iFree += iDir;
        if( pEntry->iId>=0 
            && p->xUsr(p->pUsrctx, pEntry->iBlk, pEntry->iId) 
          ){
          p->bDone = 1;
          return 1;
        }
        if( pEntry->iBlk==(u32)iBlk ) return 0;
      }
    }
  }

  if( p->xUsr(p->pUsrctx, iBlk, iSnapshot) ){
    p->bDone = 1;
    return 1;
  }
  return 0;
}

/*
** The database handle passed as the first argument must be the worker
** connection. This function iterates through the contents of the current
** free block list, invoking the supplied callback once for each list
** element.
**
** The difference between this function and lsmSortedWalkFreelist() is
** that lsmSortedWalkFreelist() only considers those free-list elements
** stored within the LSM. This function also merges in any in-memory 
** elements.
*/
static int lsmWalkFreelist(
  lsm_db *pDb,                    /* Database handle (must be worker) */
  int bReverse,                   /* True to iterate from largest to smallest */
  int (*x)(void *, int, i64),     /* Callback function */
  void *pCtx                      /* First argument to pass to callback */
){
  const int iDir = (bReverse ? -1 : 1);
  int rc;
  int iCtx;

  WalkFreelistCtx ctx[2];

  ctx[0].pDb = pDb;
  ctx[0].bReverse = bReverse;
  ctx[0].pFreelist = &pDb->pWorker->freelist;
  if( ctx[0].pFreelist && bReverse ){
    ctx[0].iFree = ctx[0].pFreelist->nEntry-1;
  }else{
    ctx[0].iFree = 0;
  }
  ctx[0].xUsr = walkFreelistCb;
  ctx[0].pUsrctx = (void *)&ctx[1];
  ctx[0].bDone = 0;

  ctx[1].pDb = pDb;
  ctx[1].bReverse = bReverse;
  ctx[1].pFreelist = pDb->pFreelist;
  if( ctx[1].pFreelist && bReverse ){
    ctx[1].iFree = ctx[1].pFreelist->nEntry-1;
  }else{
    ctx[1].iFree = 0;
  }
  ctx[1].xUsr = x;
  ctx[1].pUsrctx = pCtx;
  ctx[1].bDone = 0;

  rc = lsmSortedWalkFreelist(pDb, bReverse, walkFreelistCb, (void *)&ctx[0]);

  if( ctx[0].bDone==0 ){
    for(iCtx=0; iCtx<2; iCtx++){
      int i;
      WalkFreelistCtx *p = &ctx[iCtx];
      for(i=p->iFree; 
          p->pFreelist && rc==LSM_OK && i<p->pFreelist->nEntry && i>=0;
          i += iDir
         ){
        FreelistEntry *pEntry = &p->pFreelist->aEntry[i];
        if( pEntry->iId>=0 && p->xUsr(p->pUsrctx, pEntry->iBlk, pEntry->iId) ){
          return LSM_OK;
        }
      }
    }
  }

  return rc;
}


typedef struct FindFreeblockCtx FindFreeblockCtx;
struct FindFreeblockCtx {
  i64 iInUse;
  int iRet;
  int bNotOne;
};

static int findFreeblockCb(void *pCtx, int iBlk, i64 iSnapshot){
  FindFreeblockCtx *p = (FindFreeblockCtx *)pCtx;
  if( iSnapshot<p->iInUse && (iBlk!=1 || p->bNotOne==0) ){
    p->iRet = iBlk;
    return 1;
  }
  return 0;
}

static int findFreeblock(lsm_db *pDb, i64 iInUse, int bNotOne, int *piRet){
  int rc;                         /* Return code */
  FindFreeblockCtx ctx;           /* Context object */

  ctx.iInUse = iInUse;
  ctx.iRet = 0;
  ctx.bNotOne = bNotOne;
  rc = lsmWalkFreelist(pDb, 0, findFreeblockCb, (void *)&ctx);
  *piRet = ctx.iRet;

  return rc;
}

/*
** Allocate a new database file block to write data to, either by extending
** the database file or by recycling a free-list entry. The worker snapshot 
** must be held in order to call this function.
**
** If successful, *piBlk is set to the block number allocated and LSM_OK is
** returned. Otherwise, *piBlk is zeroed and an lsm error code returned.
*/
static int lsmBlockAllocate(lsm_db *pDb, int iBefore, int *piBlk){
  Snapshot *p = pDb->pWorker;
  int iRet = 0;                   /* Block number of allocated block */
  int rc = LSM_OK;
  i64 iInUse = 0;                 /* Snapshot id still in use */
  i64 iSynced = 0;                /* Snapshot id synced to disk */

  assert( p );

#ifdef LSM_LOG_FREELIST
  {
    static int nCall = 0;
    char *zFree = 0;
    nCall++;
    rc = lsmInfoFreelist(pDb, &zFree);
    if( rc!=LSM_OK ) return rc;
    lsmLogMessage(pDb, 0, "lsmBlockAllocate(): %d freelist: %s", nCall, zFree);
    lsmFree(pDb->pEnv, zFree);
  }
#endif

  /* Set iInUse to the smallest snapshot id that is either:
  **
  **   * Currently in use by a database client,
  **   * May be used by a database client in the future, or
  **   * Is the most recently checkpointed snapshot (i.e. the one that will
  **     be used following recovery if a failure occurs at this point).
  */
  rc = lsmCheckpointSynced(pDb, &iSynced, 0, 0);
  if( rc==LSM_OK && iSynced==0 ) iSynced = p->iId;
  iInUse = iSynced;
  if( rc==LSM_OK && pDb->iReader>=0 ){
    assert( pDb->pClient );
    iInUse = LSM_MIN(iInUse, pDb->pClient->iId);
  }
  if( rc==LSM_OK ) rc = firstSnapshotInUse(pDb, &iInUse);

#ifdef LSM_LOG_FREELIST
  {
    lsmLogMessage(pDb, 0, "lsmBlockAllocate(): "
        "snapshot-in-use: %lld (iSynced=%lld) (client-id=%lld)", 
        iInUse, iSynced, (pDb->iReader>=0 ? pDb->pClient->iId : 0)
    );
  }
#endif


  /* Unless there exists a read-only transaction (which prevents us from
  ** recycling any blocks regardless, query the free block list for a 
  ** suitable block to reuse. 
  **
  ** It might seem more natural to check for a read-only transaction at
  ** the start of this function. However, it is better do wait until after
  ** the call to lsmCheckpointSynced() to do so.
  */
  if( rc==LSM_OK ){
    int bRotrans;
    rc = lsmDetectRoTrans(pDb, &bRotrans);

    if( rc==LSM_OK && bRotrans==0 ){
      rc = findFreeblock(pDb, iInUse, (iBefore>0), &iRet);
    }
  }

  if( iBefore>0 && (iRet<=0 || iRet>=iBefore) ){
    iRet = 0;

  }else if( rc==LSM_OK ){
    /* If a block was found in the free block list, use it and remove it from 
    ** the list. Otherwise, if no suitable block was found, allocate one from
    ** the end of the file.  */
    if( iRet>0 ){
#ifdef LSM_LOG_FREELIST
      lsmLogMessage(pDb, 0, 
          "reusing block %d (snapshot-in-use=%lld)", iRet, iInUse);
#endif
      rc = freelistAppend(pDb, iRet, -1);
      if( rc==LSM_OK ){
        rc = dbTruncate(pDb, iInUse);
      }
    }else{
      iRet = ++(p->nBlock);
#ifdef LSM_LOG_FREELIST
      lsmLogMessage(pDb, 0, "extending file to %d blocks", iRet);
#endif
    }
  }

  assert( iBefore>0 || iRet>0 || rc!=LSM_OK );
  *piBlk = iRet;
  return rc;
}

/*
** Free a database block. The worker snapshot must be held in order to call 
** this function.
**
** If successful, LSM_OK is returned. Otherwise, an lsm error code (e.g. 
** LSM_NOMEM).
*/
static int lsmBlockFree(lsm_db *pDb, int iBlk){
  Snapshot *p = pDb->pWorker;
  assert( lsmShmAssertWorker(pDb) );

#ifdef LSM_LOG_FREELIST
  lsmLogMessage(pDb, LSM_OK, "lsmBlockFree(): Free block %d", iBlk);
#endif

  return freelistAppend(pDb, iBlk, p->iId);
}

/*
** Refree a database block. The worker snapshot must be held in order to call 
** this function.
**
** Refreeing is required when a block is allocated using lsmBlockAllocate()
** but then not used. This function is used to push the block back onto
** the freelist. Refreeing a block is different from freeing is, as a refreed
** block may be reused immediately. Whereas a freed block can not be reused 
** until (at least) after the next checkpoint.
*/
static int lsmBlockRefree(lsm_db *pDb, int iBlk){
  int rc = LSM_OK;                /* Return code */

#ifdef LSM_LOG_FREELIST
  lsmLogMessage(pDb, LSM_OK, "lsmBlockRefree(): Refree block %d", iBlk);
#endif

  rc = freelistAppend(pDb, iBlk, 0);
  return rc;
}

/*
** If required, copy a database checkpoint from shared memory into the
** database itself.
**
** The WORKER lock must not be held when this is called. This is because
** this function may indirectly call fsync(). And the WORKER lock should
** not be held that long (in case it is required by a client flushing an
** in-memory tree to disk).
*/
static int lsmCheckpointWrite(lsm_db *pDb, u32 *pnWrite){
  int rc;                         /* Return Code */
  u32 nWrite = 0;

  assert( pDb->pWorker==0 );
  assert( 1 || pDb->pClient==0 );
  assert( lsmShmAssertLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_UNLOCK) );

  rc = lsmShmLock(pDb, LSM_LOCK_CHECKPOINTER, LSM_LOCK_EXCL, 0);
  if( rc!=LSM_OK ) return rc;

  rc = lsmCheckpointLoad(pDb, 0);
  if( rc==LSM_OK ){
    int nBlock = lsmCheckpointNBlock(pDb->aSnapshot);
    ShmHeader *pShm = pDb->pShmhdr;
    int bDone = 0;                /* True if checkpoint is already stored */

    /* Check if this checkpoint has already been written to the database
    ** file. If so, set variable bDone to true.  */
    if( pShm->iMetaPage ){
      MetaPage *pPg;              /* Meta page */
      u8 *aData;                  /* Meta-page data buffer */
      int nData;                  /* Size of aData[] in bytes */
      i64 iCkpt;                  /* Id of checkpoint just loaded */
      i64 iDisk = 0;              /* Id of checkpoint already stored in db */
      iCkpt = lsmCheckpointId(pDb->aSnapshot, 0);
      rc = lsmFsMetaPageGet(pDb->pFS, 0, pShm->iMetaPage, &pPg);
      if( rc==LSM_OK ){
        aData = lsmFsMetaPageData(pPg, &nData);
        iDisk = lsmCheckpointId((u32 *)aData, 1);
        nWrite = lsmCheckpointNWrite((u32 *)aData, 1);
        lsmFsMetaPageRelease(pPg);
      }
      bDone = (iDisk>=iCkpt);
    }

    if( rc==LSM_OK && bDone==0 ){
      int iMeta = (pShm->iMetaPage % 2) + 1;
      if( pDb->eSafety!=LSM_SAFETY_OFF ){
        rc = lsmFsSyncDb(pDb->pFS, nBlock);
      }
      if( rc==LSM_OK ) rc = lsmCheckpointStore(pDb, iMeta);
      if( rc==LSM_OK && pDb->eSafety!=LSM_SAFETY_OFF){
        rc = lsmFsSyncDb(pDb->pFS, 0);
      }
      if( rc==LSM_OK ){
        pShm->iMetaPage = iMeta;
        nWrite = lsmCheckpointNWrite(pDb->aSnapshot, 0) - nWrite;
      }
#ifdef LSM_LOG_WORK
      lsmLogMessage(pDb, 0, "finish checkpoint %d", 
          (int)lsmCheckpointId(pDb->aSnapshot, 0)
      );
#endif
    }
  }

  lsmShmLock(pDb, LSM_LOCK_CHECKPOINTER, LSM_LOCK_UNLOCK, 0);
  if( pnWrite && rc==LSM_OK ) *pnWrite = nWrite;
  return rc;
}

static int lsmBeginWork(lsm_db *pDb){
  int rc;

  /* Attempt to take the WORKER lock */
  rc = lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL, 0);

  /* Deserialize the current worker snapshot */
  if( rc==LSM_OK ){
    rc = lsmCheckpointLoadWorker(pDb);
  }
  return rc;
}

static void lsmFreeSnapshot(lsm_env *pEnv, Snapshot *p){
  if( p ){
    lsmSortedFreeLevel(pEnv, p->pLevel);
    lsmFree(pEnv, p->freelist.aEntry);
    lsmFree(pEnv, p->redirect.a);
    lsmFree(pEnv, p);
  }
}

/*
** Attempt to populate one of the read-lock slots to contain lock values
** iLsm/iShm. Or, if such a slot exists already, this function is a no-op.
**
** It is not an error if no slot can be populated because the write-lock
** cannot be obtained. If any other error occurs, return an LSM error code.
** Otherwise, LSM_OK.
**
** This function is called at various points to try to ensure that there
** always exists at least one read-lock slot that can be used by a read-only
** client. And so that, in the usual case, there is an "exact match" available
** whenever a read transaction is opened by any client. At present this
** function is called when:
**
**    * A write transaction that called lsmTreeDiscardOld() is committed, and
**    * Whenever the working snapshot is updated (i.e. lsmFinishWork()).
*/
static int dbSetReadLock(lsm_db *db, i64 iLsm, u32 iShm){
  int rc = LSM_OK;
  ShmHeader *pShm = db->pShmhdr;
  int i;

  /* Check if there is already a slot containing the required values. */
  for(i=0; i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId==iLsm && p->iTreeId==iShm ) return LSM_OK;
  }

  /* Iterate through all read-lock slots, attempting to take a write-lock
  ** on each of them. If a write-lock succeeds, populate the locked slot
  ** with the required values and break out of the loop.  */
  for(i=0; rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL, 0);
    if( rc==LSM_BUSY ){
      rc = LSM_OK;
    }else{
      ShmReader *p = &pShm->aReader[i];
      p->iLsmId = iLsm;
      p->iTreeId = iShm;
      lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_UNLOCK, 0);
      break;
    }
  }

  return rc;
}

/*
** Release the read-lock currently held by connection db.
*/
int dbReleaseReadlock(lsm_db *db){
  int rc = LSM_OK;
  if( db->iReader>=0 ){
    rc = lsmShmLock(db, LSM_LOCK_READER(db->iReader), LSM_LOCK_UNLOCK, 0);
    db->iReader = -1;
  }
  db->bRoTrans = 0;
  return rc;
}


/*
** Argument bFlush is true if the contents of the in-memory tree has just
** been flushed to disk. The significance of this is that once the snapshot
** created to hold the updated state of the database is synced to disk, log
** file space can be recycled.
*/
static void lsmFinishWork(lsm_db *pDb, int bFlush, int *pRc){
  int rc = *pRc;
  assert( rc!=0 || pDb->pWorker );
  if( pDb->pWorker ){
    /* If no error has occurred, serialize the worker snapshot and write
    ** it to shared memory.  */
    if( rc==LSM_OK ){
      rc = lsmSaveWorker(pDb, bFlush);
    }

    /* Assuming no error has occurred, update a read lock slot with the
    ** new snapshot id (see comments above function dbSetReadLock()).  */
    if( rc==LSM_OK ){
      if( pDb->iReader<0 ){
        rc = lsmTreeLoadHeader(pDb, 0);
      }
      if( rc==LSM_OK ){
        rc = dbSetReadLock(pDb, pDb->pWorker->iId, pDb->treehdr.iUsedShmid);
      }
    }

    /* Free the snapshot object. */
    lsmFreeSnapshot(pDb->pEnv, pDb->pWorker);
    pDb->pWorker = 0;
  }

  lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_UNLOCK, 0);
  *pRc = rc;
}

/*
** Called when recovery is finished.
*/
static int lsmFinishRecovery(lsm_db *pDb){
  lsmTreeEndTransaction(pDb, 1);
  return LSM_OK;
}

/*
** Check if the currently configured compression functions
** (LSM_CONFIG_SET_COMPRESSION) are compatible with a database that has its
** compression id set to iReq. Compression routines are compatible if iReq
** is zero (indicating the database is empty), or if it is equal to the 
** compression id of the configured compression routines.
**
** If the check shows that the current compression are incompatible and there
** is a compression factory registered, give it a chance to install new
** compression routines.
**
** If, after any registered factory is invoked, the compression functions
** are still incompatible, return LSM_MISMATCH. Otherwise, LSM_OK.
*/
static int lsmCheckCompressionId(lsm_db *pDb, u32 iReq){
  if( iReq!=LSM_COMPRESSION_EMPTY && pDb->compress.iId!=iReq ){
    if( pDb->factory.xFactory ){
      pDb->bInFactory = 1;
      pDb->factory.xFactory(pDb->factory.pCtx, pDb, iReq);
      pDb->bInFactory = 0;
    }
    if( pDb->compress.iId!=iReq ){
      /* Incompatible */
      return LSM_MISMATCH;
    }
  }
  /* Compatible */
  return LSM_OK;
}

/*
** Begin a read transaction. This function is a no-op if the connection
** passed as the only argument already has an open read transaction.
*/
static int lsmBeginReadTrans(lsm_db *pDb){
  const int MAX_READLOCK_ATTEMPTS = 10;
  const int nMaxAttempt = (pDb->bRoTrans ? 1 : MAX_READLOCK_ATTEMPTS);

  int rc = LSM_OK;                /* Return code */
  int iAttempt = 0;

  assert( pDb->pWorker==0 );

  while( rc==LSM_OK && pDb->iReader<0 && (iAttempt++)<nMaxAttempt ){
    int iTreehdr = 0;
    int iSnap = 0;
    assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

    /* Load the in-memory tree header. */
    rc = lsmTreeLoadHeader(pDb, &iTreehdr);

    /* Load the database snapshot */
    if( rc==LSM_OK ){
      if( lsmCheckpointClientCacheOk(pDb)==0 ){
        lsmFreeSnapshot(pDb->pEnv, pDb->pClient);
        pDb->pClient = 0;
        lsmMCursorFreeCache(pDb);
        lsmFsPurgeCache(pDb->pFS);
        rc = lsmCheckpointLoad(pDb, &iSnap);
      }else{
        iSnap = 1;
      }
    }

    /* Take a read-lock on the tree and snapshot just loaded. Then check
    ** that the shared-memory still contains the same values. If so, proceed.
    ** Otherwise, relinquish the read-lock and retry the whole procedure
    ** (starting with loading the in-memory tree header).  */
    if( rc==LSM_OK ){
      u32 iShmMax = pDb->treehdr.iUsedShmid;
      u32 iShmMin = pDb->treehdr.iNextShmid+1-LSM_MAX_SHMCHUNKS;
      rc = lsmReadlock(
          pDb, lsmCheckpointId(pDb->aSnapshot, 0), iShmMin, iShmMax
      );
      if( rc==LSM_OK ){
        if( lsmTreeLoadHeaderOk(pDb, iTreehdr)
         && lsmCheckpointLoadOk(pDb, iSnap)
        ){
          /* Read lock has been successfully obtained. Deserialize the 
          ** checkpoint just loaded. TODO: This will be removed after 
          ** lsm_sorted.c is changed to work directly from the serialized
          ** version of the snapshot.  */
          if( pDb->pClient==0 ){
            rc = lsmCheckpointDeserialize(pDb, 0, pDb->aSnapshot,&pDb->pClient);
          }
          assert( (rc==LSM_OK)==(pDb->pClient!=0) );
          assert( pDb->iReader>=0 );

          /* Check that the client has the right compression hooks loaded.
          ** If not, set rc to LSM_MISMATCH.  */
          if( rc==LSM_OK ){
            rc = lsmCheckCompressionId(pDb, pDb->pClient->iCmpId);
          }
        }else{
          rc = dbReleaseReadlock(pDb);
        }
      }

      if( rc==LSM_BUSY ){
        rc = LSM_OK;
      }
    }
#if 0
if( rc==LSM_OK && pDb->pClient ){
  fprintf(stderr, 
      "reading %p: snapshot:%d used-shmid:%d trans-id:%d iOldShmid=%d\n",
      (void *)pDb,
      (int)pDb->pClient->iId, (int)pDb->treehdr.iUsedShmid, 
      (int)pDb->treehdr.root.iTransId,
      (int)pDb->treehdr.iOldShmid
  );
}
#endif
  }

  if( rc==LSM_OK ){
    rc = lsmShmCacheChunks(pDb, pDb->treehdr.nChunk);
  }
  if( rc!=LSM_OK ){
    dbReleaseReadlock(pDb);
  }
  if( pDb->pClient==0 && rc==LSM_OK ) rc = LSM_BUSY;
  return rc;
}

/*
** This function is used by a read-write connection to determine if there
** are currently one or more read-only transactions open on the database
** (in this context a read-only transaction is one opened by a read-only
** connection on a non-live database).
**
** If no error occurs, LSM_OK is returned and *pbExists is set to true if
** some other connection has a read-only transaction open, or false 
** otherwise. If an error occurs an LSM error code is returned and the final
** value of *pbExist is undefined.
*/
static int lsmDetectRoTrans(lsm_db *db, int *pbExist){
  int rc;

  /* Only a read-write connection may use this function. */
  assert( db->bReadonly==0 );

  rc = lsmShmTestLock(db, LSM_LOCK_ROTRANS, 1, LSM_LOCK_EXCL);
  if( rc==LSM_BUSY ){
    *pbExist = 1;
    rc = LSM_OK;
  }else{
    *pbExist = 0;
  }

  return rc;
}

/*
** db is a read-only database handle in the disconnected state. This function
** attempts to open a read-transaction on the database. This may involve
** connecting to the database system (opening shared memory etc.).
*/
static int lsmBeginRoTrans(lsm_db *db){
  int rc = LSM_OK;

  assert( db->bReadonly && db->pShmhdr==0 );
  assert( db->iReader<0 );

  if( db->bRoTrans==0 ){

    /* Attempt a shared-lock on DMS1. */
    rc = lsmShmLock(db, LSM_LOCK_DMS1, LSM_LOCK_SHARED, 0);
    if( rc!=LSM_OK ) return rc;

    rc = lsmShmTestLock(
        db, LSM_LOCK_RWCLIENT(0), LSM_LOCK_NREADER, LSM_LOCK_SHARED
    );
    if( rc==LSM_OK ){
      /* System is not live. Take a SHARED lock on the ROTRANS byte and
      ** release DMS1. Locking ROTRANS tells all read-write clients that they
      ** may not recycle any disk space from within the database or log files,
      ** as a read-only client may be using it.  */
      rc = lsmShmLock(db, LSM_LOCK_ROTRANS, LSM_LOCK_SHARED, 0);
      lsmShmLock(db, LSM_LOCK_DMS1, LSM_LOCK_UNLOCK, 0);

      if( rc==LSM_OK ){
        db->bRoTrans = 1;
        rc = lsmShmCacheChunks(db, 1);
        if( rc==LSM_OK ){
          db->pShmhdr = (ShmHeader *)db->apShm[0];
          memset(db->pShmhdr, 0, sizeof(ShmHeader));
          rc = lsmCheckpointRecover(db);
          if( rc==LSM_OK ){
            rc = lsmLogRecover(db);
          }
        }
      }
    }else if( rc==LSM_BUSY ){
      /* System is live! */
      rc = lsmShmLock(db, LSM_LOCK_DMS3, LSM_LOCK_SHARED, 0);
      lsmShmLock(db, LSM_LOCK_DMS1, LSM_LOCK_UNLOCK, 0);
      if( rc==LSM_OK ){
        rc = lsmShmCacheChunks(db, 1);
        if( rc==LSM_OK ){
          db->pShmhdr = (ShmHeader *)db->apShm[0];
        }
      }
    }

    /* In 'lsm_open()' we don't update the page and block sizes in the
    ** Filesystem for 'readonly' connection. Because member 'db->pShmhdr' is a
    ** nullpointer, this prevents loading a checkpoint. Now that the system is 
    ** live this member should be set. So we can update both values in 
    ** the Filesystem.
    **
    ** Configure the file-system connection with the page-size and block-size
    ** of this database. Even if the database file is zero bytes in size
    ** on disk, these values have been set in shared-memory by now, and so
    ** are guaranteed not to change during the lifetime of this connection. */
    if( LSM_OK==rc
     && 0==lsmCheckpointClientCacheOk(db)
     && LSM_OK==(rc=lsmCheckpointLoad(db, 0)) 
    ){
      lsmFsSetPageSize(db->pFS, lsmCheckpointPgsz(db->aSnapshot));
      lsmFsSetBlockSize(db->pFS, lsmCheckpointBlksz(db->aSnapshot));
    }

    if( rc==LSM_OK ){
      rc = lsmBeginReadTrans(db);
    }
  }

  return rc;
}

/*
** Close the currently open read transaction.
*/
static void lsmFinishReadTrans(lsm_db *pDb){

  /* Worker connections should not be closing read transactions. And
  ** read transactions should only be closed after all cursors and write
  ** transactions have been closed. Finally pClient should be non-NULL
  ** only iff pDb->iReader>=0.  */
  assert( pDb->pWorker==0 );
  assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

  if( pDb->bRoTrans ){
    int i;
    for(i=0; i<pDb->nShm; i++){
      lsmFree(pDb->pEnv, pDb->apShm[i]);
    }
    lsmFree(pDb->pEnv, pDb->apShm);
    pDb->apShm = 0;
    pDb->nShm = 0;
    pDb->pShmhdr = 0;

    lsmShmLock(pDb, LSM_LOCK_ROTRANS, LSM_LOCK_UNLOCK, 0);
  }
  dbReleaseReadlock(pDb);
}

/*
** Open a write transaction.
*/
static int lsmBeginWriteTrans(lsm_db *pDb){
  int rc = LSM_OK;                /* Return code */
  ShmHeader *pShm = pDb->pShmhdr; /* Shared memory header */

  assert( pDb->nTransOpen==0 );
  assert( pDb->bDiscardOld==0 );
  assert( pDb->bReadonly==0 );

  /* If there is no read-transaction open, open one now. */
  if( pDb->iReader<0 ){
    rc = lsmBeginReadTrans(pDb);
  }

  /* Attempt to take the WRITER lock */
  if( rc==LSM_OK ){
    rc = lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_EXCL, 0);
  }

  /* If the previous writer failed mid-transaction, run emergency rollback. */
  if( rc==LSM_OK && pShm->bWriter ){
    rc = lsmTreeRepair(pDb);
    if( rc==LSM_OK ) pShm->bWriter = 0;
  }

  /* Check that this connection is currently reading from the most recent
  ** version of the database. If not, return LSM_BUSY.  */
  if( rc==LSM_OK && memcmp(&pShm->hdr1, &pDb->treehdr, sizeof(TreeHeader)) ){
    rc = LSM_BUSY;
  }

  if( rc==LSM_OK ){
    rc = lsmLogBegin(pDb);
  }

  /* If everything was successful, set the "transaction-in-progress" flag
  ** and return LSM_OK. Otherwise, if some error occurred, relinquish the 
  ** WRITER lock and return an error code.  */
  if( rc==LSM_OK ){
    TreeHeader *p = &pDb->treehdr;
    pShm->bWriter = 1;
    p->root.iTransId++;
    if( lsmTreeHasOld(pDb) && p->iOldLog==pDb->pClient->iLogOff ){
      lsmTreeDiscardOld(pDb);
      pDb->bDiscardOld = 1;
    }
  }else{
    lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK, 0);
    if( pDb->pCsr==0 ) lsmFinishReadTrans(pDb);
  }
  return rc;
}

/*
** End the current write transaction. The connection is left with an open
** read transaction. It is an error to call this if there is no open write 
** transaction.
**
** If the transaction was committed, then a commit record has already been
** written into the log file when this function is called. Or, if the
** transaction was rolled back, both the log file and in-memory tree 
** structure have already been restored. In either case, this function 
** merely releases locks and other resources held by the write-transaction.
**
** LSM_OK is returned if successful, or an LSM error code otherwise.
*/
static int lsmFinishWriteTrans(lsm_db *pDb, int bCommit){
  int rc = LSM_OK;
  int bFlush = 0;

  lsmLogEnd(pDb, bCommit);
  if( rc==LSM_OK && bCommit && lsmTreeSize(pDb)>pDb->nTreeLimit ){
    bFlush = 1;
    lsmTreeMakeOld(pDb);
  }
  lsmTreeEndTransaction(pDb, bCommit);

  if( rc==LSM_OK ){
    if( bFlush && pDb->bAutowork ){
      rc = lsmSortedAutoWork(pDb, 1);
    }else if( bCommit && pDb->bDiscardOld ){
      rc = dbSetReadLock(pDb, pDb->pClient->iId, pDb->treehdr.iUsedShmid);
    }
  }
  pDb->bDiscardOld = 0;
  lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK, 0);

  if( bFlush && pDb->bAutowork==0 && pDb->xWork ){
    pDb->xWork(pDb, pDb->pWorkCtx);
  }
  return rc;
}


/*
** Return non-zero if the caller is holding the client mutex.
*/
#ifdef LSM_DEBUG
static int lsmHoldingClientMutex(lsm_db *pDb){
  return lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pClientMutex);
}
#endif

static int slotIsUsable(ShmReader *p, i64 iLsm, u32 iShmMin, u32 iShmMax){
  return( 
      p->iLsmId && p->iLsmId<=iLsm 
      && shm_sequence_ge(iShmMax, p->iTreeId)
      && shm_sequence_ge(p->iTreeId, iShmMin)
  );
}

/*
** Obtain a read-lock on database version identified by the combination
** of snapshot iLsm and tree iTree. Return LSM_OK if successful, or
** an LSM error code otherwise.
*/
static int lsmReadlock(lsm_db *db, i64 iLsm, u32 iShmMin, u32 iShmMax){
  int rc = LSM_OK;
  ShmHeader *pShm = db->pShmhdr;
  int i;

  assert( db->iReader<0 );
  assert( shm_sequence_ge(iShmMax, iShmMin) );

  /* This is a no-op if the read-only transaction flag is set. */
  if( db->bRoTrans ){
    db->iReader = 0;
    return LSM_OK;
  }

  /* Search for an exact match. */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId==iLsm && p->iTreeId==iShmMax ){
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED, 0);
      if( rc==LSM_OK && p->iLsmId==iLsm && p->iTreeId==iShmMax ){
        db->iReader = i;
      }else if( rc==LSM_BUSY ){
        rc = LSM_OK;
      }
    }
  }

  /* Try to obtain a write-lock on each slot, in order. If successful, set
  ** the slot values to iLsm/iTree.  */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL, 0);
    if( rc==LSM_BUSY ){
      rc = LSM_OK;
    }else{
      ShmReader *p = &pShm->aReader[i];
      p->iLsmId = iLsm;
      p->iTreeId = iShmMax;
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED, 0);
      assert( rc!=LSM_BUSY );
      if( rc==LSM_OK ) db->iReader = i;
    }
  }

  /* Search for any usable slot */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( slotIsUsable(p, iLsm, iShmMin, iShmMax) ){
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED, 0);
      if( rc==LSM_OK && slotIsUsable(p, iLsm, iShmMin, iShmMax) ){
        db->iReader = i;
      }else if( rc==LSM_BUSY ){
        rc = LSM_OK;
      }
    }
  }

  if( rc==LSM_OK && db->iReader<0 ){
    rc = LSM_BUSY;
  }
  return rc;
}

/*
** This is used to check if there exists a read-lock locking a particular
** version of either the in-memory tree or database file. 
**
** If iLsmId is non-zero, then it is a snapshot id. If there exists a 
** read-lock using this snapshot or newer, set *pbInUse to true. Or,
** if there is no such read-lock, set it to false.
**
** Or, if iLsmId is zero, then iShmid is a shared-memory sequence id.
** Search for a read-lock using this sequence id or newer. etc.
*/
static int isInUse(lsm_db *db, i64 iLsmId, u32 iShmid, int *pbInUse){
  ShmHeader *pShm = db->pShmhdr;
  int i;
  int rc = LSM_OK;

  for(i=0; rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId ){
      if( (iLsmId!=0 && p->iLsmId!=0 && iLsmId>=p->iLsmId) 
       || (iLsmId==0 && shm_sequence_ge(p->iTreeId, iShmid))
      ){
        rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL, 0);
        if( rc==LSM_OK ){
          p->iLsmId = 0;
          lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_UNLOCK, 0);
        }
      }
    }
  }

  if( rc==LSM_BUSY ){
    *pbInUse = 1;
    return LSM_OK;
  }
  *pbInUse = 0;
  return rc;
}

/*
** This function is called by worker connections to determine the smallest
** snapshot id that is currently in use by a database client. The worker
** connection uses this result to determine whether or not it is safe to
** recycle a database block.
*/
static int firstSnapshotInUse(
  lsm_db *db,                     /* Database handle */
  i64 *piInUse                    /* IN/OUT: Smallest snapshot id in use */
){
  ShmHeader *pShm = db->pShmhdr;
  i64 iInUse = *piInUse;
  int i;

  assert( iInUse>0 );
  for(i=0; i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId ){
      i64 iThis = p->iLsmId;
      if( iThis!=0 && iInUse>iThis ){
        int rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL, 0);
        if( rc==LSM_OK ){
          p->iLsmId = 0;
          lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_UNLOCK, 0);
        }else if( rc==LSM_BUSY ){
          iInUse = iThis;
        }else{
          /* Some error other than LSM_BUSY. Return the error code to
          ** the caller in this case.  */
          return rc;
        }
      }
    }
  }

  *piInUse = iInUse;
  return LSM_OK;
}

static int lsmTreeInUse(lsm_db *db, u32 iShmid, int *pbInUse){
  if( db->treehdr.iUsedShmid==iShmid ){
    *pbInUse = 1;
    return LSM_OK;
  }
  return isInUse(db, 0, iShmid, pbInUse);
}

static int lsmLsmInUse(lsm_db *db, i64 iLsmId, int *pbInUse){
  if( db->pClient && db->pClient->iId<=iLsmId ){
    *pbInUse = 1;
    return LSM_OK;
  }
  return isInUse(db, iLsmId, 0, pbInUse);
}

/*
** This function may only be called after a successful call to
** lsmDbDatabaseConnect(). It returns true if the connection is in
** multi-process mode, or false otherwise.
*/
static int lsmDbMultiProc(lsm_db *pDb){
  return pDb->pDatabase && pDb->pDatabase->bMultiProc;
}


/*************************************************************************
**************************************************************************
**************************************************************************
**************************************************************************
**************************************************************************
*************************************************************************/

/*
** Ensure that database connection db has cached pointers to at least the 
** first nChunk chunks of shared memory.
*/
static int lsmShmCacheChunks(lsm_db *db, int nChunk){
  int rc = LSM_OK;
  if( nChunk>db->nShm ){
    static const int NINCR = 16;
    Database *p = db->pDatabase;
    lsm_env *pEnv = db->pEnv;
    int nAlloc;
    int i;

    /* Ensure that the db->apShm[] array is large enough. If an attempt to
    ** allocate memory fails, return LSM_NOMEM immediately. The apShm[] array
    ** is always extended in multiples of 16 entries - so the actual allocated
    ** size can be inferred from nShm.  */ 
    nAlloc = ((db->nShm + NINCR - 1) / NINCR) * NINCR;
    while( nChunk>=nAlloc ){
      void **apShm;
      nAlloc += NINCR;
      apShm = lsmRealloc(pEnv, db->apShm, sizeof(void*)*nAlloc);
      if( !apShm ) return LSM_NOMEM_BKPT;
      db->apShm = apShm;
    }

    if( db->bRoTrans ){
      for(i=db->nShm; rc==LSM_OK && i<nChunk; i++){
        db->apShm[i] = lsmMallocZeroRc(pEnv, LSM_SHM_CHUNK_SIZE, &rc);
        db->nShm++;
      }

    }else{

      /* Enter the client mutex */
      lsmMutexEnter(pEnv, p->pClientMutex);

      /* Extend the Database objects apShmChunk[] array if necessary. Using the
       ** same pattern as for the lsm_db.apShm[] array above.  */
      nAlloc = ((p->nShmChunk + NINCR - 1) / NINCR) * NINCR;
      while( nChunk>=nAlloc ){
        void **apShm;
        nAlloc +=  NINCR;
        apShm = lsmRealloc(pEnv, p->apShmChunk, sizeof(void*)*nAlloc);
        if( !apShm ){
          rc = LSM_NOMEM_BKPT;
          break;
        }
        p->apShmChunk = apShm;
      }

      for(i=db->nShm; rc==LSM_OK && i<nChunk; i++){
        if( i>=p->nShmChunk ){
          void *pChunk = 0;
          if( p->bMultiProc==0 ){
            /* Single process mode */
            pChunk = lsmMallocZeroRc(pEnv, LSM_SHM_CHUNK_SIZE, &rc);
          }else{
            /* Multi-process mode */
            rc = lsmEnvShmMap(pEnv, p->pFile, i, LSM_SHM_CHUNK_SIZE, &pChunk);
          }
          if( rc==LSM_OK ){
            p->apShmChunk[i] = pChunk;
            p->nShmChunk++;
          }
        }
        if( rc==LSM_OK ){
          db->apShm[i] = p->apShmChunk[i];
          db->nShm++;
        }
      }

      /* Release the client mutex */
      lsmMutexLeave(pEnv, p->pClientMutex);
    }
  }

  return rc;
}

static int lockSharedFile(lsm_env *pEnv, Database *p, int iLock, int eOp){
  int rc = LSM_OK;
  if( p->bMultiProc ){
    rc = lsmEnvLock(pEnv, p->pFile, iLock, eOp);
  }
  return rc;
}

/*
** Test if it would be possible for connection db to obtain a lock of type
** eType on the nLock locks starting at iLock. If so, return LSM_OK. If it
** would not be possible to obtain the lock due to a lock held by another
** connection, return LSM_BUSY. If an IO or other error occurs (i.e. in the 
** lsm_env.xTestLock function), return some other LSM error code.
**
** Note that this function never actually locks the database - it merely
** queries the system to see if there exists a lock that would prevent
** it from doing so.
*/
static int lsmShmTestLock(
  lsm_db *db,
  int iLock,
  int nLock,
  int eOp
){
  int rc = LSM_OK;
  lsm_db *pIter;
  Database *p = db->pDatabase;
  int i;
  u64 mask = 0;

  for(i=iLock; i<(iLock+nLock); i++){
    mask |= ((u64)1 << (iLock-1));
    if( eOp==LSM_LOCK_EXCL ) mask |= ((u64)1 << (iLock+32-1));
  }

  lsmMutexEnter(db->pEnv, p->pClientMutex);
  for(pIter=p->pConn; pIter; pIter=pIter->pNext){
    if( pIter!=db && (pIter->mLock & mask) ){
      assert( pIter!=db );
      break;
    }
  }

  if( pIter ){
    rc = LSM_BUSY;
  }else if( p->bMultiProc ){
    rc = lsmEnvTestLock(db->pEnv, p->pFile, iLock, nLock, eOp);
  }

  lsmMutexLeave(db->pEnv, p->pClientMutex);
  return rc;
}

/*
** Attempt to obtain the lock identified by the iLock and bExcl parameters.
** If successful, return LSM_OK. If the lock cannot be obtained because 
** there exists some other conflicting lock, return LSM_BUSY. If some other
** error occurs, return an LSM error code.
**
** Parameter iLock must be one of LSM_LOCK_WRITER, WORKER or CHECKPOINTER,
** or else a value returned by the LSM_LOCK_READER macro.
*/
static int lsmShmLock(
  lsm_db *db, 
  int iLock,
  int eOp,                        /* One of LSM_LOCK_UNLOCK, SHARED or EXCL */
  int bBlock                      /* True for a blocking lock */
){
  lsm_db *pIter;
  const u64 me = ((u64)1 << (iLock-1));
  const u64 ms = ((u64)1 << (iLock+32-1));
  int rc = LSM_OK;
  Database *p = db->pDatabase;

  assert( eOp!=LSM_LOCK_EXCL || p->bReadonly==0 );
  assert( iLock>=1 && iLock<=LSM_LOCK_RWCLIENT(LSM_LOCK_NRWCLIENT-1) );
  assert( LSM_LOCK_RWCLIENT(LSM_LOCK_NRWCLIENT-1)<=32 );
  assert( eOp==LSM_LOCK_UNLOCK || eOp==LSM_LOCK_SHARED || eOp==LSM_LOCK_EXCL );

  /* Check for a no-op. Proceed only if this is not one of those. */
  if( (eOp==LSM_LOCK_UNLOCK && (db->mLock & (me|ms))!=0)
   || (eOp==LSM_LOCK_SHARED && (db->mLock & (me|ms))!=ms)
   || (eOp==LSM_LOCK_EXCL   && (db->mLock & me)==0)
  ){
    int nExcl = 0;                /* Number of connections holding EXCLUSIVE */
    int nShared = 0;              /* Number of connections holding SHARED */
    lsmMutexEnter(db->pEnv, p->pClientMutex);

    /* Figure out the locks currently held by this process on iLock, not
    ** including any held by connection db.  */
    for(pIter=p->pConn; pIter; pIter=pIter->pNext){
      assert( (pIter->mLock & me)==0 || (pIter->mLock & ms)!=0 );
      if( pIter!=db ){
        if( pIter->mLock & me ){
          nExcl++;
        }else if( pIter->mLock & ms ){
          nShared++;
        }
      }
    }
    assert( nExcl==0 || nExcl==1 );
    assert( nExcl==0 || nShared==0 );
    assert( nExcl==0 || (db->mLock & (me|ms))==0 );

    switch( eOp ){
      case LSM_LOCK_UNLOCK:
        if( nShared==0 ){
          lockSharedFile(db->pEnv, p, iLock, LSM_LOCK_UNLOCK);
        }
        db->mLock &= ~(me|ms);
        break;

      case LSM_LOCK_SHARED:
        if( nExcl ){
          rc = LSM_BUSY;
        }else{
          if( nShared==0 ){
            rc = lockSharedFile(db->pEnv, p, iLock, LSM_LOCK_SHARED);
          }
          if( rc==LSM_OK ){
            db->mLock |= ms;
            db->mLock &= ~me;
          }
        }
        break;

      default:
        assert( eOp==LSM_LOCK_EXCL );
        if( nExcl || nShared ){
          rc = LSM_BUSY;
        }else{
          rc = lockSharedFile(db->pEnv, p, iLock, LSM_LOCK_EXCL);
          if( rc==LSM_OK ){
            db->mLock |= (me|ms);
          }
        }
        break;
    }

    lsmMutexLeave(db->pEnv, p->pClientMutex);
  }

  return rc;
}

#ifdef LSM_DEBUG

int shmLockType(lsm_db *db, int iLock){
  const u64 me = ((u64)1 << (iLock-1));
  const u64 ms = ((u64)1 << (iLock+32-1));

  if( db->mLock & me ) return LSM_LOCK_EXCL;
  if( db->mLock & ms ) return LSM_LOCK_SHARED;
  return LSM_LOCK_UNLOCK;
}

/*
** The arguments passed to this function are similar to those passed to
** the lsmShmLock() function. However, instead of obtaining a new lock 
** this function returns true if the specified connection already holds 
** (or does not hold) such a lock, depending on the value of eOp. As
** follows:
**
**   (eOp==LSM_LOCK_UNLOCK) -> true if db has no lock on iLock
**   (eOp==LSM_LOCK_SHARED) -> true if db has at least a SHARED lock on iLock.
**   (eOp==LSM_LOCK_EXCL)   -> true if db has an EXCLUSIVE lock on iLock.
*/
static int lsmShmAssertLock(lsm_db *db, int iLock, int eOp){
  int ret = 0;
  int eHave;

  assert( iLock>=1 && iLock<=LSM_LOCK_READER(LSM_LOCK_NREADER-1) );
  assert( iLock<=16 );
  assert( eOp==LSM_LOCK_UNLOCK || eOp==LSM_LOCK_SHARED || eOp==LSM_LOCK_EXCL );

  eHave = shmLockType(db, iLock);

  switch( eOp ){
    case LSM_LOCK_UNLOCK:
      ret = (eHave==LSM_LOCK_UNLOCK);
      break;
    case LSM_LOCK_SHARED:
      ret = (eHave!=LSM_LOCK_UNLOCK);
      break;
    case LSM_LOCK_EXCL:
      ret = (eHave==LSM_LOCK_EXCL);
      break;
    default:
      assert( !"bad eOp value passed to lsmShmAssertLock()" );
      break;
  }

  return ret;
}

static int lsmShmAssertWorker(lsm_db *db){
  return lsmShmAssertLock(db, LSM_LOCK_WORKER, LSM_LOCK_EXCL) && db->pWorker;
}

/*
** This function does not contribute to library functionality, and is not
** included in release builds. It is intended to be called from within
** an interactive debugger.
**
** When called, this function prints a single line of human readable output
** to stdout describing the locks currently held by the connection. For 
** example:
**
**     (gdb) call print_db_locks(pDb)
**     (shared on dms2) (exclusive on writer) 
*/
void print_db_locks(lsm_db *db){
  int iLock;
  for(iLock=0; iLock<16; iLock++){
    int bOne = 0;
    const char *azLock[] = {0, "shared", "exclusive"};
    const char *azName[] = {
      0, "dms1", "dms2", "writer", "worker", "checkpointer",
      "reader0", "reader1", "reader2", "reader3", "reader4", "reader5"
    };
    int eHave = shmLockType(db, iLock);
    if( azLock[eHave] ){
      printf("%s(%s on %s)", (bOne?" ":""), azLock[eHave], azName[iLock]);
      bOne = 1;
    }
  }
  printf("\n");
}
void print_all_db_locks(lsm_db *db){
  lsm_db *p;
  for(p=db->pDatabase->pConn; p; p=p->pNext){
    printf("%s connection %p ", ((p==db)?"*":""), p);
    print_db_locks(p);
  }
}
#endif

static void lsmShmBarrier(lsm_db *db){
  lsmEnvShmBarrier(db->pEnv);
}

int lsm_checkpoint(lsm_db *pDb, int *pnKB){
  int rc;                         /* Return code */
  u32 nWrite = 0;                 /* Number of pages checkpointed */

  /* Attempt the checkpoint. If successful, nWrite is set to the number of
  ** pages written between this and the previous checkpoint.  */
  rc = lsmCheckpointWrite(pDb, &nWrite);

  /* If required, calculate the output variable (KB of data checkpointed). 
  ** Set it to zero if an error occured.  */
  if( pnKB ){
    int nKB = 0;
    if( rc==LSM_OK && nWrite ){
      nKB = (((i64)nWrite * lsmFsPageSize(pDb->pFS)) + 1023) / 1024;
    }
    *pnKB = nKB;
  }

  return rc;
}

#line 1 "lsm_sorted.c"
/*
** 2011-08-14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** PAGE FORMAT:
**
**   The maximum page size is 65536 bytes.
**
**   Since all records are equal to or larger than 2 bytes in size, and 
**   some space within the page is consumed by the page footer, there must
**   be less than 2^15 records on each page.
**
**   Each page ends with a footer that describes the pages contents. This
**   footer serves as similar purpose to the page header in an SQLite database.
**   A footer is used instead of a header because it makes it easier to
**   populate a new page based on a sorted list of key/value pairs.
**
**   The footer consists of the following values (starting at the end of
**   the page and continuing backwards towards the start). All values are
**   stored as unsigned big-endian integers.
**
**     * Number of records on page (2 bytes).
**     * Flags field (2 bytes).
**     * Left-hand pointer value (8 bytes).
**     * The starting offset of each record (2 bytes per record).
**
**   Records may span pages. Unless it happens to be an exact fit, the part
**   of the final record that starts on page X that does not fit on page X
**   is stored at the start of page (X+1). This means there may be pages where
**   (N==0). And on most pages the first record that starts on the page will
**   not start at byte offset 0. For example:
**
**      aaaaa bbbbb ccc <footer>    cc eeeee fffff g <footer>    gggg....
**
** RECORD FORMAT:
** 
**   The first byte of the record is a flags byte. It is a combination
**   of the following flags (defined in lsmInt.h):
**
**       LSM_START_DELETE
**       LSM_END_DELETE 
**       LSM_POINT_DELETE
**       LSM_INSERT    
**       LSM_SEPARATOR
**       LSM_SYSTEMKEY
**
**   Immediately following the type byte is a pointer to the smallest key 
**   in the next file that is larger than the key in the current record. The 
**   pointer is encoded as a varint. When added to the 32-bit page number 
**   stored in the footer, it is the page number of the page that contains the
**   smallest key in the next sorted file that is larger than this key. 
**
**   Next is the number of bytes in the key, encoded as a varint.
**
**   If the LSM_INSERT flag is set, the number of bytes in the value, as
**   a varint, is next.
**
**   Finally, the blob of data containing the key, and for LSM_INSERT
**   records, the value as well.
*/

#ifndef _LSM_INT_H
/* # include "lsmInt.h" */
#endif

#define LSM_LOG_STRUCTURE 0
#define LSM_LOG_DATA      0

/*
** Macros to help decode record types.
*/
#define rtTopic(eType)       ((eType) & LSM_SYSTEMKEY)
#define rtIsDelete(eType)    (((eType) & 0x0F)==LSM_POINT_DELETE)

#define rtIsSeparator(eType) (((eType) & LSM_SEPARATOR)!=0)
#define rtIsWrite(eType)     (((eType) & LSM_INSERT)!=0)
#define rtIsSystem(eType)    (((eType) & LSM_SYSTEMKEY)!=0)

/*
** The following macros are used to access a page footer.
*/
#define SEGMENT_NRECORD_OFFSET(pgsz)        ((pgsz) - 2)
#define SEGMENT_FLAGS_OFFSET(pgsz)          ((pgsz) - 2 - 2)
#define SEGMENT_POINTER_OFFSET(pgsz)        ((pgsz) - 2 - 2 - 8)
#define SEGMENT_CELLPTR_OFFSET(pgsz, iCell) ((pgsz) - 2 - 2 - 8 - 2 - (iCell)*2)

#define SEGMENT_EOF(pgsz, nEntry) SEGMENT_CELLPTR_OFFSET(pgsz, nEntry-1)

#define SEGMENT_BTREE_FLAG     0x0001
#define PGFTR_SKIP_NEXT_FLAG   0x0002
#define PGFTR_SKIP_THIS_FLAG   0x0004


#ifndef LSM_SEGMENTPTR_FREE_THRESHOLD
# define LSM_SEGMENTPTR_FREE_THRESHOLD 1024
#endif

typedef struct SegmentPtr SegmentPtr;
typedef struct LsmBlob LsmBlob;

struct LsmBlob {
  lsm_env *pEnv;
  void *pData;
  int nData;
  int nAlloc;
};

/*
** A SegmentPtr object may be used for one of two purposes:
**
**   * To iterate and/or seek within a single Segment (the combination of a 
**     main run and an optional sorted run).
**
**   * To iterate through the separators array of a segment.
*/
struct SegmentPtr {
  Level *pLevel;                /* Level object segment is part of */
  Segment *pSeg;                /* Segment to access */

  /* Current page. See segmentPtrLoadPage(). */
  Page *pPg;                    /* Current page */
  u16 flags;                    /* Copy of page flags field */
  int nCell;                    /* Number of cells on pPg */
  LsmPgno iPtr;                 /* Base cascade pointer */

  /* Current cell. See segmentPtrLoadCell() */
  int iCell;                    /* Current record within page pPg */
  int eType;                    /* Type of current record */
  LsmPgno iPgPtr;               /* Cascade pointer offset */
  void *pKey; int nKey;         /* Key associated with current record */
  void *pVal; int nVal;         /* Current record value (eType==WRITE only) */

  /* Blobs used to allocate buffers for pKey and pVal as required */
  LsmBlob blob1;
  LsmBlob blob2;
};

/*
** Used to iterate through the keys stored in a b-tree hierarchy from start
** to finish. Only First() and Next() operations are required.
**
**   btreeCursorNew()
**   btreeCursorFirst()
**   btreeCursorNext()
**   btreeCursorFree()
**   btreeCursorPosition()
**   btreeCursorRestore()
*/
typedef struct BtreePg BtreePg;
typedef struct BtreeCursor BtreeCursor;
struct BtreePg {
  Page *pPage;
  int iCell;
};
struct BtreeCursor {
  Segment *pSeg;                  /* Iterate through this segments btree */
  FileSystem *pFS;                /* File system to read pages from */
  int nDepth;                     /* Allocated size of aPg[] */
  int iPg;                        /* Current entry in aPg[]. -1 -> EOF. */
  BtreePg *aPg;                   /* Pages from root to current location */

  /* Cache of current entry. pKey==0 for EOF. */
  void *pKey;
  int nKey;
  int eType;
  LsmPgno iPtr;

  /* Storage for key, if not local */
  LsmBlob blob;
};


/*
** A cursor used for merged searches or iterations through up to one
** Tree structure and any number of sorted files.
**
**   lsmMCursorNew()
**   lsmMCursorSeek()
**   lsmMCursorNext()
**   lsmMCursorPrev()
**   lsmMCursorFirst()
**   lsmMCursorLast()
**   lsmMCursorKey()
**   lsmMCursorValue()
**   lsmMCursorValid()
**
** iFree:
**   This variable is only used by cursors providing input data for a
**   new top-level segment. Such cursors only ever iterate forwards, not
**   backwards.
*/
struct MultiCursor {
  lsm_db *pDb;                    /* Connection that owns this cursor */
  MultiCursor *pNext;             /* Next cursor owned by connection pDb */
  int flags;                      /* Mask of CURSOR_XXX flags */

  int eType;                      /* Cache of current key type */
  LsmBlob key;                    /* Cache of current key (or NULL) */
  LsmBlob val;                    /* Cache of current value */

  /* All the component cursors: */
  TreeCursor *apTreeCsr[2];       /* Up to two tree cursors */
  int iFree;                      /* Next element of free-list (-ve for eof) */
  SegmentPtr *aPtr;               /* Array of segment pointers */
  int nPtr;                       /* Size of array aPtr[] */
  BtreeCursor *pBtCsr;            /* b-tree cursor (db writes only) */

  /* Comparison results */
  int nTree;                      /* Size of aTree[] array */
  int *aTree;                     /* Array of comparison results */

  /* Used by cursors flushing the in-memory tree only */
  void *pSystemVal;               /* Pointer to buffer to free */

  /* Used by worker cursors only */
  LsmPgno *pPrevMergePtr;
};

/*
** The following constants are used to assign integers to each component
** cursor of a multi-cursor.
*/
#define CURSOR_DATA_TREE0     0   /* Current tree cursor (apTreeCsr[0]) */
#define CURSOR_DATA_TREE1     1   /* The "old" tree, if any (apTreeCsr[1]) */
#define CURSOR_DATA_SYSTEM    2   /* Free-list entries (new-toplevel only) */
#define CURSOR_DATA_SEGMENT   3   /* First segment pointer (aPtr[0]) */

/*
** CURSOR_IGNORE_DELETE
**   If set, this cursor will not visit SORTED_DELETE keys.
**
** CURSOR_FLUSH_FREELIST
**   This cursor is being used to create a new toplevel. It should also 
**   iterate through the contents of the in-memory free block list.
**
** CURSOR_IGNORE_SYSTEM
**   If set, this cursor ignores system keys.
**
** CURSOR_NEXT_OK
**   Set if it is Ok to call lsm_csr_next().
**
** CURSOR_PREV_OK
**   Set if it is Ok to call lsm_csr_prev().
**
** CURSOR_READ_SEPARATORS
**   Set if this cursor should visit the separator keys in segment 
**   aPtr[nPtr-1].
**
** CURSOR_SEEK_EQ
**   Cursor has undergone a successful lsm_csr_seek(LSM_SEEK_EQ) operation.
**   The key and value are stored in MultiCursor.key and MultiCursor.val
**   respectively.
*/
#define CURSOR_IGNORE_DELETE    0x00000001
#define CURSOR_FLUSH_FREELIST   0x00000002
#define CURSOR_IGNORE_SYSTEM    0x00000010
#define CURSOR_NEXT_OK          0x00000020
#define CURSOR_PREV_OK          0x00000040
#define CURSOR_READ_SEPARATORS  0x00000080
#define CURSOR_SEEK_EQ          0x00000100

typedef struct MergeWorker MergeWorker;
typedef struct Hierarchy Hierarchy;

struct Hierarchy {
  Page **apHier;
  int nHier;
};

/*
** aSave:
**   When mergeWorkerNextPage() is called to advance to the next page in
**   the output segment, if the bStore flag for an element of aSave[] is
**   true, it is cleared and the corresponding iPgno value is set to the 
**   page number of the page just completed.
**
**   aSave[0] is used to record the pointer value to be pushed into the
**   b-tree hierarchy. aSave[1] is used to save the page number of the
**   page containing the indirect key most recently written to the b-tree.
**   see mergeWorkerPushHierarchy() for details.
*/
struct MergeWorker {
  lsm_db *pDb;                    /* Database handle */
  Level *pLevel;                  /* Worker snapshot Level being merged */
  MultiCursor *pCsr;              /* Cursor to read new segment contents from */
  int bFlush;                     /* True if this is an in-memory tree flush */
  Hierarchy hier;                 /* B-tree hierarchy under construction */
  Page *pPage;                    /* Current output page */
  int nWork;                      /* Number of calls to mergeWorkerNextPage() */
  LsmPgno *aGobble;               /* Gobble point for each input segment */

  LsmPgno iIndirect;
  struct SavedPgno {
    LsmPgno iPgno;
    int bStore;
  } aSave[2];
};

#ifdef LSM_DEBUG_EXPENSIVE
static int assertPointersOk(lsm_db *, Segment *, Segment *, int);
static int assertBtreeOk(lsm_db *, Segment *);
static void assertRunInOrder(lsm_db *pDb, Segment *pSeg);
#else
#define assertRunInOrder(x,y)
#define assertBtreeOk(x,y)
#endif


struct FilePage { u8 *aData; int nData; };
static u8 *fsPageData(Page *pPg, int *pnData){
  *pnData = ((struct FilePage *)(pPg))->nData;
  return ((struct FilePage *)(pPg))->aData;
}
/*UNUSED static u8 *fsPageDataPtr(Page *pPg){
  return ((struct FilePage *)(pPg))->aData;
}*/

/*
** Write nVal as a 16-bit unsigned big-endian integer into buffer aOut.
*/
static void lsmPutU16(u8 *aOut, u16 nVal){
  aOut[0] = (u8)((nVal>>8) & 0xFF);
  aOut[1] = (u8)(nVal & 0xFF);
}

static void lsmPutU32(u8 *aOut, u32 nVal){
  aOut[0] = (u8)((nVal>>24) & 0xFF);
  aOut[1] = (u8)((nVal>>16) & 0xFF);
  aOut[2] = (u8)((nVal>> 8) & 0xFF);
  aOut[3] = (u8)((nVal    ) & 0xFF);
}

static int lsmGetU16(u8 *aOut){
  return (aOut[0] << 8) + aOut[1];
}

static u32 lsmGetU32(u8 *aOut){
  return ((u32)aOut[0] << 24) 
       + ((u32)aOut[1] << 16) 
       + ((u32)aOut[2] << 8) 
       + ((u32)aOut[3]);
}

static u64 lsmGetU64(u8 *aOut){
  return ((u64)aOut[0] << 56) 
       + ((u64)aOut[1] << 48) 
       + ((u64)aOut[2] << 40) 
       + ((u64)aOut[3] << 32) 
       + ((u64)aOut[4] << 24)
       + ((u32)aOut[5] << 16) 
       + ((u32)aOut[6] << 8) 
       + ((u32)aOut[7]);
}

static void lsmPutU64(u8 *aOut, u64 nVal){
  aOut[0] = (u8)((nVal>>56) & 0xFF);
  aOut[1] = (u8)((nVal>>48) & 0xFF);
  aOut[2] = (u8)((nVal>>40) & 0xFF);
  aOut[3] = (u8)((nVal>>32) & 0xFF);
  aOut[4] = (u8)((nVal>>24) & 0xFF);
  aOut[5] = (u8)((nVal>>16) & 0xFF);
  aOut[6] = (u8)((nVal>> 8) & 0xFF);
  aOut[7] = (u8)((nVal    ) & 0xFF);
}

static int sortedBlobGrow(lsm_env *pEnv, LsmBlob *pBlob, int nData){
  assert( pBlob->pEnv==pEnv || (pBlob->pEnv==0 && pBlob->pData==0) );
  if( pBlob->nAlloc<nData ){
    pBlob->pData = lsmReallocOrFree(pEnv, pBlob->pData, nData);
    if( !pBlob->pData ) return LSM_NOMEM_BKPT;
    pBlob->nAlloc = nData;
    pBlob->pEnv = pEnv;
  }
  return LSM_OK;
}

static int sortedBlobSet(lsm_env *pEnv, LsmBlob *pBlob, void *pData, int nData){
  if( sortedBlobGrow(pEnv, pBlob, nData) ) return LSM_NOMEM;
  memcpy(pBlob->pData, pData, nData);
  pBlob->nData = nData;
  return LSM_OK;
}

#if 0
static int sortedBlobCopy(LsmBlob *pDest, LsmBlob *pSrc){
  return sortedBlobSet(pDest, pSrc->pData, pSrc->nData);
}
#endif

static void sortedBlobFree(LsmBlob *pBlob){
  assert( pBlob->pEnv || pBlob->pData==0 );
  if( pBlob->pData ) lsmFree(pBlob->pEnv, pBlob->pData);
  memset(pBlob, 0, sizeof(LsmBlob));
}

static int sortedReadData(
  Segment *pSeg,
  Page *pPg,
  int iOff,
  int nByte,
  void **ppData,
  LsmBlob *pBlob
){
  int rc = LSM_OK;
  int iEnd;
  int nData;
  int nCell;
  u8 *aData;

  aData = fsPageData(pPg, &nData);
  nCell = lsmGetU16(&aData[SEGMENT_NRECORD_OFFSET(nData)]);
  iEnd = SEGMENT_EOF(nData, nCell);
  assert( iEnd>0 && iEnd<nData );

  if( iOff+nByte<=iEnd ){
    *ppData = (void *)&aData[iOff];
  }else{
    int nRem = nByte;
    int i = iOff;
    u8 *aDest;

    /* Make sure the blob is big enough to store the value being loaded. */
    rc = sortedBlobGrow(lsmPageEnv(pPg), pBlob, nByte);
    if( rc!=LSM_OK ) return rc;
    pBlob->nData = nByte;
    aDest = (u8 *)pBlob->pData;
    *ppData = pBlob->pData;

    /* Increment the pointer pages ref-count. */
    lsmFsPageRef(pPg);

    while( rc==LSM_OK ){
      Page *pNext;
      int flags;

      /* Copy data from pPg into the output buffer. */
      int nCopy = LSM_MIN(nRem, iEnd-i);
      if( nCopy>0 ){
        memcpy(&aDest[nByte-nRem], &aData[i], nCopy);
        nRem -= nCopy;
        i += nCopy;
        assert( nRem==0 || i==iEnd );
      }
      assert( nRem>=0 );
      if( nRem==0 ) break;
      i -= iEnd;

      /* Grab the next page in the segment */

      do {
        rc = lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
        if( rc==LSM_OK && pNext==0 ){
          rc = LSM_CORRUPT_BKPT;
        }
        if( rc ) break;
        lsmFsPageRelease(pPg);
        pPg = pNext;
        aData = fsPageData(pPg, &nData);
        flags = lsmGetU16(&aData[SEGMENT_FLAGS_OFFSET(nData)]);
      }while( flags&SEGMENT_BTREE_FLAG );

      iEnd = SEGMENT_EOF(nData, lsmGetU16(&aData[nData-2]));
      assert( iEnd>0 && iEnd<nData );
    }

    lsmFsPageRelease(pPg);
  }

  return rc;
}

static int pageGetNRec(u8 *aData, int nData){
  return (int)lsmGetU16(&aData[SEGMENT_NRECORD_OFFSET(nData)]);
}

static LsmPgno pageGetPtr(u8 *aData, int nData){
  return (LsmPgno)lsmGetU64(&aData[SEGMENT_POINTER_OFFSET(nData)]);
}

static int pageGetFlags(u8 *aData, int nData){
  return (int)lsmGetU16(&aData[SEGMENT_FLAGS_OFFSET(nData)]);
}

static u8 *pageGetCell(u8 *aData, int nData, int iCell){
  return &aData[lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, iCell)])];
}

/*
** Return the number of cells on page pPg.
*/
static int pageObjGetNRec(Page *pPg){
  int nData;
  u8 *aData = lsmFsPageData(pPg, &nData);
  return pageGetNRec(aData, nData);
}

/*
** Return the decoded (possibly relative) pointer value stored in cell 
** iCell from page aData/nData.
*/
static LsmPgno pageGetRecordPtr(u8 *aData, int nData, int iCell){
  LsmPgno iRet;                   /* Return value */
  u8 *aCell;                      /* Pointer to cell iCell */

  assert( iCell<pageGetNRec(aData, nData) && iCell>=0 );
  aCell = pageGetCell(aData, nData, iCell);
  lsmVarintGet64(&aCell[1], &iRet);
  return iRet;
}

static u8 *pageGetKey(
  Segment *pSeg,                  /* Segment pPg belongs to */
  Page *pPg,                      /* Page to read from */
  int iCell,                      /* Index of cell on page to read */
  int *piTopic,                   /* OUT: Topic associated with this key */
  int *pnKey,                     /* OUT: Size of key in bytes */
  LsmBlob *pBlob                  /* If required, use this for dynamic memory */
){
  u8 *pKey;
  i64 nDummy;
  int eType;
  u8 *aData;
  int nData;

  aData = fsPageData(pPg, &nData);

  assert( !(pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG) );
  assert( iCell<pageGetNRec(aData, nData) );

  pKey = pageGetCell(aData, nData, iCell);
  eType = *pKey++;
  pKey += lsmVarintGet64(pKey, &nDummy);
  pKey += lsmVarintGet32(pKey, pnKey);
  if( rtIsWrite(eType) ){
    pKey += lsmVarintGet64(pKey, &nDummy);
  }
  *piTopic = rtTopic(eType);

  sortedReadData(pSeg, pPg, pKey-aData, *pnKey, (void **)&pKey, pBlob);
  return pKey;
}

static int pageGetKeyCopy(
  lsm_env *pEnv,                  /* Environment handle */
  Segment *pSeg,                  /* Segment pPg belongs to */
  Page *pPg,                      /* Page to read from */
  int iCell,                      /* Index of cell on page to read */
  int *piTopic,                   /* OUT: Topic associated with this key */
  LsmBlob *pBlob                  /* If required, use this for dynamic memory */
){
  int rc = LSM_OK;
  int nKey;
  u8 *aKey;

  aKey = pageGetKey(pSeg, pPg, iCell, piTopic, &nKey, pBlob);
  assert( (void *)aKey!=pBlob->pData || nKey==pBlob->nData );
  if( (void *)aKey!=pBlob->pData ){
    rc = sortedBlobSet(pEnv, pBlob, aKey, nKey);
  }

  return rc;
}

static LsmPgno pageGetBtreeRef(Page *pPg, int iKey){
  LsmPgno iRef;
  u8 *aData;
  int nData;
  u8 *aCell;

  aData = fsPageData(pPg, &nData);
  aCell = pageGetCell(aData, nData, iKey);
  assert( aCell[0]==0 );
  aCell++;
  aCell += lsmVarintGet64(aCell, &iRef);
  lsmVarintGet64(aCell, &iRef);
  assert( iRef>0 );
  return iRef;
}

#define GETVARINT64(a, i) (((i)=((u8*)(a))[0])<=240?1:lsmVarintGet64((a), &(i)))
#define GETVARINT32(a, i) (((i)=((u8*)(a))[0])<=240?1:lsmVarintGet32((a), &(i)))

static int pageGetBtreeKey(
  Segment *pSeg,                  /* Segment page pPg belongs to */
  Page *pPg,
  int iKey, 
  LsmPgno *piPtr, 
  int *piTopic, 
  void **ppKey,
  int *pnKey,
  LsmBlob *pBlob
){
  u8 *aData;
  int nData;
  u8 *aCell;
  int eType;

  aData = fsPageData(pPg, &nData);
  assert( SEGMENT_BTREE_FLAG & pageGetFlags(aData, nData) );
  assert( iKey>=0 && iKey<pageGetNRec(aData, nData) );

  aCell = pageGetCell(aData, nData, iKey);
  eType = *aCell++;
  aCell += GETVARINT64(aCell, *piPtr);

  if( eType==0 ){
    int rc;
    LsmPgno iRef;               /* Page number of referenced page */
    Page *pRef;
    aCell += GETVARINT64(aCell, iRef);
    rc = lsmFsDbPageGet(lsmPageFS(pPg), pSeg, iRef, &pRef);
    if( rc!=LSM_OK ) return rc;
    pageGetKeyCopy(lsmPageEnv(pPg), pSeg, pRef, 0, &eType, pBlob);
    lsmFsPageRelease(pRef);
    *ppKey = pBlob->pData;
    *pnKey = pBlob->nData;
  }else{
    aCell += GETVARINT32(aCell, *pnKey);
    *ppKey = aCell;
  }
  if( piTopic ) *piTopic = rtTopic(eType);

  return LSM_OK;
}

static int btreeCursorLoadKey(BtreeCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->iPg<0 ){
    pCsr->pKey = 0;
    pCsr->nKey = 0;
    pCsr->eType = 0;
  }else{
    LsmPgno dummy;
    int iPg = pCsr->iPg;
    int iCell = pCsr->aPg[iPg].iCell;
    while( iCell<0 && (--iPg)>=0 ){
      iCell = pCsr->aPg[iPg].iCell-1;
    }
    if( iPg<0 || iCell<0 ) return LSM_CORRUPT_BKPT;

    rc = pageGetBtreeKey(
        pCsr->pSeg,
        pCsr->aPg[iPg].pPage, iCell,
        &dummy, &pCsr->eType, &pCsr->pKey, &pCsr->nKey, &pCsr->blob
    );
    pCsr->eType |= LSM_SEPARATOR;
  }

  return rc;
}

static LsmPgno btreeCursorPtr(u8 *aData, int nData, int iCell){
  int nCell;

  nCell = pageGetNRec(aData, nData);
  if( iCell>=nCell ){
    return pageGetPtr(aData, nData);
  }
  return pageGetRecordPtr(aData, nData, iCell);
}

static int btreeCursorNext(BtreeCursor *pCsr){
  int rc = LSM_OK;

  BtreePg *pPg = &pCsr->aPg[pCsr->iPg];
  int nCell; 
  u8 *aData;
  int nData;

  assert( pCsr->iPg>=0 );
  assert( pCsr->iPg==pCsr->nDepth-1 );

  aData = fsPageData(pPg->pPage, &nData);
  nCell = pageGetNRec(aData, nData);
  assert( pPg->iCell<=nCell );
  pPg->iCell++;
  if( pPg->iCell==nCell ){
    LsmPgno iLoad;

    /* Up to parent. */
    lsmFsPageRelease(pPg->pPage);
    pPg->pPage = 0;
    pCsr->iPg--;
    while( pCsr->iPg>=0 ){
      pPg = &pCsr->aPg[pCsr->iPg];
      aData = fsPageData(pPg->pPage, &nData);
      if( pPg->iCell<pageGetNRec(aData, nData) ) break;
      lsmFsPageRelease(pPg->pPage);
      pCsr->iPg--;
    }

    /* Read the key */
    rc = btreeCursorLoadKey(pCsr);

    /* Unless the cursor is at EOF, descend to cell -1 (yes, negative one) of 
    ** the left-most most descendent. */
    if( pCsr->iPg>=0 ){
      pCsr->aPg[pCsr->iPg].iCell++;

      iLoad = btreeCursorPtr(aData, nData, pPg->iCell);
      do {
        Page *pLoad;
        pCsr->iPg++;
        rc = lsmFsDbPageGet(pCsr->pFS, pCsr->pSeg, iLoad, &pLoad);
        pCsr->aPg[pCsr->iPg].pPage = pLoad;
        pCsr->aPg[pCsr->iPg].iCell = 0;
        if( rc==LSM_OK ){
          if( pCsr->iPg==(pCsr->nDepth-1) ) break;
          aData = fsPageData(pLoad, &nData);
          iLoad = btreeCursorPtr(aData, nData, 0);
        }
      }while( rc==LSM_OK && pCsr->iPg<(pCsr->nDepth-1) );
      pCsr->aPg[pCsr->iPg].iCell = -1;
    }

  }else{
    rc = btreeCursorLoadKey(pCsr);
  }

  if( rc==LSM_OK && pCsr->iPg>=0 ){
    aData = fsPageData(pCsr->aPg[pCsr->iPg].pPage, &nData);
    pCsr->iPtr = btreeCursorPtr(aData, nData, pCsr->aPg[pCsr->iPg].iCell+1);
  }

  return rc;
}

static void btreeCursorFree(BtreeCursor *pCsr){
  if( pCsr ){
    int i;
    lsm_env *pEnv = lsmFsEnv(pCsr->pFS);
    for(i=0; i<=pCsr->iPg; i++){
      lsmFsPageRelease(pCsr->aPg[i].pPage);
    }
    sortedBlobFree(&pCsr->blob);
    lsmFree(pEnv, pCsr->aPg);
    lsmFree(pEnv, pCsr);
  }
}

static int btreeCursorFirst(BtreeCursor *pCsr){
  int rc;

  Page *pPg = 0;
  FileSystem *pFS = pCsr->pFS;
  LsmPgno iPg = pCsr->pSeg->iRoot;

  do {
    rc = lsmFsDbPageGet(pFS, pCsr->pSeg, iPg, &pPg);
    assert( (rc==LSM_OK)==(pPg!=0) );
    if( rc==LSM_OK ){
      u8 *aData;
      int nData;
      int flags;

      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( (flags & SEGMENT_BTREE_FLAG)==0 ) break;

      if( (pCsr->nDepth % 8)==0 ){
        int nNew = pCsr->nDepth + 8;
        pCsr->aPg = (BtreePg *)lsmReallocOrFreeRc(
            lsmFsEnv(pFS), pCsr->aPg, sizeof(BtreePg) * nNew, &rc
        );
        if( rc==LSM_OK ){
          memset(&pCsr->aPg[pCsr->nDepth], 0, sizeof(BtreePg) * 8);
        }
      }

      if( rc==LSM_OK ){
        assert( pCsr->aPg[pCsr->nDepth].iCell==0 );
        pCsr->aPg[pCsr->nDepth].pPage = pPg;
        pCsr->nDepth++;
        iPg = pageGetRecordPtr(aData, nData, 0);
      }
    }
  }while( rc==LSM_OK );
  lsmFsPageRelease(pPg);
  pCsr->iPg = pCsr->nDepth-1;

  if( rc==LSM_OK && pCsr->nDepth ){
    pCsr->aPg[pCsr->iPg].iCell = -1;
    rc = btreeCursorNext(pCsr);
  }

  return rc;
}

static void btreeCursorPosition(BtreeCursor *pCsr, MergeInput *p){
  if( pCsr->iPg>=0 ){
    p->iPg = lsmFsPageNumber(pCsr->aPg[pCsr->iPg].pPage);
    p->iCell = ((pCsr->aPg[pCsr->iPg].iCell + 1) << 8) + pCsr->nDepth;
  }else{
    p->iPg = 0;
    p->iCell = 0;
  }
}

static void btreeCursorSplitkey(BtreeCursor *pCsr, MergeInput *p){
  int iCell = pCsr->aPg[pCsr->iPg].iCell;
  if( iCell>=0 ){
    p->iCell = iCell;
    p->iPg = lsmFsPageNumber(pCsr->aPg[pCsr->iPg].pPage);
  }else{
    int i;
    for(i=pCsr->iPg-1; i>=0; i--){
      if( pCsr->aPg[i].iCell>0 ) break;
    }
    assert( i>=0 );
    p->iCell = pCsr->aPg[i].iCell-1;
    p->iPg = lsmFsPageNumber(pCsr->aPg[i].pPage);
  }
}

static int sortedKeyCompare(
  int (*xCmp)(void *, int, void *, int),
  int iLhsTopic, void *pLhsKey, int nLhsKey,
  int iRhsTopic, void *pRhsKey, int nRhsKey
){
  int res = iLhsTopic - iRhsTopic;
  if( res==0 ){
    res = xCmp(pLhsKey, nLhsKey, pRhsKey, nRhsKey);
  }
  return res;
}

static int btreeCursorRestore(
  BtreeCursor *pCsr, 
  int (*xCmp)(void *, int, void *, int),
  MergeInput *p
){
  int rc = LSM_OK;

  if( p->iPg ){
    lsm_env *pEnv = lsmFsEnv(pCsr->pFS);
    int iCell;                    /* Current cell number on leaf page */
    LsmPgno iLeaf;                /* Page number of current leaf page */
    int nDepth;                   /* Depth of b-tree structure */
    Segment *pSeg = pCsr->pSeg;

    /* Decode the MergeInput structure */
    iLeaf = p->iPg;
    nDepth = (p->iCell & 0x00FF);
    iCell = (p->iCell >> 8) - 1;

    /* Allocate the BtreeCursor.aPg[] array */
    assert( pCsr->aPg==0 );
    pCsr->aPg = (BtreePg *)lsmMallocZeroRc(pEnv, sizeof(BtreePg) * nDepth, &rc);

    /* Populate the last entry of the aPg[] array */
    if( rc==LSM_OK ){
      Page **pp = &pCsr->aPg[nDepth-1].pPage;
      pCsr->iPg = nDepth-1;
      pCsr->nDepth = nDepth;
      pCsr->aPg[pCsr->iPg].iCell = iCell;
      rc = lsmFsDbPageGet(pCsr->pFS, pSeg, iLeaf, pp);
    }

    /* Populate any other aPg[] array entries */
    if( rc==LSM_OK && nDepth>1 ){
      LsmBlob blob = {0,0,0};
      void *pSeek;
      int nSeek;
      int iTopicSeek;
      int iPg = 0;
      LsmPgno iLoad = pSeg->iRoot;
      Page *pPg = pCsr->aPg[nDepth-1].pPage;
 
      if( pageObjGetNRec(pPg)==0 ){
        /* This can happen when pPg is the right-most leaf in the b-tree.
        ** In this case, set the iTopicSeek/pSeek/nSeek key to a value
        ** greater than any real key.  */
        assert( iCell==-1 );
        iTopicSeek = 1000;
        pSeek = 0;
        nSeek = 0;
      }else{
        LsmPgno dummy;
        rc = pageGetBtreeKey(pSeg, pPg,
            0, &dummy, &iTopicSeek, &pSeek, &nSeek, &pCsr->blob
        );
      }

      do {
        Page *pPg2;
        rc = lsmFsDbPageGet(pCsr->pFS, pSeg, iLoad, &pPg2);
        assert( rc==LSM_OK || pPg2==0 );
        if( rc==LSM_OK ){
          u8 *aData;                  /* Buffer containing page data */
          int nData;                  /* Size of aData[] in bytes */
          int iMin;
          int iMax;
          int iCell2;

          aData = fsPageData(pPg2, &nData);
          assert( (pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG) );

          iLoad = pageGetPtr(aData, nData);
          iCell2 = pageGetNRec(aData, nData); 
          iMax = iCell2-1;
          iMin = 0;

          while( iMax>=iMin ){
            int iTry = (iMin+iMax)/2;
            void *pKey; int nKey;         /* Key for cell iTry */
            int iTopic;                   /* Topic for key pKeyT/nKeyT */
            LsmPgno iPtr;                 /* Pointer for cell iTry */
            int res;                      /* (pSeek - pKeyT) */

            rc = pageGetBtreeKey(
                pSeg, pPg2, iTry, &iPtr, &iTopic, &pKey, &nKey, &blob
            );
            if( rc!=LSM_OK ) break;

            res = sortedKeyCompare(
                xCmp, iTopicSeek, pSeek, nSeek, iTopic, pKey, nKey
            );
            assert( res!=0 );

            if( res<0 ){
              iLoad = iPtr;
              iCell2 = iTry;
              iMax = iTry-1;
            }else{
              iMin = iTry+1;
            }
          }

          pCsr->aPg[iPg].pPage = pPg2;
          pCsr->aPg[iPg].iCell = iCell2;
          iPg++;
          assert( iPg!=nDepth-1 
               || lsmFsRedirectPage(pCsr->pFS, pSeg->pRedirect, iLoad)==iLeaf
          );
        }
      }while( rc==LSM_OK && iPg<(nDepth-1) );
      sortedBlobFree(&blob);
    }

    /* Load the current key and pointer */
    if( rc==LSM_OK ){
      BtreePg *pBtreePg;
      u8 *aData;
      int nData;

      pBtreePg = &pCsr->aPg[pCsr->iPg];
      aData = fsPageData(pBtreePg->pPage, &nData);
      pCsr->iPtr = btreeCursorPtr(aData, nData, pBtreePg->iCell+1);
      if( pBtreePg->iCell<0 ){
        LsmPgno dummy;
        int i;
        for(i=pCsr->iPg-1; i>=0; i--){
          if( pCsr->aPg[i].iCell>0 ) break;
        }
        assert( i>=0 );
        rc = pageGetBtreeKey(pSeg,
            pCsr->aPg[i].pPage, pCsr->aPg[i].iCell-1,
            &dummy, &pCsr->eType, &pCsr->pKey, &pCsr->nKey, &pCsr->blob
        );
        pCsr->eType |= LSM_SEPARATOR;

      }else{
        rc = btreeCursorLoadKey(pCsr);
      }
    }
  }
  return rc;
}

static int btreeCursorNew(
  lsm_db *pDb,
  Segment *pSeg,
  BtreeCursor **ppCsr
){
  int rc = LSM_OK;
  BtreeCursor *pCsr;
  
  assert( pSeg->iRoot );
  pCsr = lsmMallocZeroRc(pDb->pEnv, sizeof(BtreeCursor), &rc);
  if( pCsr ){
    pCsr->pFS = pDb->pFS;
    pCsr->pSeg = pSeg;
    pCsr->iPg = -1;
  }

  *ppCsr = pCsr;
  return rc;
}

static void segmentPtrSetPage(SegmentPtr *pPtr, Page *pNext){
  lsmFsPageRelease(pPtr->pPg);
  if( pNext ){
    int nData;
    u8 *aData = fsPageData(pNext, &nData);
    pPtr->nCell = pageGetNRec(aData, nData);
    pPtr->flags = (u16)pageGetFlags(aData, nData);
    pPtr->iPtr = pageGetPtr(aData, nData);
  }
  pPtr->pPg = pNext;
}

/*
** Load a new page into the SegmentPtr object pPtr.
*/
static int segmentPtrLoadPage(
  FileSystem *pFS,
  SegmentPtr *pPtr,              /* Load page into this SegmentPtr object */
  LsmPgno iNew                       /* Page number of new page */
){
  Page *pPg = 0;                 /* The new page */
  int rc;                        /* Return Code */

  rc = lsmFsDbPageGet(pFS, pPtr->pSeg, iNew, &pPg);
  assert( rc==LSM_OK || pPg==0 );
  segmentPtrSetPage(pPtr, pPg);

  return rc;
}

static int segmentPtrReadData(
  SegmentPtr *pPtr,
  int iOff,
  int nByte,
  void **ppData,
  LsmBlob *pBlob
){
  return sortedReadData(pPtr->pSeg, pPtr->pPg, iOff, nByte, ppData, pBlob);
}

static int segmentPtrNextPage(
  SegmentPtr *pPtr,              /* Load page into this SegmentPtr object */
  int eDir                       /* +1 for next(), -1 for prev() */
){
  Page *pNext;                   /* New page to load */
  int rc;                        /* Return code */

  assert( eDir==1 || eDir==-1 );
  assert( pPtr->pPg );
  assert( pPtr->pSeg || eDir>0 );

  rc = lsmFsDbPageNext(pPtr->pSeg, pPtr->pPg, eDir, &pNext);
  assert( rc==LSM_OK || pNext==0 );
  segmentPtrSetPage(pPtr, pNext);
  return rc;
}

static int segmentPtrLoadCell(
  SegmentPtr *pPtr,              /* Load page into this SegmentPtr object */
  int iNew                       /* Cell number of new cell */
){
  int rc = LSM_OK;
  if( pPtr->pPg ){
    u8 *aData;                    /* Pointer to page data buffer */
    int iOff;                     /* Offset in aData[] to read from */
    int nPgsz;                    /* Size of page (aData[]) in bytes */

    assert( iNew<pPtr->nCell );
    pPtr->iCell = iNew;
    aData = fsPageData(pPtr->pPg, &nPgsz);
    iOff = lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nPgsz, pPtr->iCell)]);
    pPtr->eType = aData[iOff];
    iOff++;
    iOff += GETVARINT64(&aData[iOff], pPtr->iPgPtr);
    iOff += GETVARINT32(&aData[iOff], pPtr->nKey);
    if( rtIsWrite(pPtr->eType) ){
      iOff += GETVARINT32(&aData[iOff], pPtr->nVal);
    }
    assert( pPtr->nKey>=0 );

    rc = segmentPtrReadData(
        pPtr, iOff, pPtr->nKey, &pPtr->pKey, &pPtr->blob1
    );
    if( rc==LSM_OK && rtIsWrite(pPtr->eType) ){
      rc = segmentPtrReadData(
          pPtr, iOff+pPtr->nKey, pPtr->nVal, &pPtr->pVal, &pPtr->blob2
      );
    }else{
      pPtr->nVal = 0;
      pPtr->pVal = 0;
    }
  }

  return rc;
}


static Segment *sortedSplitkeySegment(Level *pLevel){
  Merge *pMerge = pLevel->pMerge;
  MergeInput *p = &pMerge->splitkey;
  Segment *pSeg;
  int i;

  for(i=0; i<pMerge->nInput; i++){
    if( p->iPg==pMerge->aInput[i].iPg ) break;
  }
  if( pMerge->nInput==(pLevel->nRight+1) && i>=(pMerge->nInput-1) ){
    pSeg = &pLevel->pNext->lhs;
  }else{
    pSeg = &pLevel->aRhs[i];
  }

  return pSeg;
}

static void sortedSplitkey(lsm_db *pDb, Level *pLevel, int *pRc){
  Segment *pSeg;
  Page *pPg = 0;
  lsm_env *pEnv = pDb->pEnv;      /* Environment handle */
  int rc = *pRc;
  Merge *pMerge = pLevel->pMerge;

  pSeg = sortedSplitkeySegment(pLevel);
  if( rc==LSM_OK ){
    rc = lsmFsDbPageGet(pDb->pFS, pSeg, pMerge->splitkey.iPg, &pPg);
  }
  if( rc==LSM_OK ){
    int iTopic;
    LsmBlob blob = {0, 0, 0, 0};
    u8 *aData;
    int nData;
  
    aData = lsmFsPageData(pPg, &nData);
    if( pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG ){
      void *pKey;
      int nKey;
      LsmPgno dummy;
      rc = pageGetBtreeKey(pSeg,
          pPg, pMerge->splitkey.iCell, &dummy, &iTopic, &pKey, &nKey, &blob
      );
      if( rc==LSM_OK && blob.pData!=pKey ){
        rc = sortedBlobSet(pEnv, &blob, pKey, nKey);
      }
    }else{
      rc = pageGetKeyCopy(
          pEnv, pSeg, pPg, pMerge->splitkey.iCell, &iTopic, &blob
      );
    }

    pLevel->iSplitTopic = iTopic;
    pLevel->pSplitKey = blob.pData;
    pLevel->nSplitKey = blob.nData;
    lsmFsPageRelease(pPg);
  }

  *pRc = rc;
}

/*
** Reset a segment cursor. Also free its buffers if they are nThreshold
** bytes or larger in size.
*/
static void segmentPtrReset(SegmentPtr *pPtr, int nThreshold){
  lsmFsPageRelease(pPtr->pPg);
  pPtr->pPg = 0;
  pPtr->nCell = 0;
  pPtr->pKey = 0;
  pPtr->nKey = 0;
  pPtr->pVal = 0;
  pPtr->nVal = 0;
  pPtr->eType = 0;
  pPtr->iCell = 0;
  if( pPtr->blob1.nAlloc>=nThreshold ) sortedBlobFree(&pPtr->blob1);
  if( pPtr->blob2.nAlloc>=nThreshold ) sortedBlobFree(&pPtr->blob2);
}

static int segmentPtrIgnoreSeparators(MultiCursor *pCsr, SegmentPtr *pPtr){
  return (pCsr->flags & CURSOR_READ_SEPARATORS)==0
      || (pPtr!=&pCsr->aPtr[pCsr->nPtr-1]);
}

static int segmentPtrAdvance(
  MultiCursor *pCsr, 
  SegmentPtr *pPtr,
  int bReverse
){
  int eDir = (bReverse ? -1 : 1);
  Level *pLvl = pPtr->pLevel;
  do {
    int rc;
    int iCell;                    /* Number of new cell in page */
    int svFlags = 0;              /* SegmentPtr.eType before advance */

    iCell = pPtr->iCell + eDir;
    assert( pPtr->pPg );
    assert( iCell<=pPtr->nCell && iCell>=-1 );

    if( bReverse && pPtr->pSeg!=&pPtr->pLevel->lhs ){
      svFlags = pPtr->eType;
      assert( svFlags );
    }

    if( iCell>=pPtr->nCell || iCell<0 ){
      do {
        rc = segmentPtrNextPage(pPtr, eDir); 
      }while( rc==LSM_OK 
           && pPtr->pPg 
           && (pPtr->nCell==0 || (pPtr->flags & SEGMENT_BTREE_FLAG) ) 
      );
      if( rc!=LSM_OK ) return rc;
      iCell = bReverse ? (pPtr->nCell-1) : 0;
    }
    rc = segmentPtrLoadCell(pPtr, iCell);
    if( rc!=LSM_OK ) return rc;

    if( svFlags && pPtr->pPg ){
      int res = sortedKeyCompare(pCsr->pDb->xCmp,
          rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey,
          pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey
      );
      if( res<0 ) segmentPtrReset(pPtr, LSM_SEGMENTPTR_FREE_THRESHOLD);
    }

    if( pPtr->pPg==0 && (svFlags & LSM_END_DELETE) ){
      Segment *pSeg = pPtr->pSeg;
      rc = lsmFsDbPageGet(pCsr->pDb->pFS, pSeg, pSeg->iFirst, &pPtr->pPg);
      if( rc!=LSM_OK ) return rc;
      pPtr->eType = LSM_START_DELETE | LSM_POINT_DELETE;
      pPtr->eType |= (pLvl->iSplitTopic ? LSM_SYSTEMKEY : 0);
      pPtr->pKey = pLvl->pSplitKey;
      pPtr->nKey = pLvl->nSplitKey;
    }

  }while( pCsr 
       && pPtr->pPg 
       && segmentPtrIgnoreSeparators(pCsr, pPtr)
       && rtIsSeparator(pPtr->eType)
  );

  return LSM_OK;
}

static void segmentPtrEndPage(
  FileSystem *pFS, 
  SegmentPtr *pPtr, 
  int bLast, 
  int *pRc
){
  if( *pRc==LSM_OK ){
    Segment *pSeg = pPtr->pSeg;
    Page *pNew = 0;
    if( bLast ){
      *pRc = lsmFsDbPageLast(pFS, pSeg, &pNew);
    }else{
      *pRc = lsmFsDbPageGet(pFS, pSeg, pSeg->iFirst, &pNew);
    }
    segmentPtrSetPage(pPtr, pNew);
  }
}


/*
** Try to move the segment pointer passed as the second argument so that it
** points at either the first (bLast==0) or last (bLast==1) cell in the valid
** region of the segment defined by pPtr->iFirst and pPtr->iLast.
**
** Return LSM_OK if successful or an lsm error code if something goes
** wrong (IO error, OOM etc.).
*/
static int segmentPtrEnd(MultiCursor *pCsr, SegmentPtr *pPtr, int bLast){
  Level *pLvl = pPtr->pLevel;
  int rc = LSM_OK;
  FileSystem *pFS = pCsr->pDb->pFS;
  int bIgnore;

  segmentPtrEndPage(pFS, pPtr, bLast, &rc);
  while( rc==LSM_OK && pPtr->pPg 
      && (pPtr->nCell==0 || (pPtr->flags & SEGMENT_BTREE_FLAG))
  ){
    rc = segmentPtrNextPage(pPtr, (bLast ? -1 : 1));
  }

  if( rc==LSM_OK && pPtr->pPg ){
    rc = segmentPtrLoadCell(pPtr, bLast ? (pPtr->nCell-1) : 0);
    if( rc==LSM_OK && bLast && pPtr->pSeg!=&pLvl->lhs ){
      int res = sortedKeyCompare(pCsr->pDb->xCmp,
          rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey,
          pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey
      );
      if( res<0 ) segmentPtrReset(pPtr, LSM_SEGMENTPTR_FREE_THRESHOLD);
    }
  }
  
  bIgnore = segmentPtrIgnoreSeparators(pCsr, pPtr);
  if( rc==LSM_OK && pPtr->pPg && bIgnore && rtIsSeparator(pPtr->eType) ){
    rc = segmentPtrAdvance(pCsr, pPtr, bLast);
  }

#if 0
  if( bLast && rc==LSM_OK && pPtr->pPg
   && pPtr->pSeg==&pLvl->lhs 
   && pLvl->nRight && (pPtr->eType & LSM_START_DELETE)
  ){
    pPtr->iCell++;
    pPtr->eType = LSM_END_DELETE | (pLvl->iSplitTopic);
    pPtr->pKey = pLvl->pSplitKey;
    pPtr->nKey = pLvl->nSplitKey;
    pPtr->pVal = 0;
    pPtr->nVal = 0;
  }
#endif

  return rc;
}

static void segmentPtrKey(SegmentPtr *pPtr, void **ppKey, int *pnKey){
  assert( pPtr->pPg );
  *ppKey = pPtr->pKey;
  *pnKey = pPtr->nKey;
}

#if 0 /* NOT USED */
static char *keyToString(lsm_env *pEnv, void *pKey, int nKey){
  int i;
  u8 *aKey = (u8 *)pKey;
  char *zRet = (char *)lsmMalloc(pEnv, nKey+1);

  for(i=0; i<nKey; i++){
    zRet[i] = (char)(isalnum(aKey[i]) ? aKey[i] : '.');
  }
  zRet[nKey] = '\0';
  return zRet;
}
#endif

#if 0 /* NOT USED */
/*
** Check that the page that pPtr currently has loaded is the correct page
** to search for key (pKey/nKey). If it is, return 1. Otherwise, an assert
** fails and this function does not return.
*/
static int assertKeyLocation(
  MultiCursor *pCsr, 
  SegmentPtr *pPtr, 
  void *pKey, int nKey
){
  lsm_env *pEnv = lsmFsEnv(pCsr->pDb->pFS);
  LsmBlob blob = {0, 0, 0};
  int eDir;
  int iTopic = 0;                 /* TODO: Fix me */

  for(eDir=-1; eDir<=1; eDir+=2){
    Page *pTest = pPtr->pPg;

    lsmFsPageRef(pTest);
    while( pTest ){
      Segment *pSeg = pPtr->pSeg;
      Page *pNext;

      int rc = lsmFsDbPageNext(pSeg, pTest, eDir, &pNext);
      lsmFsPageRelease(pTest);
      if( rc ) return 1;
      pTest = pNext;

      if( pTest ){
        int nData;
        u8 *aData = fsPageData(pTest, &nData);
        int nCell = pageGetNRec(aData, nData);
        int flags = pageGetFlags(aData, nData);
        if( nCell && 0==(flags&SEGMENT_BTREE_FLAG) ){
          int nPgKey;
          int iPgTopic;
          u8 *pPgKey;
          int res;
          int iCell;

          iCell = ((eDir < 0) ? (nCell-1) : 0);
          pPgKey = pageGetKey(pSeg, pTest, iCell, &iPgTopic, &nPgKey, &blob);
          res = iTopic - iPgTopic;
          if( res==0 ) res = pCsr->pDb->xCmp(pKey, nKey, pPgKey, nPgKey);
          if( (eDir==1 && res>0) || (eDir==-1 && res<0) ){
            /* Taking this branch means something has gone wrong. */
            char *zMsg = lsmMallocPrintf(pEnv, "Key \"%s\" is not on page %d", 
                keyToString(pEnv, pKey, nKey), lsmFsPageNumber(pPtr->pPg)
            );
            fprintf(stderr, "%s\n", zMsg);
            assert( !"assertKeyLocation() failed" );
          }
          lsmFsPageRelease(pTest);
          pTest = 0;
        }
      }
    }
  }

  sortedBlobFree(&blob);
  return 1;
}
#endif

#ifndef NDEBUG
static int assertSeekResult(
  MultiCursor *pCsr,
  SegmentPtr *pPtr,
  int iTopic,
  void *pKey,
  int nKey,
  int eSeek
){
  if( pPtr->pPg ){
    int res;
    res = sortedKeyCompare(pCsr->pDb->xCmp, iTopic, pKey, nKey,
        rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey
    );

    if( eSeek==LSM_SEEK_EQ ) return (res==0);
    if( eSeek==LSM_SEEK_LE ) return (res>=0);
    if( eSeek==LSM_SEEK_GE ) return (res<=0);
  }

  return 1;
}
#endif

static int segmentPtrSearchOversized(
  MultiCursor *pCsr,              /* Cursor context */
  SegmentPtr *pPtr,               /* Pointer to seek */
  int iTopic,                     /* Topic of key to search for */
  void *pKey, int nKey            /* Key to seek to */
){
  int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
  int rc = LSM_OK;

  /* If the OVERSIZED flag is set, then there is no pointer in the
  ** upper level to the next page in the segment that contains at least
  ** one key. So compare the largest key on the current page with the
  ** key being sought (pKey/nKey). If (pKey/nKey) is larger, advance
  ** to the next page in the segment that contains at least one key. 
  */
  while( rc==LSM_OK && (pPtr->flags & PGFTR_SKIP_NEXT_FLAG) ){
    u8 *pLastKey;
    int nLastKey;
    int iLastTopic;
    int res;                      /* Result of comparison */
    Page *pNext;

    /* Load the last key on the current page. */
    pLastKey = pageGetKey(pPtr->pSeg,
        pPtr->pPg, pPtr->nCell-1, &iLastTopic, &nLastKey, &pPtr->blob1
    );

    /* If the loaded key is >= than (pKey/nKey), break out of the loop.
    ** If (pKey/nKey) is present in this array, it must be on the current 
    ** page.  */
    res = sortedKeyCompare(
        xCmp, iLastTopic, pLastKey, nLastKey, iTopic, pKey, nKey
    );
    if( res>=0 ) break;

    /* Advance to the next page that contains at least one key. */
    pNext = pPtr->pPg;
    lsmFsPageRef(pNext);
    while( 1 ){
      Page *pLoad;
      u8 *aData; int nData;

      rc = lsmFsDbPageNext(pPtr->pSeg, pNext, 1, &pLoad);
      lsmFsPageRelease(pNext);
      pNext = pLoad;
      if( pNext==0 ) break;

      assert( rc==LSM_OK );
      aData = lsmFsPageData(pNext, &nData);
      if( (pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG)==0
       && pageGetNRec(aData, nData)>0
      ){
        break;
      }
    }
    if( pNext==0 ) break;
    segmentPtrSetPage(pPtr, pNext);

    /* This should probably be an LSM_CORRUPT error. */
    assert( rc!=LSM_OK || (pPtr->flags & PGFTR_SKIP_THIS_FLAG) );
  }

  return rc;
}

static int ptrFwdPointer(
  Page *pPage,
  int iCell,
  Segment *pSeg,
  LsmPgno *piPtr,
  int *pbFound
){
  Page *pPg = pPage;
  int iFirst = iCell;
  int rc = LSM_OK;

  do {
    Page *pNext = 0;
    u8 *aData;
    int nData;

    aData = lsmFsPageData(pPg, &nData);
    if( (pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG)==0 ){
      int i;
      int nCell = pageGetNRec(aData, nData);
      for(i=iFirst; i<nCell; i++){
        u8 eType = *pageGetCell(aData, nData, i);
        if( (eType & LSM_START_DELETE)==0 ){
          *pbFound = 1;
          *piPtr = pageGetRecordPtr(aData, nData, i) + pageGetPtr(aData, nData);
          lsmFsPageRelease(pPg);
          return LSM_OK;
        }
      }
    }

    rc = lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
    lsmFsPageRelease(pPg);
    pPg = pNext;
    iFirst = 0;
  }while( pPg && rc==LSM_OK );
  lsmFsPageRelease(pPg);

  *pbFound = 0;
  return rc;
}

static int sortedRhsFirst(MultiCursor *pCsr, Level *pLvl, SegmentPtr *pPtr){
  int rc;
  rc = segmentPtrEnd(pCsr, pPtr, 0);
  while( pPtr->pPg && rc==LSM_OK ){
    int res = sortedKeyCompare(pCsr->pDb->xCmp,
        pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey,
        rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey
    );
    if( res<=0 ) break;
    rc = segmentPtrAdvance(pCsr, pPtr, 0);
  }
  return rc;
}


/*
** This function is called as part of a SEEK_GE op on a multi-cursor if the 
** FC pointer read from segment *pPtr comes from an entry with the 
** LSM_START_DELETE flag set. In this case the pointer value cannot be 
** trusted. Instead, the pointer that should be followed is that associated
** with the next entry in *pPtr that does not have LSM_START_DELETE set.
**
** Why the pointers can't be trusted:
**
**
**
** TODO: This is a stop-gap solution:
** 
**   At the moment, this function is called from within segmentPtrSeek(), 
**   as part of the initial lsmMCursorSeek() call. However, consider a 
**   database where the following has occurred:
**
**      1. A range delete removes keys 1..9999 using a range delete.
**      2. Keys 1 through 9999 are reinserted.
**      3. The levels containing the ops in 1. and 2. above are merged. Call
**         this level N. Level N contains FC pointers to level N+1.
**
**   Then, if the user attempts to query for (key>=2 LIMIT 10), the 
**   lsmMCursorSeek() call will iterate through 9998 entries searching for a 
**   pointer down to the level N+1 that is never actually used. It would be
**   much better if the multi-cursor could do this lazily - only seek to the
**   level (N+1) page after the user has moved the cursor on level N passed
**   the big range-delete.
*/
static int segmentPtrFwdPointer(
  MultiCursor *pCsr,              /* Multi-cursor pPtr belongs to */
  SegmentPtr *pPtr,               /* Segment-pointer to extract FC ptr from */
  LsmPgno *piPtr                  /* OUT: FC pointer value */
){
  Level *pLvl = pPtr->pLevel;
  Level *pNext = pLvl->pNext;
  Page *pPg = pPtr->pPg;
  int rc;
  int bFound;
  LsmPgno iOut = 0;

  if( pPtr->pSeg==&pLvl->lhs || pPtr->pSeg==&pLvl->aRhs[pLvl->nRight-1] ){
    if( pNext==0 
        || (pNext->nRight==0 && pNext->lhs.iRoot)
        || (pNext->nRight!=0 && pNext->aRhs[0].iRoot)
      ){
      /* Do nothing. The pointer will not be used anyway. */
      return LSM_OK;
    }
  }else{
    if( pPtr[1].pSeg->iRoot ){
      return LSM_OK;
    }
  }

  /* Search for a pointer within the current segment. */
  lsmFsPageRef(pPg);
  rc = ptrFwdPointer(pPg, pPtr->iCell, pPtr->pSeg, &iOut, &bFound);

  if( rc==LSM_OK && bFound==0 ){
    /* This case happens when pPtr points to the left-hand-side of a segment
    ** currently undergoing an incremental merge. In this case, jump to the
    ** oldest segment in the right-hand-side of the same level and continue
    ** searching. But - do not consider any keys smaller than the levels
    ** split-key. */
    SegmentPtr ptr;

    if( pPtr->pLevel->nRight==0 || pPtr->pSeg!=&pPtr->pLevel->lhs ){
      return LSM_CORRUPT_BKPT;
    }

    memset(&ptr, 0, sizeof(SegmentPtr));
    ptr.pLevel = pPtr->pLevel;
    ptr.pSeg = &ptr.pLevel->aRhs[ptr.pLevel->nRight-1];
    rc = sortedRhsFirst(pCsr, ptr.pLevel, &ptr);
    if( rc==LSM_OK ){
      rc = ptrFwdPointer(ptr.pPg, ptr.iCell, ptr.pSeg, &iOut, &bFound);
      ptr.pPg = 0;
    }
    segmentPtrReset(&ptr, 0);
  }

  *piPtr = iOut;
  return rc;
}

static int segmentPtrSeek(
  MultiCursor *pCsr,              /* Cursor context */
  SegmentPtr *pPtr,               /* Pointer to seek */
  int iTopic,                     /* Key topic to seek to */
  void *pKey, int nKey,           /* Key to seek to */
  int eSeek,                      /* Search bias - see above */
  LsmPgno *piPtr,                 /* OUT: FC pointer */
  int *pbStop
){
  int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
  int res = 0;                        /* Result of comparison operation */
  int rc = LSM_OK;
  int iMin;
  int iMax;
  LsmPgno iPtrOut = 0;

  /* If the current page contains an oversized entry, then there are no
  ** pointers to one or more of the subsequent pages in the sorted run.
  ** The following call ensures that the segment-ptr points to the correct 
  ** page in this case.  */
  rc = segmentPtrSearchOversized(pCsr, pPtr, iTopic, pKey, nKey);
  iPtrOut = pPtr->iPtr;

  /* Assert that this page is the right page of this segment for the key
  ** that we are searching for. Do this by loading page (iPg-1) and testing
  ** that pKey/nKey is greater than all keys on that page, and then by 
  ** loading (iPg+1) and testing that pKey/nKey is smaller than all
  ** the keys it houses.  
  **
  ** TODO: With range-deletes in the tree, the test described above may fail.
  */
#if 0
  assert( assertKeyLocation(pCsr, pPtr, pKey, nKey) );
#endif

  assert( pPtr->nCell>0 
       || pPtr->pSeg->nSize==1 
       || lsmFsDbPageIsLast(pPtr->pSeg, pPtr->pPg)
  );
  if( pPtr->nCell==0 ){
    segmentPtrReset(pPtr, LSM_SEGMENTPTR_FREE_THRESHOLD);
  }else{
    iMin = 0;
    iMax = pPtr->nCell-1;

    while( 1 ){
      int iTry = (iMin+iMax)/2;
      void *pKeyT; int nKeyT;       /* Key for cell iTry */
      int iTopicT;

      assert( iTry<iMax || iMin==iMax );

      rc = segmentPtrLoadCell(pPtr, iTry);
      if( rc!=LSM_OK ) break;

      segmentPtrKey(pPtr, &pKeyT, &nKeyT);
      iTopicT = rtTopic(pPtr->eType);

      res = sortedKeyCompare(xCmp, iTopicT, pKeyT, nKeyT, iTopic, pKey, nKey);
      if( res<=0 ){
        iPtrOut = pPtr->iPtr + pPtr->iPgPtr;
      }

      if( res==0 || iMin==iMax ){
        break;
      }else if( res>0 ){
        iMax = LSM_MAX(iTry-1, iMin);
      }else{
        iMin = iTry+1;
      }
    }

    if( rc==LSM_OK ){
      assert( res==0 || (iMin==iMax && iMin>=0 && iMin<pPtr->nCell) );
      if( res ){
        rc = segmentPtrLoadCell(pPtr, iMin);
      }
      assert( rc!=LSM_OK || res>0 || iPtrOut==(pPtr->iPtr + pPtr->iPgPtr) );

      if( rc==LSM_OK ){
        switch( eSeek ){
          case LSM_SEEK_EQ: {
            int eType = pPtr->eType;
            if( (res<0 && (eType & LSM_START_DELETE))
             || (res>0 && (eType & LSM_END_DELETE))
             || (res==0 && (eType & LSM_POINT_DELETE))
            ){
              *pbStop = 1;
            }else if( res==0 && (eType & LSM_INSERT) ){
              lsm_env *pEnv = pCsr->pDb->pEnv;
              *pbStop = 1;
              pCsr->eType = pPtr->eType;
              rc = sortedBlobSet(pEnv, &pCsr->key, pPtr->pKey, pPtr->nKey);
              if( rc==LSM_OK ){
                rc = sortedBlobSet(pEnv, &pCsr->val, pPtr->pVal, pPtr->nVal);
              }
              pCsr->flags |= CURSOR_SEEK_EQ;
            }
            segmentPtrReset(pPtr, LSM_SEGMENTPTR_FREE_THRESHOLD);
            break;
          }
          case LSM_SEEK_LE:
            if( res>0 ) rc = segmentPtrAdvance(pCsr, pPtr, 1);
            break;
          case LSM_SEEK_GE: {
            /* Figure out if we need to 'skip' the pointer forward or not */
            if( (res<=0 && (pPtr->eType & LSM_START_DELETE)) 
             || (res>0  && (pPtr->eType & LSM_END_DELETE)) 
            ){
              rc = segmentPtrFwdPointer(pCsr, pPtr, &iPtrOut);
            }
            if( res<0 && rc==LSM_OK ){
              rc = segmentPtrAdvance(pCsr, pPtr, 0);
            }
            break;
          }
        }
      }
    }

    /* If the cursor seek has found a separator key, and this cursor is
    ** supposed to ignore separators keys, advance to the next entry.  */
    if( rc==LSM_OK && pPtr->pPg
     && segmentPtrIgnoreSeparators(pCsr, pPtr) 
     && rtIsSeparator(pPtr->eType)
    ){
      assert( eSeek!=LSM_SEEK_EQ );
      rc = segmentPtrAdvance(pCsr, pPtr, eSeek==LSM_SEEK_LE);
    }
  }

  assert( rc!=LSM_OK || assertSeekResult(pCsr,pPtr,iTopic,pKey,nKey,eSeek) );
  *piPtr = iPtrOut;
  return rc;
}

static int seekInBtree(
  MultiCursor *pCsr,              /* Multi-cursor object */
  Segment *pSeg,                  /* Seek within this segment */
  int iTopic,
  void *pKey, int nKey,           /* Key to seek to */
  LsmPgno *aPg,                   /* OUT: Page numbers */
  Page **ppPg                     /* OUT: Leaf (sorted-run) page reference */
){
  int i = 0;
  int rc;
  LsmPgno iPg;
  Page *pPg = 0;
  LsmBlob blob = {0, 0, 0};

  iPg = pSeg->iRoot;
  do {
    LsmPgno *piFirst = 0;
    if( aPg ){
      aPg[i++] = iPg;
      piFirst = &aPg[i];
    }

    rc = lsmFsDbPageGet(pCsr->pDb->pFS, pSeg, iPg, &pPg);
    assert( rc==LSM_OK || pPg==0 );
    if( rc==LSM_OK ){
      u8 *aData;                  /* Buffer containing page data */
      int nData;                  /* Size of aData[] in bytes */
      int iMin;
      int iMax;
      int nRec;
      int flags;

      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( (flags & SEGMENT_BTREE_FLAG)==0 ) break;

      iPg = pageGetPtr(aData, nData);
      nRec = pageGetNRec(aData, nData);

      iMin = 0;
      iMax = nRec-1;
      while( iMax>=iMin ){
        int iTry = (iMin+iMax)/2;
        void *pKeyT; int nKeyT;       /* Key for cell iTry */
        int iTopicT;                  /* Topic for key pKeyT/nKeyT */
        LsmPgno iPtr;                 /* Pointer associated with cell iTry */
        int res;                      /* (pKey - pKeyT) */

        rc = pageGetBtreeKey(
            pSeg, pPg, iTry, &iPtr, &iTopicT, &pKeyT, &nKeyT, &blob
        );
        if( rc!=LSM_OK ) break;
        if( piFirst && pKeyT==blob.pData ){
          *piFirst = pageGetBtreeRef(pPg, iTry);
          piFirst = 0;
          i++;
        }

        res = sortedKeyCompare(
            pCsr->pDb->xCmp, iTopic, pKey, nKey, iTopicT, pKeyT, nKeyT
        );
        if( res<0 ){
          iPg = iPtr;
          iMax = iTry-1;
        }else{
          iMin = iTry+1;
        }
      }
      lsmFsPageRelease(pPg);
      pPg = 0;
    }
  }while( rc==LSM_OK );

  sortedBlobFree(&blob);
  assert( (rc==LSM_OK)==(pPg!=0) );
  if( ppPg ){
    *ppPg = pPg;
  }else{
    lsmFsPageRelease(pPg);
  }
  return rc;
}

static int seekInSegment(
  MultiCursor *pCsr, 
  SegmentPtr *pPtr,
  int iTopic,
  void *pKey, int nKey,
  LsmPgno iPg,                    /* Page to search */
  int eSeek,                      /* Search bias - see above */
  LsmPgno *piPtr,                 /* OUT: FC pointer */
  int *pbStop                     /* OUT: Stop search flag */
){
  LsmPgno iPtr = iPg;
  int rc = LSM_OK;

  if( pPtr->pSeg->iRoot ){
    Page *pPg;
    assert( pPtr->pSeg->iRoot!=0 );
    rc = seekInBtree(pCsr, pPtr->pSeg, iTopic, pKey, nKey, 0, &pPg);
    if( rc==LSM_OK ) segmentPtrSetPage(pPtr, pPg);
  }else{
    if( iPtr==0 ){
      iPtr = pPtr->pSeg->iFirst;
    }
    if( rc==LSM_OK ){
      rc = segmentPtrLoadPage(pCsr->pDb->pFS, pPtr, iPtr);
    }
  }

  if( rc==LSM_OK ){
    rc = segmentPtrSeek(pCsr, pPtr, iTopic, pKey, nKey, eSeek, piPtr, pbStop);
  }
  return rc;
}

/*
** Seek each segment pointer in the array of (pLvl->nRight+1) at aPtr[].
**
** pbStop:
**   This parameter is only significant if parameter eSeek is set to
**   LSM_SEEK_EQ. In this case, it is set to true before returning if
**   the seek operation is finished. This can happen in two ways:
**   
**     a) A key matching (pKey/nKey) is found, or
**     b) A point-delete or range-delete deleting the key is found.
**
**   In case (a), the multi-cursor CURSOR_SEEK_EQ flag is set and the pCsr->key
**   and pCsr->val blobs populated before returning.
*/
static int seekInLevel(
  MultiCursor *pCsr,              /* Sorted cursor object to seek */
  SegmentPtr *aPtr,               /* Pointer to array of (nRhs+1) SPs */
  int eSeek,                      /* Search bias - see above */
  int iTopic,                     /* Key topic to search for */
  void *pKey, int nKey,           /* Key to search for */
  LsmPgno *piPgno,                /* IN/OUT: fraction cascade pointer (or 0) */
  int *pbStop                     /* OUT: See above */
){
  Level *pLvl = aPtr[0].pLevel;   /* Level to seek within */
  int rc = LSM_OK;                /* Return code */
  LsmPgno iOut = 0;               /* Pointer to return to caller */
  int res = -1;                   /* Result of xCmp(pKey, split) */
  int nRhs = pLvl->nRight;        /* Number of right-hand-side segments */
  int bStop = 0;

  /* If this is a composite level (one currently undergoing an incremental
  ** merge), figure out if the search key is larger or smaller than the
  ** levels split-key.  */
  if( nRhs ){
    res = sortedKeyCompare(pCsr->pDb->xCmp, iTopic, pKey, nKey, 
        pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey
    );
  }

  /* If (res<0), then key pKey/nKey is smaller than the split-key (or this
  ** is not a composite level and there is no split-key). Search the 
  ** left-hand-side of the level in this case.  */
  if( res<0 ){
    int i;
    LsmPgno iPtr = 0;
    if( nRhs==0 ) iPtr = *piPgno;

    rc = seekInSegment(
        pCsr, &aPtr[0], iTopic, pKey, nKey, iPtr, eSeek, &iOut, &bStop
    );
    if( rc==LSM_OK && nRhs>0 && eSeek==LSM_SEEK_GE && aPtr[0].pPg==0 ){
      res = 0;
    }
    for(i=1; i<=nRhs; i++){
      segmentPtrReset(&aPtr[i], LSM_SEGMENTPTR_FREE_THRESHOLD);
    }
  }
  
  if( res>=0 ){
    int bHit = 0;                 /* True if at least one rhs is not EOF */
    LsmPgno iPtr = *piPgno;
    int i;
    segmentPtrReset(&aPtr[0], LSM_SEGMENTPTR_FREE_THRESHOLD);
    for(i=1; rc==LSM_OK && i<=nRhs && bStop==0; i++){
      SegmentPtr *pPtr = &aPtr[i];
      iOut = 0;
      rc = seekInSegment(
          pCsr, pPtr, iTopic, pKey, nKey, iPtr, eSeek, &iOut, &bStop
      );
      iPtr = iOut;

      /* If the segment-pointer has settled on a key that is smaller than
      ** the splitkey, invalidate the segment-pointer.  */
      if( pPtr->pPg ){
        res = sortedKeyCompare(pCsr->pDb->xCmp, 
            rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey, 
            pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey
        );
        if( res<0 ){
          if( pPtr->eType & LSM_START_DELETE ){
            pPtr->eType &= ~LSM_INSERT;
            pPtr->pKey = pLvl->pSplitKey;
            pPtr->nKey = pLvl->nSplitKey;
            pPtr->pVal = 0;
            pPtr->nVal = 0;
          }else{
            segmentPtrReset(pPtr, LSM_SEGMENTPTR_FREE_THRESHOLD);
          }
        }
      }

      if( aPtr[i].pKey ) bHit = 1;
    }

    if( rc==LSM_OK && eSeek==LSM_SEEK_LE && bHit==0 ){
      rc = segmentPtrEnd(pCsr, &aPtr[0], 1);
    }
  }

  assert( eSeek==LSM_SEEK_EQ || bStop==0 );
  *piPgno = iOut;
  *pbStop = bStop;
  return rc;
}

static void multiCursorGetKey(
  MultiCursor *pCsr, 
  int iKey,
  int *peType,                    /* OUT: Key type (SORTED_WRITE etc.) */
  void **ppKey,                   /* OUT: Pointer to buffer containing key */
  int *pnKey                      /* OUT: Size of *ppKey in bytes */
){
  int nKey = 0;
  void *pKey = 0;
  int eType = 0;

  switch( iKey ){
    case CURSOR_DATA_TREE0:
    case CURSOR_DATA_TREE1: {
      TreeCursor *pTreeCsr = pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0];
      if( lsmTreeCursorValid(pTreeCsr) ){
        lsmTreeCursorKey(pTreeCsr, &eType, &pKey, &nKey);
      }
      break;
    }

    case CURSOR_DATA_SYSTEM: {
      Snapshot *pWorker = pCsr->pDb->pWorker;
      if( pWorker && (pCsr->flags & CURSOR_FLUSH_FREELIST) ){
        int nEntry = pWorker->freelist.nEntry;
        if( pCsr->iFree < (nEntry*2) ){
          FreelistEntry *aEntry = pWorker->freelist.aEntry;
          int i = nEntry - 1 - (pCsr->iFree / 2);
          u32 iKey2 = 0;

          if( (pCsr->iFree % 2) ){
            eType = LSM_END_DELETE|LSM_SYSTEMKEY;
            iKey2 = aEntry[i].iBlk-1;
          }else if( aEntry[i].iId>=0 ){
            eType = LSM_INSERT|LSM_SYSTEMKEY;
            iKey2 = aEntry[i].iBlk;

            /* If the in-memory entry immediately before this one was a
             ** DELETE, and the block number is one greater than the current
             ** block number, mark this entry as an "end-delete-range". */
            if( i<(nEntry-1) && aEntry[i+1].iBlk==iKey2+1 && aEntry[i+1].iId<0 ){
              eType |= LSM_END_DELETE;
            }

          }else{
            eType = LSM_START_DELETE|LSM_SYSTEMKEY;
            iKey2 = aEntry[i].iBlk + 1;
          }

          /* If the in-memory entry immediately after this one is a
          ** DELETE, and the block number is one less than the current
          ** key, mark this entry as an "start-delete-range".  */
          if( i>0 && aEntry[i-1].iBlk==iKey2-1 && aEntry[i-1].iId<0 ){
            eType |= LSM_START_DELETE;
          }

          pKey = pCsr->pSystemVal;
          nKey = 4;
          lsmPutU32(pKey, ~iKey2);
        }
      }
      break;
    }

    default: {
      int iPtr = iKey - CURSOR_DATA_SEGMENT;
      assert( iPtr>=0 );
      if( iPtr==pCsr->nPtr ){
        if( pCsr->pBtCsr ){
          pKey = pCsr->pBtCsr->pKey;
          nKey = pCsr->pBtCsr->nKey;
          eType = pCsr->pBtCsr->eType;
        }
      }else if( iPtr<pCsr->nPtr ){
        SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
        if( pPtr->pPg ){
          pKey = pPtr->pKey;
          nKey = pPtr->nKey;
          eType = pPtr->eType;
        }
      }
      break;
    }
  }

  if( peType ) *peType = eType;
  if( pnKey ) *pnKey = nKey;
  if( ppKey ) *ppKey = pKey;
}

static int sortedDbKeyCompare(
  MultiCursor *pCsr,
  int iLhsFlags, void *pLhsKey, int nLhsKey,
  int iRhsFlags, void *pRhsKey, int nRhsKey
){
  int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
  int res;

  /* Compare the keys, including the system flag. */
  res = sortedKeyCompare(xCmp, 
    rtTopic(iLhsFlags), pLhsKey, nLhsKey,
    rtTopic(iRhsFlags), pRhsKey, nRhsKey
  );

  /* If a key has the LSM_START_DELETE flag set, but not the LSM_INSERT or
  ** LSM_POINT_DELETE flags, it is considered a delta larger. This prevents
  ** the beginning of an open-ended set from masking a database entry or
  ** delete at a lower level.  */
  if( res==0 && (pCsr->flags & CURSOR_IGNORE_DELETE) ){
    const int m = LSM_POINT_DELETE|LSM_INSERT|LSM_END_DELETE |LSM_START_DELETE;
    int iDel1 = 0;
    int iDel2 = 0;

    if( LSM_START_DELETE==(iLhsFlags & m) ) iDel1 = +1;
    if( LSM_END_DELETE  ==(iLhsFlags & m) ) iDel1 = -1;
    if( LSM_START_DELETE==(iRhsFlags & m) ) iDel2 = +1;
    if( LSM_END_DELETE  ==(iRhsFlags & m) ) iDel2 = -1;

    res = (iDel1 - iDel2);
  }

  return res;
}

static void multiCursorDoCompare(MultiCursor *pCsr, int iOut, int bReverse){
  int i1;
  int i2;
  int iRes;
  void *pKey1; int nKey1; int eType1;
  void *pKey2; int nKey2; int eType2;
  const int mul = (bReverse ? -1 : 1);

  assert( pCsr->aTree && iOut<pCsr->nTree );
  if( iOut>=(pCsr->nTree/2) ){
    i1 = (iOut - pCsr->nTree/2) * 2;
    i2 = i1 + 1;
  }else{
    i1 = pCsr->aTree[iOut*2];
    i2 = pCsr->aTree[iOut*2+1];
  }

  multiCursorGetKey(pCsr, i1, &eType1, &pKey1, &nKey1);
  multiCursorGetKey(pCsr, i2, &eType2, &pKey2, &nKey2);

  if( pKey1==0 ){
    iRes = i2;
  }else if( pKey2==0 ){
    iRes = i1;
  }else{
    int res;

    /* Compare the keys */
    res = sortedDbKeyCompare(pCsr,
        eType1, pKey1, nKey1, eType2, pKey2, nKey2
    );

    res = res * mul;
    if( res==0 ){
      /* The two keys are identical. Normally, this means that the key from
      ** the newer run clobbers the old. However, if the newer key is a
      ** separator key, or a range-delete-boundary only, do not allow it
      ** to clobber an older entry.  */
      int nc1 = (eType1 & (LSM_INSERT|LSM_POINT_DELETE))==0;
      int nc2 = (eType2 & (LSM_INSERT|LSM_POINT_DELETE))==0;
      iRes = (nc1 > nc2) ? i2 : i1;
    }else if( res<0 ){
      iRes = i1;
    }else{
      iRes = i2;
    }
  }

  pCsr->aTree[iOut] = iRes;
}

/*
** This function advances segment pointer iPtr belonging to multi-cursor
** pCsr forward (bReverse==0) or backward (bReverse!=0).
**
** If the segment pointer points to a segment that is part of a composite
** level, then the following special case is handled.
**
**   * If iPtr is the lhs of a composite level, and the cursor is being
**     advanced forwards, and segment iPtr is at EOF, move all pointers
**     that correspond to rhs segments of the same level to the first
**     key in their respective data.
*/
static int segmentCursorAdvance(
  MultiCursor *pCsr, 
  int iPtr,
  int bReverse
){
  int rc;
  SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
  Level *pLvl = pPtr->pLevel;
  int bComposite;                 /* True if pPtr is part of composite level */

  /* Advance the segment-pointer object. */
  rc = segmentPtrAdvance(pCsr, pPtr, bReverse);
  if( rc!=LSM_OK ) return rc;

  bComposite = (pLvl->nRight>0 && pCsr->nPtr>pLvl->nRight);
  if( bComposite && pPtr->pPg==0 ){
    int bFix = 0;
    if( (bReverse==0)==(pPtr->pSeg==&pLvl->lhs) ){
      int i;
      if( bReverse ){
        SegmentPtr *pLhs = &pCsr->aPtr[iPtr - 1 - (pPtr->pSeg - pLvl->aRhs)];
        for(i=0; i<pLvl->nRight; i++){
          if( pLhs[i+1].pPg ) break;
        }
        if( i==pLvl->nRight ){
          bFix = 1;
          rc = segmentPtrEnd(pCsr, pLhs, 1);
        }
      }else{
        bFix = 1;
        for(i=0; rc==LSM_OK && i<pLvl->nRight; i++){
          rc = sortedRhsFirst(pCsr, pLvl, &pCsr->aPtr[iPtr+1+i]);
        }
      }
    }

    if( bFix ){
      int i;
      for(i=pCsr->nTree-1; i>0; i--){
        multiCursorDoCompare(pCsr, i, bReverse);
      }
    }
  }

#if 0
  if( bComposite && pPtr->pSeg==&pLvl->lhs       /* lhs of composite level */
   && bReverse==0                                /* csr advanced forwards */
   && pPtr->pPg==0                               /* segment at EOF */
  ){
    int i;
    for(i=0; rc==LSM_OK && i<pLvl->nRight; i++){
      rc = sortedRhsFirst(pCsr, pLvl, &pCsr->aPtr[iPtr+1+i]);
    }
    for(i=pCsr->nTree-1; i>0; i--){
      multiCursorDoCompare(pCsr, i, 0);
    }
  }
#endif

  return rc;
}

static void mcursorFreeComponents(MultiCursor *pCsr){
  int i;
  lsm_env *pEnv = pCsr->pDb->pEnv;

  /* Close the tree cursor, if any. */
  lsmTreeCursorDestroy(pCsr->apTreeCsr[0]);
  lsmTreeCursorDestroy(pCsr->apTreeCsr[1]);

  /* Reset the segment pointers */
  for(i=0; i<pCsr->nPtr; i++){
    segmentPtrReset(&pCsr->aPtr[i], 0);
  }

  /* And the b-tree cursor, if any */
  btreeCursorFree(pCsr->pBtCsr);

  /* Free allocations */
  lsmFree(pEnv, pCsr->aPtr);
  lsmFree(pEnv, pCsr->aTree);
  lsmFree(pEnv, pCsr->pSystemVal);

  /* Zero fields */
  pCsr->nPtr = 0;
  pCsr->aPtr = 0;
  pCsr->nTree = 0;
  pCsr->aTree = 0;
  pCsr->pSystemVal = 0;
  pCsr->apTreeCsr[0] = 0;
  pCsr->apTreeCsr[1] = 0;
  pCsr->pBtCsr = 0;
}

static void lsmMCursorFreeCache(lsm_db *pDb){
  MultiCursor *p;
  MultiCursor *pNext;
  for(p=pDb->pCsrCache; p; p=pNext){
    pNext = p->pNext;
    lsmMCursorClose(p, 0);
  }
  pDb->pCsrCache = 0;
}

/*
** Close the cursor passed as the first argument.
**
** If the bCache parameter is true, then shift the cursor to the pCsrCache
** list for possible reuse instead of actually deleting it.
*/
static void lsmMCursorClose(MultiCursor *pCsr, int bCache){
  if( pCsr ){
    lsm_db *pDb = pCsr->pDb;
    MultiCursor **pp;             /* Iterator variable */

    /* The cursor may or may not be currently part of the linked list 
    ** starting at lsm_db.pCsr. If it is, extract it.  */
    for(pp=&pDb->pCsr; *pp; pp=&((*pp)->pNext)){
      if( *pp==pCsr ){
        *pp = pCsr->pNext;
        break;
      }
    }

    if( bCache ){
      int i;                      /* Used to iterate through segment-pointers */

      /* Release any page references held by this cursor. */
      assert( !pCsr->pBtCsr );
      for(i=0; i<pCsr->nPtr; i++){
        SegmentPtr *pPtr = &pCsr->aPtr[i];
        lsmFsPageRelease(pPtr->pPg);
        pPtr->pPg = 0;
      }

      /* Reset the tree cursors */
      lsmTreeCursorReset(pCsr->apTreeCsr[0]);
      lsmTreeCursorReset(pCsr->apTreeCsr[1]);

      /* Add the cursor to the pCsrCache list */
      pCsr->pNext = pDb->pCsrCache;
      pDb->pCsrCache = pCsr;
    }else{
      /* Free the allocation used to cache the current key, if any. */
      sortedBlobFree(&pCsr->key);
      sortedBlobFree(&pCsr->val);

      /* Free the component cursors */
      mcursorFreeComponents(pCsr);

      /* Free the cursor structure itself */
      lsmFree(pDb->pEnv, pCsr);
    }
  }
}

#define TREE_NONE 0
#define TREE_OLD  1
#define TREE_BOTH 2

/*
** Parameter eTree is one of TREE_OLD or TREE_BOTH.
*/
static int multiCursorAddTree(MultiCursor *pCsr, Snapshot *pSnap, int eTree){
  int rc = LSM_OK;
  lsm_db *db = pCsr->pDb;

  /* Add a tree cursor on the 'old' tree, if it exists. */
  if( eTree!=TREE_NONE 
   && lsmTreeHasOld(db) 
   && db->treehdr.iOldLog!=pSnap->iLogOff 
  ){
    rc = lsmTreeCursorNew(db, 1, &pCsr->apTreeCsr[1]);
  }

  /* Add a tree cursor on the 'current' tree, if required. */
  if( rc==LSM_OK && eTree==TREE_BOTH ){
    rc = lsmTreeCursorNew(db, 0, &pCsr->apTreeCsr[0]);
  }

  return rc;
}

static int multiCursorAddRhs(MultiCursor *pCsr, Level *pLvl){
  int i;
  int nRhs = pLvl->nRight;

  assert( pLvl->nRight>0 );
  assert( pCsr->aPtr==0 );
  pCsr->aPtr = lsmMallocZero(pCsr->pDb->pEnv, sizeof(SegmentPtr) * nRhs);
  if( !pCsr->aPtr ) return LSM_NOMEM_BKPT;
  pCsr->nPtr = nRhs;

  for(i=0; i<nRhs; i++){
    pCsr->aPtr[i].pSeg = &pLvl->aRhs[i];
    pCsr->aPtr[i].pLevel = pLvl;
  }

  return LSM_OK;
}

static void multiCursorAddOne(MultiCursor *pCsr, Level *pLvl, int *pRc){
  if( *pRc==LSM_OK ){
    int iPtr = pCsr->nPtr;
    int i;
    pCsr->aPtr[iPtr].pLevel = pLvl;
    pCsr->aPtr[iPtr].pSeg = &pLvl->lhs;
    iPtr++;
    for(i=0; i<pLvl->nRight; i++){
      pCsr->aPtr[iPtr].pLevel = pLvl;
      pCsr->aPtr[iPtr].pSeg = &pLvl->aRhs[i];
      iPtr++;
    }

    if( pLvl->nRight && pLvl->pSplitKey==0 ){
      sortedSplitkey(pCsr->pDb, pLvl, pRc);
    }
    pCsr->nPtr = iPtr;
  }
}

static int multiCursorAddAll(MultiCursor *pCsr, Snapshot *pSnap){
  Level *pLvl;
  int nPtr = 0;
  int rc = LSM_OK;

  for(pLvl=pSnap->pLevel; pLvl; pLvl=pLvl->pNext){
    /* If the LEVEL_INCOMPLETE flag is set, then this function is being
    ** called (indirectly) from within a sortedNewToplevel() call to
    ** construct pLvl. In this case ignore pLvl - this cursor is going to
    ** be used to retrieve a freelist entry from the LSM, and the partially
    ** complete level may confuse it.  */
    if( pLvl->flags & LEVEL_INCOMPLETE ) continue;
    nPtr += (1 + pLvl->nRight);
  }

  assert( pCsr->aPtr==0 );
  pCsr->aPtr = lsmMallocZeroRc(pCsr->pDb->pEnv, sizeof(SegmentPtr) * nPtr, &rc);

  for(pLvl=pSnap->pLevel; pLvl; pLvl=pLvl->pNext){
    if( (pLvl->flags & LEVEL_INCOMPLETE)==0 ){
      multiCursorAddOne(pCsr, pLvl, &rc);
    }
  }

  return rc;
}

static int multiCursorInit(MultiCursor *pCsr, Snapshot *pSnap){
  int rc;
  rc = multiCursorAddAll(pCsr, pSnap);
  if( rc==LSM_OK ){
    rc = multiCursorAddTree(pCsr, pSnap, TREE_BOTH);
  }
  pCsr->flags |= (CURSOR_IGNORE_SYSTEM | CURSOR_IGNORE_DELETE);
  return rc;
}

static MultiCursor *multiCursorNew(lsm_db *db, int *pRc){
  MultiCursor *pCsr;
  pCsr = (MultiCursor *)lsmMallocZeroRc(db->pEnv, sizeof(MultiCursor), pRc);
  if( pCsr ){
    pCsr->pNext = db->pCsr;
    db->pCsr = pCsr;
    pCsr->pDb = db;
  }
  return pCsr;
}


static void lsmSortedRemap(lsm_db *pDb){
  MultiCursor *pCsr;
  for(pCsr=pDb->pCsr; pCsr; pCsr=pCsr->pNext){
    int iPtr;
    if( pCsr->pBtCsr ){
      btreeCursorLoadKey(pCsr->pBtCsr);
    }
    for(iPtr=0; iPtr<pCsr->nPtr; iPtr++){
      segmentPtrLoadCell(&pCsr->aPtr[iPtr], pCsr->aPtr[iPtr].iCell);
    }
  }
}

static void multiCursorReadSeparators(MultiCursor *pCsr){
  if( pCsr->nPtr>0 ){
    pCsr->flags |= CURSOR_READ_SEPARATORS;
  }
}

/*
** Have this cursor skip over SORTED_DELETE entries.
*/
static void multiCursorIgnoreDelete(MultiCursor *pCsr){
  if( pCsr ) pCsr->flags |= CURSOR_IGNORE_DELETE;
}

/*
** If the free-block list is not empty, then have this cursor visit a key
** with (a) the system bit set, and (b) the key "FREELIST" and (c) a value 
** blob containing the serialized free-block list.
*/
static int multiCursorVisitFreelist(MultiCursor *pCsr){
  int rc = LSM_OK;
  pCsr->flags |= CURSOR_FLUSH_FREELIST;
  pCsr->pSystemVal = lsmMallocRc(pCsr->pDb->pEnv, 4 + 8, &rc);
  return rc;
}

/*
** Allocate and return a new database cursor.
**
** This method should only be called to allocate user cursors. As it may
** recycle a cursor from lsm_db.pCsrCache.
*/
static int lsmMCursorNew(
  lsm_db *pDb,                    /* Database handle */
  MultiCursor **ppCsr             /* OUT: Allocated cursor */
){
  MultiCursor *pCsr = 0;
  int rc = LSM_OK;

  if( pDb->pCsrCache ){
    int bOld;                     /* True if there is an old in-memory tree */

    /* Remove a cursor from the pCsrCache list and add it to the open list. */
    pCsr = pDb->pCsrCache;
    pDb->pCsrCache = pCsr->pNext;
    pCsr->pNext = pDb->pCsr;
    pDb->pCsr = pCsr;

    /* The cursor can almost be used as is, except that the old in-memory
    ** tree cursor may be present and not required, or required and not
    ** present. Fix this if required.  */
    bOld = (lsmTreeHasOld(pDb) && pDb->treehdr.iOldLog!=pDb->pClient->iLogOff);
    if( !bOld && pCsr->apTreeCsr[1] ){
      lsmTreeCursorDestroy(pCsr->apTreeCsr[1]);
      pCsr->apTreeCsr[1] = 0;
    }else if( bOld && !pCsr->apTreeCsr[1] ){
      rc = lsmTreeCursorNew(pDb, 1, &pCsr->apTreeCsr[1]);
    }

    pCsr->flags = (CURSOR_IGNORE_SYSTEM | CURSOR_IGNORE_DELETE);

  }else{
    pCsr = multiCursorNew(pDb, &rc);
    if( rc==LSM_OK ) rc = multiCursorInit(pCsr, pDb->pClient);
  }

  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr, 0);
    pCsr = 0;
  }
  assert( (rc==LSM_OK)==(pCsr!=0) );
  *ppCsr = pCsr;
  return rc;
}

static int multiCursorGetVal(
  MultiCursor *pCsr, 
  int iVal, 
  void **ppVal, 
  int *pnVal
){
  int rc = LSM_OK;

  *ppVal = 0;
  *pnVal = 0;

  switch( iVal ){
    case CURSOR_DATA_TREE0:
    case CURSOR_DATA_TREE1: {
      TreeCursor *pTreeCsr = pCsr->apTreeCsr[iVal-CURSOR_DATA_TREE0];
      if( lsmTreeCursorValid(pTreeCsr) ){
        lsmTreeCursorValue(pTreeCsr, ppVal, pnVal);
      }else{
        *ppVal = 0;
        *pnVal = 0;
      }
      break;
    }

    case CURSOR_DATA_SYSTEM: {
      Snapshot *pWorker = pCsr->pDb->pWorker;
      if( pWorker 
       && (pCsr->iFree % 2)==0
       && pCsr->iFree < (pWorker->freelist.nEntry*2)
      ){
        int iEntry = pWorker->freelist.nEntry - 1 - (pCsr->iFree / 2);
        u8 *aVal = &((u8 *)(pCsr->pSystemVal))[4];
        lsmPutU64(aVal, pWorker->freelist.aEntry[iEntry].iId);
        *ppVal = aVal;
        *pnVal = 8;
      }
      break;
    }

    default: {
      int iPtr = iVal-CURSOR_DATA_SEGMENT;
      if( iPtr<pCsr->nPtr ){
        SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
        if( pPtr->pPg ){
          *ppVal = pPtr->pVal;
          *pnVal = pPtr->nVal;
        }
      }
    }
  }

  assert( rc==LSM_OK || (*ppVal==0 && *pnVal==0) );
  return rc;
}

static int multiCursorAdvance(MultiCursor *pCsr, int bReverse);

/*
** This function is called by worker connections to walk the part of the
** free-list stored within the LSM data structure.
*/
static int lsmSortedWalkFreelist(
  lsm_db *pDb,                    /* Database handle */
  int bReverse,                   /* True to iterate from largest to smallest */
  int (*x)(void *, int, i64),     /* Callback function */
  void *pCtx                      /* First argument to pass to callback */
){
  MultiCursor *pCsr;              /* Cursor used to read db */
  int rc = LSM_OK;                /* Return Code */
  Snapshot *pSnap = 0;

  assert( pDb->pWorker );
  if( pDb->bIncrMerge ){
    rc = lsmCheckpointDeserialize(pDb, 0, pDb->pShmhdr->aSnap1, &pSnap);
    if( rc!=LSM_OK ) return rc;
  }else{
    pSnap = pDb->pWorker;
  }

  pCsr = multiCursorNew(pDb, &rc);
  if( pCsr ){
    rc = multiCursorAddAll(pCsr, pSnap);
    pCsr->flags |= CURSOR_IGNORE_DELETE;
  }
  
  if( rc==LSM_OK ){
    if( bReverse==0 ){
      rc = lsmMCursorLast(pCsr);
    }else{
      rc = lsmMCursorSeek(pCsr, 1, "", 0, LSM_SEEK_GE);
    }

    while( rc==LSM_OK && lsmMCursorValid(pCsr) && rtIsSystem(pCsr->eType) ){
      void *pKey; int nKey;
      void *pVal = 0; int nVal = 0;

      rc = lsmMCursorKey(pCsr, &pKey, &nKey);
      if( rc==LSM_OK ) rc = lsmMCursorValue(pCsr, &pVal, &nVal);
      if( rc==LSM_OK && (nKey!=4 || nVal!=8) ) rc = LSM_CORRUPT_BKPT;

      if( rc==LSM_OK ){
        int iBlk;
        i64 iSnap;
        iBlk = (int)(~(lsmGetU32((u8 *)pKey)));
        iSnap = (i64)lsmGetU64((u8 *)pVal);
        if( x(pCtx, iBlk, iSnap) ) break;
        rc = multiCursorAdvance(pCsr, !bReverse);
      }
    }
  }

  lsmMCursorClose(pCsr, 0);
  if( pSnap!=pDb->pWorker ){
    lsmFreeSnapshot(pDb->pEnv, pSnap);
  }

  return rc;
}

static int lsmSortedLoadFreelist(
  lsm_db *pDb,                    /* Database handle (must be worker) */
  void **ppVal,                   /* OUT: Blob containing LSM free-list */
  int *pnVal                      /* OUT: Size of *ppVal blob in bytes */
){
  MultiCursor *pCsr;              /* Cursor used to retreive free-list */
  int rc = LSM_OK;                /* Return Code */

  assert( pDb->pWorker );
  assert( *ppVal==0 && *pnVal==0 );

  pCsr = multiCursorNew(pDb, &rc);
  if( pCsr ){
    rc = multiCursorAddAll(pCsr, pDb->pWorker);
    pCsr->flags |= CURSOR_IGNORE_DELETE;
  }
  
  if( rc==LSM_OK ){
    rc = lsmMCursorLast(pCsr);
    if( rc==LSM_OK 
     && rtIsWrite(pCsr->eType) && rtIsSystem(pCsr->eType)
     && pCsr->key.nData==8 
     && 0==memcmp(pCsr->key.pData, "FREELIST", 8)
    ){
      void *pVal; int nVal;         /* Value read from database */
      rc = lsmMCursorValue(pCsr, &pVal, &nVal);
      if( rc==LSM_OK ){
        *ppVal = lsmMallocRc(pDb->pEnv, nVal, &rc);
        if( *ppVal ){
          memcpy(*ppVal, pVal, nVal);
          *pnVal = nVal;
        }
      }
    }

    lsmMCursorClose(pCsr, 0);
  }

  return rc;
}

static int multiCursorAllocTree(MultiCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->aTree==0 ){
    int nByte;                    /* Bytes of space to allocate */
    int nMin;                     /* Total number of cursors being merged */

    nMin = CURSOR_DATA_SEGMENT + pCsr->nPtr + (pCsr->pBtCsr!=0);
    pCsr->nTree = 2;
    while( pCsr->nTree<nMin ){
      pCsr->nTree = pCsr->nTree*2;
    }

    nByte = sizeof(int)*pCsr->nTree*2;
    pCsr->aTree = (int *)lsmMallocZeroRc(pCsr->pDb->pEnv, nByte, &rc);
  }
  return rc;
}

static void multiCursorCacheKey(MultiCursor *pCsr, int *pRc){
  if( *pRc==LSM_OK ){
    void *pKey;
    int nKey;
    multiCursorGetKey(pCsr, pCsr->aTree[1], &pCsr->eType, &pKey, &nKey);
    *pRc = sortedBlobSet(pCsr->pDb->pEnv, &pCsr->key, pKey, nKey);
  }
}

#ifdef LSM_DEBUG_EXPENSIVE
static void assertCursorTree(MultiCursor *pCsr){
  int bRev = !!(pCsr->flags & CURSOR_PREV_OK);
  int *aSave = pCsr->aTree;
  int nSave = pCsr->nTree;
  int rc;

  pCsr->aTree = 0;
  pCsr->nTree = 0;
  rc = multiCursorAllocTree(pCsr);
  if( rc==LSM_OK ){
    int i;
    for(i=pCsr->nTree-1; i>0; i--){
      multiCursorDoCompare(pCsr, i, bRev);
    }

    assert( nSave==pCsr->nTree 
        && 0==memcmp(aSave, pCsr->aTree, sizeof(int)*nSave)
    );

    lsmFree(pCsr->pDb->pEnv, pCsr->aTree);
  }

  pCsr->aTree = aSave;
  pCsr->nTree = nSave;
}
#else
# define assertCursorTree(x)
#endif

static int mcursorLocationOk(MultiCursor *pCsr, int bDeleteOk){
  int eType = pCsr->eType;
  int iKey;
  int i;
  int rdmask;
  
  assert( pCsr->flags & (CURSOR_NEXT_OK|CURSOR_PREV_OK) );
  assertCursorTree(pCsr);

  rdmask = (pCsr->flags & CURSOR_NEXT_OK) ? LSM_END_DELETE : LSM_START_DELETE;

  /* If the cursor does not currently point to an actual database key (i.e.
  ** it points to a delete key, or the start or end of a range-delete), and
  ** the CURSOR_IGNORE_DELETE flag is set, skip past this entry.  */
  if( (pCsr->flags & CURSOR_IGNORE_DELETE) && bDeleteOk==0 ){
    if( (eType & LSM_INSERT)==0 ) return 0;
  }

  /* If the cursor points to a system key (free-list entry), and the
  ** CURSOR_IGNORE_SYSTEM flag is set, skip thie entry.  */
  if( (pCsr->flags & CURSOR_IGNORE_SYSTEM) && rtTopic(eType)!=0 ){
    return 0;
  }

#ifndef NDEBUG
  /* This block fires assert() statements to check one of the assumptions
  ** in the comment below - that if the lhs sub-cursor of a level undergoing
  ** a merge is valid, then all the rhs sub-cursors must be at EOF. 
  **
  ** Also assert that all rhs sub-cursors are either at EOF or point to
  ** a key that is not less than the level split-key.  */
  for(i=0; i<pCsr->nPtr; i++){
    SegmentPtr *pPtr = &pCsr->aPtr[i];
    Level *pLvl = pPtr->pLevel;
    if( pLvl->nRight && pPtr->pPg ){
      if( pPtr->pSeg==&pLvl->lhs ){
        int j;
        for(j=0; j<pLvl->nRight; j++) assert( pPtr[j+1].pPg==0 );
      }else{
        int res = sortedKeyCompare(pCsr->pDb->xCmp, 
            rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey,
            pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey
        );
        assert( res>=0 );
      }
    }
  }
#endif

  /* Now check if this key has already been deleted by a range-delete. If 
  ** so, skip past it.
  **
  ** Assume, for the moment, that the tree contains no levels currently 
  ** undergoing incremental merge, and that this cursor is iterating forwards
  ** through the database keys. The cursor currently points to a key in
  ** level L. This key has already been deleted if any of the sub-cursors
  ** that point to levels newer than L (or to the in-memory tree) point to
  ** a key greater than the current key with the LSM_END_DELETE flag set.
  **
  ** Or, if the cursor is iterating backwards through data keys, if any
  ** such sub-cursor points to a key smaller than the current key with the
  ** LSM_START_DELETE flag set.
  **
  ** Why it works with levels undergoing a merge too:
  **
  ** When a cursor iterates forwards, the sub-cursors for the rhs of a 
  ** level are only activated once the lhs reaches EOF. So when iterating
  ** forwards, the keys visited are the same as if the level was completely
  ** merged.
  **
  ** If the cursor is iterating backwards, then the lhs sub-cursor is not 
  ** initialized until the last of the rhs sub-cursors has reached EOF.
  ** Additionally, if the START_DELETE flag is set on the last entry (in
  ** reverse order - so the entry with the smallest key) of a rhs sub-cursor,
  ** then a pseudo-key equal to the levels split-key with the END_DELETE
  ** flag set is visited by the sub-cursor.
  */ 
  iKey = pCsr->aTree[1];
  for(i=0; i<iKey; i++){
    int csrflags;
    multiCursorGetKey(pCsr, i, &csrflags, 0, 0);
    if( (rdmask & csrflags) ){
      const int SD_ED = (LSM_START_DELETE|LSM_END_DELETE);
      if( (csrflags & SD_ED)==SD_ED 
       || (pCsr->flags & CURSOR_IGNORE_DELETE)==0
      ){
        void *pKey; int nKey;
        multiCursorGetKey(pCsr, i, 0, &pKey, &nKey);
        if( 0==sortedKeyCompare(pCsr->pDb->xCmp,
              rtTopic(eType), pCsr->key.pData, pCsr->key.nData,
              rtTopic(csrflags), pKey, nKey
        )){
          continue;
        }
      }
      return 0;
    }
  }

  /* The current cursor position is one this cursor should visit. Return 1. */
  return 1;
}

static int multiCursorSetupTree(MultiCursor *pCsr, int bRev){
  int rc;

  rc = multiCursorAllocTree(pCsr);
  if( rc==LSM_OK ){
    int i;
    for(i=pCsr->nTree-1; i>0; i--){
      multiCursorDoCompare(pCsr, i, bRev);
    }
  }

  assertCursorTree(pCsr);
  multiCursorCacheKey(pCsr, &rc);

  if( rc==LSM_OK && mcursorLocationOk(pCsr, 0)==0 ){
    rc = multiCursorAdvance(pCsr, bRev);
  }
  return rc;
}


static int multiCursorEnd(MultiCursor *pCsr, int bLast){
  int rc = LSM_OK;
  int i;

  pCsr->flags &= ~(CURSOR_NEXT_OK | CURSOR_PREV_OK | CURSOR_SEEK_EQ);
  pCsr->flags |= (bLast ? CURSOR_PREV_OK : CURSOR_NEXT_OK);
  pCsr->iFree = 0;

  /* Position the two in-memory tree cursors */
  for(i=0; rc==LSM_OK && i<2; i++){
    if( pCsr->apTreeCsr[i] ){
      rc = lsmTreeCursorEnd(pCsr->apTreeCsr[i], bLast);
    }
  }

  for(i=0; rc==LSM_OK && i<pCsr->nPtr; i++){
    SegmentPtr *pPtr = &pCsr->aPtr[i];
    Level *pLvl = pPtr->pLevel;
    int iRhs;
    int bHit = 0;

    if( bLast ){
      for(iRhs=0; iRhs<pLvl->nRight && rc==LSM_OK; iRhs++){
        rc = segmentPtrEnd(pCsr, &pPtr[iRhs+1], 1);
        if( pPtr[iRhs+1].pPg ) bHit = 1;
      }
      if( bHit==0 && rc==LSM_OK ){
        rc = segmentPtrEnd(pCsr, pPtr, 1);
      }else{
        segmentPtrReset(pPtr, LSM_SEGMENTPTR_FREE_THRESHOLD);
      }
    }else{
      int bLhs = (pPtr->pSeg==&pLvl->lhs);
      assert( pPtr->pSeg==&pLvl->lhs || pPtr->pSeg==&pLvl->aRhs[0] );

      if( bLhs ){
        rc = segmentPtrEnd(pCsr, pPtr, 0);
        if( pPtr->pKey ) bHit = 1;
      }
      for(iRhs=0; iRhs<pLvl->nRight && rc==LSM_OK; iRhs++){
        if( bHit ){
          segmentPtrReset(&pPtr[iRhs+1], LSM_SEGMENTPTR_FREE_THRESHOLD);
        }else{
          rc = sortedRhsFirst(pCsr, pLvl, &pPtr[iRhs+bLhs]);
        }
      }
    }
    i += pLvl->nRight;
  }

  /* And the b-tree cursor, if applicable */
  if( rc==LSM_OK && pCsr->pBtCsr ){
    assert( bLast==0 );
    rc = btreeCursorFirst(pCsr->pBtCsr);
  }

  if( rc==LSM_OK ){
    rc = multiCursorSetupTree(pCsr, bLast);
  }
  
  return rc;
}


int mcursorSave(MultiCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->aTree ){
    int iTree = pCsr->aTree[1];
    if( iTree==CURSOR_DATA_TREE0 || iTree==CURSOR_DATA_TREE1 ){
      multiCursorCacheKey(pCsr, &rc);
    }
  }
  mcursorFreeComponents(pCsr);
  return rc;
}

int mcursorRestore(lsm_db *pDb, MultiCursor *pCsr){
  int rc;
  rc = multiCursorInit(pCsr, pDb->pClient);
  if( rc==LSM_OK && pCsr->key.pData ){
    rc = lsmMCursorSeek(pCsr, 
         rtTopic(pCsr->eType), pCsr->key.pData, pCsr->key.nData, +1
    );
  }
  return rc;
}

static int lsmSaveCursors(lsm_db *pDb){
  int rc = LSM_OK;
  MultiCursor *pCsr;

  for(pCsr=pDb->pCsr; rc==LSM_OK && pCsr; pCsr=pCsr->pNext){
    rc = mcursorSave(pCsr);
  }
  return rc;
}

static int lsmRestoreCursors(lsm_db *pDb){
  int rc = LSM_OK;
  MultiCursor *pCsr;

  for(pCsr=pDb->pCsr; rc==LSM_OK && pCsr; pCsr=pCsr->pNext){
    rc = mcursorRestore(pDb, pCsr);
  }
  return rc;
}

static int lsmMCursorFirst(MultiCursor *pCsr){
  return multiCursorEnd(pCsr, 0);
}

static int lsmMCursorLast(MultiCursor *pCsr){
  return multiCursorEnd(pCsr, 1);
}

lsm_db *lsmMCursorDb(MultiCursor *pCsr){
  return pCsr->pDb;
}

static void lsmMCursorReset(MultiCursor *pCsr){
  int i;
  lsmTreeCursorReset(pCsr->apTreeCsr[0]);
  lsmTreeCursorReset(pCsr->apTreeCsr[1]);
  for(i=0; i<pCsr->nPtr; i++){
    segmentPtrReset(&pCsr->aPtr[i], LSM_SEGMENTPTR_FREE_THRESHOLD);
  }
  pCsr->key.nData = 0;
}

static int treeCursorSeek(
  MultiCursor *pCsr,
  TreeCursor *pTreeCsr, 
  void *pKey, int nKey, 
  int eSeek,
  int *pbStop
){
  int rc = LSM_OK;
  if( pTreeCsr ){
    int res = 0;
    lsmTreeCursorSeek(pTreeCsr, pKey, nKey, &res);
    switch( eSeek ){
      case LSM_SEEK_EQ: {
        int eType = lsmTreeCursorFlags(pTreeCsr);
        if( (res<0 && (eType & LSM_START_DELETE))
         || (res>0 && (eType & LSM_END_DELETE))
         || (res==0 && (eType & LSM_POINT_DELETE))
        ){
          *pbStop = 1;
        }else if( res==0 && (eType & LSM_INSERT) ){
          lsm_env *pEnv = pCsr->pDb->pEnv;
          void *p; int n;         /* Key/value from tree-cursor */
          *pbStop = 1;
          pCsr->flags |= CURSOR_SEEK_EQ;
          rc = lsmTreeCursorKey(pTreeCsr, &pCsr->eType, &p, &n);
          if( rc==LSM_OK ) rc = sortedBlobSet(pEnv, &pCsr->key, p, n);
          if( rc==LSM_OK ) rc = lsmTreeCursorValue(pTreeCsr, &p, &n);
          if( rc==LSM_OK ) rc = sortedBlobSet(pEnv, &pCsr->val, p, n);
        }
        lsmTreeCursorReset(pTreeCsr);
        break;
      }
      case LSM_SEEK_GE:
        if( res<0 && lsmTreeCursorValid(pTreeCsr) ){
          lsmTreeCursorNext(pTreeCsr);
        }
        break;
      default:
        if( res>0 ){
          assert( lsmTreeCursorValid(pTreeCsr) );
          lsmTreeCursorPrev(pTreeCsr);
        }
        break;
    }
  }
  return rc;
}


/*
** Seek the cursor.
*/
static int lsmMCursorSeek(
  MultiCursor *pCsr, 
  int iTopic, 
  void *pKey, int nKey, 
  int eSeek
){
  int eESeek = eSeek;             /* Effective eSeek parameter */
  int bStop = 0;                  /* Set to true to halt search operation */
  int rc = LSM_OK;                /* Return code */
  int iPtr = 0;                   /* Used to iterate through pCsr->aPtr[] */
  LsmPgno iPgno = 0;              /* FC pointer value */

  assert( pCsr->apTreeCsr[0]==0 || iTopic==0 );
  assert( pCsr->apTreeCsr[1]==0 || iTopic==0 );

  if( eESeek==LSM_SEEK_LEFAST ) eESeek = LSM_SEEK_LE;

  assert( eESeek==LSM_SEEK_EQ || eESeek==LSM_SEEK_LE || eESeek==LSM_SEEK_GE );
  assert( (pCsr->flags & CURSOR_FLUSH_FREELIST)==0 );
  assert( pCsr->nPtr==0 || pCsr->aPtr[0].pLevel );

  pCsr->flags &= ~(CURSOR_NEXT_OK | CURSOR_PREV_OK | CURSOR_SEEK_EQ);
  rc = treeCursorSeek(pCsr, pCsr->apTreeCsr[0], pKey, nKey, eESeek, &bStop);
  if( rc==LSM_OK && bStop==0 ){
    rc = treeCursorSeek(pCsr, pCsr->apTreeCsr[1], pKey, nKey, eESeek, &bStop);
  }

  /* Seek all segment pointers. */
  for(iPtr=0; iPtr<pCsr->nPtr && rc==LSM_OK && bStop==0; iPtr++){
    SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
    assert( pPtr->pSeg==&pPtr->pLevel->lhs );
    rc = seekInLevel(pCsr, pPtr, eESeek, iTopic, pKey, nKey, &iPgno, &bStop);
    iPtr += pPtr->pLevel->nRight;
  }

  if( eSeek!=LSM_SEEK_EQ ){
    if( rc==LSM_OK ){
      rc = multiCursorAllocTree(pCsr);
    }
    if( rc==LSM_OK ){
      int i;
      for(i=pCsr->nTree-1; i>0; i--){
        multiCursorDoCompare(pCsr, i, eESeek==LSM_SEEK_LE);
      }
      if( eSeek==LSM_SEEK_GE ) pCsr->flags |= CURSOR_NEXT_OK;
      if( eSeek==LSM_SEEK_LE ) pCsr->flags |= CURSOR_PREV_OK;
    }

    multiCursorCacheKey(pCsr, &rc);
    if( rc==LSM_OK && eSeek!=LSM_SEEK_LEFAST && 0==mcursorLocationOk(pCsr, 0) ){
      switch( eESeek ){
        case LSM_SEEK_EQ:
          lsmMCursorReset(pCsr);
          break;
        case LSM_SEEK_GE:
          rc = lsmMCursorNext(pCsr);
          break;
        default:
          rc = lsmMCursorPrev(pCsr);
          break;
      }
    }
  }

  return rc;
}

static int lsmMCursorValid(MultiCursor *pCsr){
  int res = 0;
  if( pCsr->flags & CURSOR_SEEK_EQ ){
    res = 1;
  }else if( pCsr->aTree ){
    int iKey = pCsr->aTree[1];
    if( iKey==CURSOR_DATA_TREE0 || iKey==CURSOR_DATA_TREE1 ){
      res = lsmTreeCursorValid(pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0]);
    }else{
      void *pKey; 
      multiCursorGetKey(pCsr, iKey, 0, &pKey, 0);
      res = pKey!=0;
    }
  }
  return res;
}

static int mcursorAdvanceOk(
  MultiCursor *pCsr, 
  int bReverse,
  int *pRc
){
  void *pNew;                     /* Pointer to buffer containing new key */
  int nNew;                       /* Size of buffer pNew in bytes */
  int eNewType;                   /* Type of new record */

  if( *pRc ) return 1;

  /* Check the current key value. If it is not greater than (if bReverse==0)
  ** or less than (if bReverse!=0) the key currently cached in pCsr->key, 
  ** then the cursor has not yet been successfully advanced.  
  */
  multiCursorGetKey(pCsr, pCsr->aTree[1], &eNewType, &pNew, &nNew);
  if( pNew ){
    int typemask = (pCsr->flags & CURSOR_IGNORE_DELETE) ? ~(0) : LSM_SYSTEMKEY;
    int res = sortedDbKeyCompare(pCsr,
      eNewType & typemask, pNew, nNew, 
      pCsr->eType & typemask, pCsr->key.pData, pCsr->key.nData
    );

    if( (bReverse==0 && res<=0) || (bReverse!=0 && res>=0) ){
      return 0;
    }

    multiCursorCacheKey(pCsr, pRc);
    assert( pCsr->eType==eNewType );

    /* If this cursor is configured to skip deleted keys, and the current
    ** cursor points to a SORTED_DELETE entry, then the cursor has not been 
    ** successfully advanced.  
    **
    ** Similarly, if the cursor is configured to skip system keys and the
    ** current cursor points to a system key, it has not yet been advanced.
    */
    if( *pRc==LSM_OK && 0==mcursorLocationOk(pCsr, 0) ) return 0;
  }
  return 1;
}

static void flCsrAdvance(MultiCursor *pCsr){
  assert( pCsr->flags & CURSOR_FLUSH_FREELIST );
  if( pCsr->iFree % 2 ){
    pCsr->iFree++;
  }else{
    int nEntry = pCsr->pDb->pWorker->freelist.nEntry;
    FreelistEntry *aEntry = pCsr->pDb->pWorker->freelist.aEntry;

    int i = nEntry - 1 - (pCsr->iFree / 2);

    /* If the current entry is a delete and the "end-delete" key will not
    ** be attached to the next entry, increment iFree by 1 only. */
    if( aEntry[i].iId<0 ){
      while( 1 ){
        if( i==0 || aEntry[i-1].iBlk!=aEntry[i].iBlk-1 ){
          pCsr->iFree--;
          break;
        }
        if( aEntry[i-1].iId>=0 ) break;
        pCsr->iFree += 2;
        i--;
      }
    }
    pCsr->iFree += 2;
  }
}

static int multiCursorAdvance(MultiCursor *pCsr, int bReverse){
  int rc = LSM_OK;                /* Return Code */
  if( lsmMCursorValid(pCsr) ){
    do {
      int iKey = pCsr->aTree[1];

      assertCursorTree(pCsr);

      /* If this multi-cursor is advancing forwards, and the sub-cursor
      ** being advanced is the one that separator keys may be being read
      ** from, record the current absolute pointer value.  */
      if( pCsr->pPrevMergePtr ){
        if( iKey==(CURSOR_DATA_SEGMENT+pCsr->nPtr) ){
          assert( pCsr->pBtCsr );
          *pCsr->pPrevMergePtr = pCsr->pBtCsr->iPtr;
        }else if( pCsr->pBtCsr==0 && pCsr->nPtr>0
               && iKey==(CURSOR_DATA_SEGMENT+pCsr->nPtr-1) 
        ){
          SegmentPtr *pPtr = &pCsr->aPtr[iKey-CURSOR_DATA_SEGMENT];
          *pCsr->pPrevMergePtr = pPtr->iPtr+pPtr->iPgPtr;
        }
      }

      if( iKey==CURSOR_DATA_TREE0 || iKey==CURSOR_DATA_TREE1 ){
        TreeCursor *pTreeCsr = pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0];
        if( bReverse ){
          rc = lsmTreeCursorPrev(pTreeCsr);
        }else{
          rc = lsmTreeCursorNext(pTreeCsr);
        }
      }else if( iKey==CURSOR_DATA_SYSTEM ){
        assert( pCsr->flags & CURSOR_FLUSH_FREELIST );
        assert( bReverse==0 );
        flCsrAdvance(pCsr);
      }else if( iKey==(CURSOR_DATA_SEGMENT+pCsr->nPtr) ){
        assert( bReverse==0 && pCsr->pBtCsr );
        rc = btreeCursorNext(pCsr->pBtCsr);
      }else{
        rc = segmentCursorAdvance(pCsr, iKey-CURSOR_DATA_SEGMENT, bReverse);
      }
      if( rc==LSM_OK ){
        int i;
        for(i=(iKey+pCsr->nTree)/2; i>0; i=i/2){
          multiCursorDoCompare(pCsr, i, bReverse);
        }
        assertCursorTree(pCsr);
      }
    }while( mcursorAdvanceOk(pCsr, bReverse, &rc)==0 );
  }
  return rc;
}

static int lsmMCursorNext(MultiCursor *pCsr){
  if( (pCsr->flags & CURSOR_NEXT_OK)==0 ) return LSM_MISUSE_BKPT;
  return multiCursorAdvance(pCsr, 0);
}

static int lsmMCursorPrev(MultiCursor *pCsr){
  if( (pCsr->flags & CURSOR_PREV_OK)==0 ) return LSM_MISUSE_BKPT;
  return multiCursorAdvance(pCsr, 1);
}

static int lsmMCursorKey(MultiCursor *pCsr, void **ppKey, int *pnKey){
  if( (pCsr->flags & CURSOR_SEEK_EQ) || pCsr->aTree==0 ){
    *pnKey = pCsr->key.nData;
    *ppKey = pCsr->key.pData;
  }else{
    int iKey = pCsr->aTree[1];

    if( iKey==CURSOR_DATA_TREE0 || iKey==CURSOR_DATA_TREE1 ){
      TreeCursor *pTreeCsr = pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0];
      lsmTreeCursorKey(pTreeCsr, 0, ppKey, pnKey);
    }else{
      int nKey;

#ifndef NDEBUG
      void *pKey;
      int eType;
      multiCursorGetKey(pCsr, iKey, &eType, &pKey, &nKey);
      assert( eType==pCsr->eType );
      assert( nKey==pCsr->key.nData );
      assert( memcmp(pKey, pCsr->key.pData, nKey)==0 );
#endif

      nKey = pCsr->key.nData;
      if( nKey==0 ){
        *ppKey = 0;
      }else{
        *ppKey = pCsr->key.pData;
      }
      *pnKey = nKey; 
    }
  }
  return LSM_OK;
}

/*
** Compare the current key that cursor csr points to with pKey/nKey. Set
** *piRes to the result and return LSM_OK.
*/
int lsm_csr_cmp(lsm_cursor *csr, const void *pKey, int nKey, int *piRes){
  MultiCursor *pCsr = (MultiCursor *)csr;
  void *pCsrkey; int nCsrkey;
  int rc;
  rc = lsmMCursorKey(pCsr, &pCsrkey, &nCsrkey);
  if( rc==LSM_OK ){
    int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
    *piRes = sortedKeyCompare(xCmp, 0, pCsrkey, nCsrkey, 0, (void *)pKey, nKey);
  }
  return rc;
}

static int lsmMCursorValue(MultiCursor *pCsr, void **ppVal, int *pnVal){
  void *pVal;
  int nVal;
  int rc;
  if( (pCsr->flags & CURSOR_SEEK_EQ) || pCsr->aTree==0 ){
    rc = LSM_OK;
    nVal = pCsr->val.nData;
    pVal = pCsr->val.pData;
  }else{

    assert( pCsr->aTree );
    assert( mcursorLocationOk(pCsr, (pCsr->flags & CURSOR_IGNORE_DELETE)) );

    rc = multiCursorGetVal(pCsr, pCsr->aTree[1], &pVal, &nVal);
    if( pVal && rc==LSM_OK ){
      rc = sortedBlobSet(pCsr->pDb->pEnv, &pCsr->val, pVal, nVal);
      pVal = pCsr->val.pData;
    }

    if( rc!=LSM_OK ){
      pVal = 0;
      nVal = 0;
    }
  }
  *ppVal = pVal;
  *pnVal = nVal;
  return rc;
}

static int lsmMCursorType(MultiCursor *pCsr, int *peType){
  assert( pCsr->aTree );
  multiCursorGetKey(pCsr, pCsr->aTree[1], peType, 0, 0);
  return LSM_OK;
}

/*
** Buffer aData[], size nData, is assumed to contain a valid b-tree 
** hierarchy page image. Return the offset in aData[] of the next free
** byte in the data area (where a new cell may be written if there is
** space).
*/
static int mergeWorkerPageOffset(u8 *aData, int nData){
  int nRec;
  int iOff;
  int nKey;
  int eType;
  i64 nDummy;


  nRec = lsmGetU16(&aData[SEGMENT_NRECORD_OFFSET(nData)]);
  iOff = lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec-1)]);
  eType = aData[iOff++];
  assert( eType==0 
       || eType==(LSM_SYSTEMKEY|LSM_SEPARATOR) 
       || eType==(LSM_SEPARATOR)
  );

  iOff += lsmVarintGet64(&aData[iOff], &nDummy);
  iOff += lsmVarintGet32(&aData[iOff], &nKey);

  return iOff + (eType ? nKey : 0);
}

/*
** Following a checkpoint operation, database pages that are part of the
** checkpointed state of the LSM are deemed read-only. This includes the
** right-most page of the b-tree hierarchy of any separators array under
** construction, and all pages between it and the b-tree root, inclusive.
** This is a problem, as when further pages are appended to the separators
** array, entries must be added to the indicated b-tree hierarchy pages.
**
** This function copies all such b-tree pages to new locations, so that
** they can be modified as required.
**
** The complication is that not all database pages are the same size - due
** to the way the file.c module works some (the first and last in each block)
** are 4 bytes smaller than the others.
*/
static int mergeWorkerMoveHierarchy(
  MergeWorker *pMW,               /* Merge worker */
  int bSep                        /* True for separators run */
){
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  int rc = LSM_OK;                /* Return code */
  int i;
  Page **apHier = pMW->hier.apHier;
  int nHier = pMW->hier.nHier;

  for(i=0; rc==LSM_OK && i<nHier; i++){
    Page *pNew = 0;
    rc = lsmFsSortedAppend(pDb->pFS, pDb->pWorker, pMW->pLevel, 1, &pNew);
    assert( rc==LSM_OK );

    if( rc==LSM_OK ){
      u8 *a1; int n1;
      u8 *a2; int n2;

      a1 = fsPageData(pNew, &n1);
      a2 = fsPageData(apHier[i], &n2);

      assert( n1==n2 || n1+4==n2 );

      if( n1==n2 ){
        memcpy(a1, a2, n2);
      }else{
        int nEntry = pageGetNRec(a2, n2);
        int iEof1 = SEGMENT_EOF(n1, nEntry);
        int iEof2 = SEGMENT_EOF(n2, nEntry);

        memcpy(a1, a2, iEof2 - 4);
        memcpy(&a1[iEof1], &a2[iEof2], n2 - iEof2);
      }

      lsmFsPageRelease(apHier[i]);
      apHier[i] = pNew;

#if 0
      assert( n1==n2 || n1+4==n2 || n2+4==n1 );
      if( n1>=n2 ){
        /* If n1 (size of the new page) is equal to or greater than n2 (the
        ** size of the old page), then copy the data into the new page. If
        ** n1==n2, this could be done with a single memcpy(). However, 
        ** since sometimes n1>n2, the page content and footer must be copied 
        ** separately. */
        int nEntry = pageGetNRec(a2, n2);
        int iEof1 = SEGMENT_EOF(n1, nEntry);
        int iEof2 = SEGMENT_EOF(n2, nEntry);
        memcpy(a1, a2, iEof2);
        memcpy(&a1[iEof1], &a2[iEof2], n2 - iEof2);
        lsmFsPageRelease(apHier[i]);
        apHier[i] = pNew;
      }else{
        lsmPutU16(&a1[SEGMENT_FLAGS_OFFSET(n1)], SEGMENT_BTREE_FLAG);
        lsmPutU16(&a1[SEGMENT_NRECORD_OFFSET(n1)], 0);
        lsmPutU64(&a1[SEGMENT_POINTER_OFFSET(n1)], 0);
        i = i - 1;
        lsmFsPageRelease(pNew);
      }
#endif
    }
  }

#ifdef LSM_DEBUG
  if( rc==LSM_OK ){
    for(i=0; i<nHier; i++) assert( lsmFsPageWritable(apHier[i]) );
  }
#endif

  return rc;
}

/*
** Allocate and populate the MergeWorker.apHier[] array.
*/
static int mergeWorkerLoadHierarchy(MergeWorker *pMW){
  int rc = LSM_OK;
  Segment *pSeg;
  Hierarchy *p;
 
  pSeg = &pMW->pLevel->lhs;
  p = &pMW->hier;

  if( p->apHier==0 && pSeg->iRoot!=0 ){
    FileSystem *pFS = pMW->pDb->pFS;
    lsm_env *pEnv = pMW->pDb->pEnv;
    Page **apHier = 0;
    int nHier = 0;
    LsmPgno iPg = pSeg->iRoot;

    do {
      Page *pPg = 0;
      u8 *aData;
      int nData;
      int flags;

      rc = lsmFsDbPageGet(pFS, pSeg, iPg, &pPg);
      if( rc!=LSM_OK ) break;

      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( flags&SEGMENT_BTREE_FLAG ){
        Page **apNew = (Page **)lsmRealloc(
            pEnv, apHier, sizeof(Page *)*(nHier+1)
        );
        if( apNew==0 ){
          rc = LSM_NOMEM_BKPT;
          break;
        }
        apHier = apNew;
        memmove(&apHier[1], &apHier[0], sizeof(Page *) * nHier);
        nHier++;

        apHier[0] = pPg;
        iPg = pageGetPtr(aData, nData);
      }else{
        lsmFsPageRelease(pPg);
        break;
      }
    }while( 1 );

    if( rc==LSM_OK ){
      u8 *aData;
      int nData;
      aData = fsPageData(apHier[0], &nData);
      pMW->aSave[0].iPgno = pageGetPtr(aData, nData);
      p->nHier = nHier;
      p->apHier = apHier;
      rc = mergeWorkerMoveHierarchy(pMW, 0);
    }else{
      int i;
      for(i=0; i<nHier; i++){
        lsmFsPageRelease(apHier[i]);
      }
      lsmFree(pEnv, apHier);
    }
  }

  return rc;
}

/*
** B-tree pages use almost the same format as regular pages. The 
** differences are:
**
**   1. The record format is (usually, see below) as follows:
**
**         + Type byte (always SORTED_SEPARATOR or SORTED_SYSTEM_SEPARATOR),
**         + Absolute pointer value (varint),
**         + Number of bytes in key (varint),
**         + LsmBlob containing key data.
**
**   2. All pointer values are stored as absolute values (not offsets 
**      relative to the footer pointer value).
**
**   3. Each pointer that is part of a record points to a page that 
**      contains keys smaller than the records key (note: not "equal to or
**      smaller than - smaller than").
**
**   4. The pointer in the page footer of a b-tree page points to a page
**      that contains keys equal to or larger than the largest key on the
**      b-tree page.
**
** The reason for having the page footer pointer point to the right-child
** (instead of the left) is that doing things this way makes the 
** mergeWorkerMoveHierarchy() operation less complicated (since the pointers 
** that need to be updated are all stored as fixed-size integers within the 
** page footer, not varints in page records).
**
** Records may not span b-tree pages. If this function is called to add a
** record larger than (page-size / 4) bytes, then a pointer to the indexed
** array page that contains the main record is added to the b-tree instead.
** In this case the record format is:
**
**         + 0x00 byte (1 byte) 
**         + Absolute pointer value (varint),
**         + Absolute page number of page containing key (varint).
**
** See function seekInBtree() for the code that traverses b-tree pages.
*/

static int mergeWorkerBtreeWrite(
  MergeWorker *pMW,
  u8 eType,
  LsmPgno iPtr,
  LsmPgno iKeyPg,
  void *pKey,
  int nKey
){
  Hierarchy *p = &pMW->hier;
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  int rc = LSM_OK;                /* Return Code */
  int iLevel;                     /* Level of b-tree hierachy to write to */
  int nData;                      /* Size of aData[] in bytes */
  u8 *aData;                      /* Page data for level iLevel */
  int iOff;                       /* Offset on b-tree page to write record to */
  int nRec;                       /* Initial number of records on b-tree page */

  /* iKeyPg should be zero for an ordinary b-tree key, or non-zero for an
  ** indirect key. The flags byte for an indirect key is 0x00.  */
  assert( (eType==0)==(iKeyPg!=0) );

  /* The MergeWorker.apHier[] array contains the right-most leaf of the b-tree
  ** hierarchy, the root node, and all nodes that lie on the path between.
  ** apHier[0] is the right-most leaf and apHier[pMW->nHier-1] is the current
  ** root page.
  **
  ** This loop searches for a node with enough space to store the key on,
  ** starting with the leaf and iterating up towards the root. When the loop
  ** exits, the key may be written to apHier[iLevel].  */
  for(iLevel=0; iLevel<=p->nHier; iLevel++){
    int nByte;                    /* Number of free bytes required */

    if( iLevel==p->nHier ){
      /* Extend the array and allocate a new root page. */
      Page **aNew;
      aNew = (Page **)lsmRealloc(
          pMW->pDb->pEnv, p->apHier, sizeof(Page *)*(p->nHier+1)
      );
      if( !aNew ){
        return LSM_NOMEM_BKPT;
      }
      p->apHier = aNew;
    }else{
      Page *pOld;
      int nFree;

      /* If the key will fit on this page, break out of the loop here.
      ** The new entry will be written to page apHier[iLevel]. */
      pOld = p->apHier[iLevel];
      assert( lsmFsPageWritable(pOld) );
      aData = fsPageData(pOld, &nData);
      if( eType==0 ){
        nByte = 2 + 1 + lsmVarintLen64(iPtr) + lsmVarintLen64(iKeyPg);
      }else{
        nByte = 2 + 1 + lsmVarintLen64(iPtr) + lsmVarintLen32(nKey) + nKey;
      }

      nRec = pageGetNRec(aData, nData);
      nFree = SEGMENT_EOF(nData, nRec) - mergeWorkerPageOffset(aData, nData);
      if( nByte<=nFree ) break;

      /* Otherwise, this page is full. Set the right-hand-child pointer
      ** to iPtr and release it.  */
      lsmPutU64(&aData[SEGMENT_POINTER_OFFSET(nData)], iPtr);
      assert( lsmFsPageNumber(pOld)==0 );
      rc = lsmFsPagePersist(pOld);
      if( rc==LSM_OK ){
        iPtr = lsmFsPageNumber(pOld);
        lsmFsPageRelease(pOld);
      }
    }

    /* Allocate a new page for apHier[iLevel]. */
    p->apHier[iLevel] = 0;
    if( rc==LSM_OK ){
      rc = lsmFsSortedAppend(
          pDb->pFS, pDb->pWorker, pMW->pLevel, 1, &p->apHier[iLevel]
      );
    }
    if( rc!=LSM_OK ) return rc;

    aData = fsPageData(p->apHier[iLevel], &nData);
    memset(aData, 0, nData);
    lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], SEGMENT_BTREE_FLAG);
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], 0);

    if( iLevel==p->nHier ){
      p->nHier++;
      break;
    }
  }

  /* Write the key into page apHier[iLevel]. */
  aData = fsPageData(p->apHier[iLevel], &nData);
  iOff = mergeWorkerPageOffset(aData, nData);
  nRec = pageGetNRec(aData, nData);
  lsmPutU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec)], (u16)iOff);
  lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], (u16)(nRec+1));
  if( eType==0 ){
    aData[iOff++] = 0x00;
    iOff += lsmVarintPut64(&aData[iOff], iPtr);
    iOff += lsmVarintPut64(&aData[iOff], iKeyPg);
  }else{
    aData[iOff++] = eType;
    iOff += lsmVarintPut64(&aData[iOff], iPtr);
    iOff += lsmVarintPut32(&aData[iOff], nKey);
    memcpy(&aData[iOff], pKey, nKey);
  }

  return rc;
}

static int mergeWorkerBtreeIndirect(MergeWorker *pMW){
  int rc = LSM_OK;
  if( pMW->iIndirect ){
    LsmPgno iKeyPg = pMW->aSave[1].iPgno;
    rc = mergeWorkerBtreeWrite(pMW, 0, pMW->iIndirect, iKeyPg, 0, 0);
    pMW->iIndirect = 0;
  }
  return rc;
}

/*
** Append the database key (iTopic/pKey/nKey) to the b-tree under 
** construction. This key has not yet been written to a segment page.
** The pointer that will accompany the new key in the b-tree - that
** points to the completed segment page that contains keys smaller than
** (pKey/nKey) is currently stored in pMW->aSave[0].iPgno.
*/
static int mergeWorkerPushHierarchy(
  MergeWorker *pMW,               /* Merge worker object */
  int iTopic,                     /* Topic value for this key */
  void *pKey,                     /* Pointer to key buffer */
  int nKey                        /* Size of pKey buffer in bytes */
){
  int rc = LSM_OK;                /* Return Code */
  LsmPgno iPtr;                   /* Pointer value to accompany pKey/nKey */

  assert( pMW->aSave[0].bStore==0 );
  assert( pMW->aSave[1].bStore==0 );
  rc = mergeWorkerBtreeIndirect(pMW);

  /* Obtain the absolute pointer value to store along with the key in the
  ** page body. This pointer points to a page that contains keys that are
  ** smaller than pKey/nKey.  */
  iPtr = pMW->aSave[0].iPgno;
  assert( iPtr!=0 );

  /* Determine if the indirect format should be used. */
  if( (nKey*4 > lsmFsPageSize(pMW->pDb->pFS)) ){
    pMW->iIndirect = iPtr;
    pMW->aSave[1].bStore = 1;
  }else{
    rc = mergeWorkerBtreeWrite(
        pMW, (u8)(iTopic | LSM_SEPARATOR), iPtr, 0, pKey, nKey
    );
  }

  /* Ensure that the SortedRun.iRoot field is correct. */
  return rc;
}

static int mergeWorkerFinishHierarchy(
  MergeWorker *pMW                /* Merge worker object */
){
  int i;                          /* Used to loop through apHier[] */
  int rc = LSM_OK;                /* Return code */
  LsmPgno iPtr;                   /* New right-hand-child pointer value */

  iPtr = pMW->aSave[0].iPgno;
  for(i=0; i<pMW->hier.nHier && rc==LSM_OK; i++){
    Page *pPg = pMW->hier.apHier[i];
    int nData;                    /* Size of aData[] in bytes */
    u8 *aData;                    /* Page data for pPg */

    aData = fsPageData(pPg, &nData);
    lsmPutU64(&aData[SEGMENT_POINTER_OFFSET(nData)], iPtr);

    rc = lsmFsPagePersist(pPg);
    iPtr = lsmFsPageNumber(pPg);
    lsmFsPageRelease(pPg);
  }

  if( pMW->hier.nHier ){
    pMW->pLevel->lhs.iRoot = iPtr;
    lsmFree(pMW->pDb->pEnv, pMW->hier.apHier);
    pMW->hier.apHier = 0;
    pMW->hier.nHier = 0;
  }

  return rc;
}

static int mergeWorkerAddPadding(
  MergeWorker *pMW                /* Merge worker object */
){
  FileSystem *pFS = pMW->pDb->pFS;
  return lsmFsSortedPadding(pFS, pMW->pDb->pWorker, &pMW->pLevel->lhs);
}

/*
** Release all page references currently held by the merge-worker passed
** as the only argument. Unless an error has occurred, all pages have
** already been released.
*/
static void mergeWorkerReleaseAll(MergeWorker *pMW){
  int i;
  lsmFsPageRelease(pMW->pPage);
  pMW->pPage = 0;

  for(i=0; i<pMW->hier.nHier; i++){
    lsmFsPageRelease(pMW->hier.apHier[i]);
    pMW->hier.apHier[i] = 0;
  }
  lsmFree(pMW->pDb->pEnv, pMW->hier.apHier);
  pMW->hier.apHier = 0;
  pMW->hier.nHier = 0;
}

static int keyszToSkip(FileSystem *pFS, int nKey){
  int nPgsz;                /* Nominal database page size */
  nPgsz = lsmFsPageSize(pFS);
  return LSM_MIN(((nKey * 4) / nPgsz), 3);
}

/*
** Release the reference to the current output page of merge-worker *pMW
** (reference pMW->pPage). Set the page number values in aSave[] as 
** required (see comments above struct MergeWorker for details).
*/
static int mergeWorkerPersistAndRelease(MergeWorker *pMW){
  int rc;
  int i;

  assert( pMW->pPage || (pMW->aSave[0].bStore==0 && pMW->aSave[1].bStore==0) );

  /* Persist the page */
  rc = lsmFsPagePersist(pMW->pPage);

  /* If required, save the page number. */
  for(i=0; i<2; i++){
    if( pMW->aSave[i].bStore ){
      pMW->aSave[i].iPgno = lsmFsPageNumber(pMW->pPage);
      pMW->aSave[i].bStore = 0;
    }
  }

  /* Release the completed output page. */
  lsmFsPageRelease(pMW->pPage);
  pMW->pPage = 0;
  return rc;
}

/*
** Advance to the next page of an output run being populated by merge-worker
** pMW. The footer of the new page is initialized to indicate that it contains
** zero records. The flags field is cleared. The page footer pointer field
** is set to iFPtr.
**
** If successful, LSM_OK is returned. Otherwise, an error code.
*/
static int mergeWorkerNextPage(
  MergeWorker *pMW,               /* Merge worker object to append page to */
  LsmPgno iFPtr                   /* Pointer value for footer of new page */
){
  int rc = LSM_OK;                /* Return code */
  Page *pNext = 0;                /* New page appended to run */
  lsm_db *pDb = pMW->pDb;         /* Database handle */

  rc = lsmFsSortedAppend(pDb->pFS, pDb->pWorker, pMW->pLevel, 0, &pNext);
  assert( rc || pMW->pLevel->lhs.iFirst>0 || pMW->pDb->compress.xCompress );

  if( rc==LSM_OK ){
    u8 *aData;                    /* Data buffer belonging to page pNext */
    int nData;                    /* Size of aData[] in bytes */

    rc = mergeWorkerPersistAndRelease(pMW);

    pMW->pPage = pNext;
    pMW->pLevel->pMerge->iOutputOff = 0;
    aData = fsPageData(pNext, &nData);
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], 0);
    lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], 0);
    lsmPutU64(&aData[SEGMENT_POINTER_OFFSET(nData)], iFPtr);
    pMW->nWork++;
  }

  return rc;
}

/*
** Write a blob of data into an output segment being populated by a 
** merge-worker object. If argument bSep is true, write into the separators
** array. Otherwise, the main array.
**
** This function is used to write the blobs of data for keys and values.
*/
static int mergeWorkerData(
  MergeWorker *pMW,               /* Merge worker object */
  int bSep,                       /* True to write to separators run */
  LsmPgno iFPtr,                  /* Footer ptr for new pages */
  u8 *aWrite,                     /* Write data from this buffer */
  int nWrite                      /* Size of aWrite[] in bytes */
){
  int rc = LSM_OK;                /* Return code */
  int nRem = nWrite;              /* Number of bytes still to write */

  while( rc==LSM_OK && nRem>0 ){
    Merge *pMerge = pMW->pLevel->pMerge;
    int nCopy;                    /* Number of bytes to copy */
    u8 *aData;                    /* Pointer to buffer of current output page */
    int nData;                    /* Size of aData[] in bytes */
    int nRec;                     /* Number of records on current output page */
    int iOff;                     /* Offset in aData[] to write to */

    assert( lsmFsPageWritable(pMW->pPage) );
   
    aData = fsPageData(pMW->pPage, &nData);
    nRec = pageGetNRec(aData, nData);
    iOff = pMerge->iOutputOff;
    nCopy = LSM_MIN(nRem, SEGMENT_EOF(nData, nRec) - iOff);

    memcpy(&aData[iOff], &aWrite[nWrite-nRem], nCopy);
    nRem -= nCopy;

    if( nRem>0 ){
      rc = mergeWorkerNextPage(pMW, iFPtr);
    }else{
      pMerge->iOutputOff = iOff + nCopy;
    }
  }

  return rc;
}


/*
** The MergeWorker passed as the only argument is working to merge two or
** more existing segments together (not to flush an in-memory tree). It
** has not yet written the first key to the first page of the output.
*/
static int mergeWorkerFirstPage(MergeWorker *pMW){
  int rc = LSM_OK;                /* Return code */
  Page *pPg = 0;                  /* First page of run pSeg */
  LsmPgno iFPtr = 0;              /* Pointer value read from footer of pPg */
  MultiCursor *pCsr = pMW->pCsr;

  assert( pMW->pPage==0 );

  if( pCsr->pBtCsr ){
    rc = LSM_OK;
    iFPtr = pMW->pLevel->pNext->lhs.iFirst;
  }else if( pCsr->nPtr>0 ){
    Segment *pSeg;
    pSeg = pCsr->aPtr[pCsr->nPtr-1].pSeg;
    rc = lsmFsDbPageGet(pMW->pDb->pFS, pSeg, pSeg->iFirst, &pPg);
    if( rc==LSM_OK ){
      u8 *aData;                    /* Buffer for page pPg */
      int nData;                    /* Size of aData[] in bytes */
      aData = fsPageData(pPg, &nData);
      iFPtr = pageGetPtr(aData, nData);
      lsmFsPageRelease(pPg);
    }
  }

  if( rc==LSM_OK ){
    rc = mergeWorkerNextPage(pMW, iFPtr);
    if( pCsr->pPrevMergePtr ) *pCsr->pPrevMergePtr = iFPtr;
    pMW->aSave[0].bStore = 1;
  }

  return rc;
}

static int mergeWorkerWrite(
  MergeWorker *pMW,               /* Merge worker object to write into */
  int eType,                      /* One of SORTED_SEPARATOR, WRITE or DELETE */
  void *pKey, int nKey,           /* Key value */
  void *pVal, int nVal,           /* Value value */
  LsmPgno iPtr                    /* Absolute value of page pointer, or 0 */
){
  int rc = LSM_OK;                /* Return code */
  Merge *pMerge;                  /* Persistent part of level merge state */
  int nHdr;                       /* Space required for this record header */
  Page *pPg;                      /* Page to write to */
  u8 *aData;                      /* Data buffer for page pWriter->pPage */
  int nData = 0;                  /* Size of buffer aData[] in bytes */
  int nRec = 0;                   /* Number of records on page pPg */
  LsmPgno iFPtr = 0;              /* Value of pointer in footer of pPg */
  LsmPgno iRPtr = 0;              /* Value of pointer written into record */
  int iOff = 0;                   /* Current write offset within page pPg */
  Segment *pSeg;                  /* Segment being written */
  int flags = 0;                  /* If != 0, flags value for page footer */
  int bFirst = 0;                 /* True for first key of output run */

  pMerge = pMW->pLevel->pMerge;    
  pSeg = &pMW->pLevel->lhs;

  if( pSeg->iFirst==0 && pMW->pPage==0 ){
    rc = mergeWorkerFirstPage(pMW);
    bFirst = 1;
  }
  pPg = pMW->pPage;
  if( pPg ){
    aData = fsPageData(pPg, &nData);
    nRec = pageGetNRec(aData, nData);
    iFPtr = pageGetPtr(aData, nData);
    iRPtr = iPtr ? (iPtr - iFPtr) : 0;
  }
     
  /* Figure out how much space is required by the new record. The space
  ** required is divided into two sections: the header and the body. The
  ** header consists of the intial varint fields. The body are the blobs 
  ** of data that correspond to the key and value data. The entire header 
  ** must be stored on the page. The body may overflow onto the next and
  ** subsequent pages.
  **
  ** The header space is:
  **
  **     1) record type - 1 byte.
  **     2) Page-pointer-offset - 1 varint
  **     3) Key size - 1 varint
  **     4) Value size - 1 varint (only if LSM_INSERT flag is set)
  */
  if( rc==LSM_OK ){
    nHdr = 1 + lsmVarintLen64(iRPtr) + lsmVarintLen32(nKey);
    if( rtIsWrite(eType) ) nHdr += lsmVarintLen32(nVal);

    /* If the entire header will not fit on page pPg, or if page pPg is 
    ** marked read-only, advance to the next page of the output run. */
    iOff = pMerge->iOutputOff;
    if( iOff<0 || pPg==0 || iOff+nHdr > SEGMENT_EOF(nData, nRec+1) ){
      if( iOff>=0 && pPg ){
        /* Zero any free space on the page */
        assert( aData );
        memset(&aData[iOff], 0, SEGMENT_EOF(nData, nRec)-iOff);
      }
      iFPtr = *pMW->pCsr->pPrevMergePtr;
      iRPtr = iPtr ? (iPtr - iFPtr) : 0;
      iOff = 0;
      nRec = 0;
      rc = mergeWorkerNextPage(pMW, iFPtr);
      pPg = pMW->pPage;
    }
  }

  /* If this record header will be the first on the page, and the page is 
  ** not the very first in the entire run, add a copy of the key to the
  ** b-tree hierarchy.
  */
  if( rc==LSM_OK && nRec==0 && bFirst==0 ){
    assert( pMerge->nSkip>=0 );

    if( pMerge->nSkip==0 ){
      rc = mergeWorkerPushHierarchy(pMW, rtTopic(eType), pKey, nKey);
      assert( pMW->aSave[0].bStore==0 );
      pMW->aSave[0].bStore = 1;
      pMerge->nSkip = keyszToSkip(pMW->pDb->pFS, nKey);
    }else{
      pMerge->nSkip--;
      flags = PGFTR_SKIP_THIS_FLAG;
    }

    if( pMerge->nSkip ) flags |= PGFTR_SKIP_NEXT_FLAG;
  }

  /* Update the output segment */
  if( rc==LSM_OK ){
    aData = fsPageData(pPg, &nData);

    /* Update the page footer. */
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], (u16)(nRec+1));
    lsmPutU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec)], (u16)iOff);
    if( flags ) lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], (u16)flags);

    /* Write the entry header into the current page. */
    aData[iOff++] = (u8)eType;                                           /* 1 */
    iOff += lsmVarintPut64(&aData[iOff], iRPtr);                         /* 2 */
    iOff += lsmVarintPut32(&aData[iOff], nKey);                          /* 3 */
    if( rtIsWrite(eType) ) iOff += lsmVarintPut32(&aData[iOff], nVal);   /* 4 */
    pMerge->iOutputOff = iOff;

    /* Write the key and data into the segment. */
    assert( iFPtr==pageGetPtr(aData, nData) );
    rc = mergeWorkerData(pMW, 0, iFPtr+iRPtr, pKey, nKey);
    if( rc==LSM_OK && rtIsWrite(eType) ){
      if( rc==LSM_OK ){
        rc = mergeWorkerData(pMW, 0, iFPtr+iRPtr, pVal, nVal);
      }
    }
  }

  return rc;
}


/*
** Free all resources allocated by mergeWorkerInit().
*/
static void mergeWorkerShutdown(MergeWorker *pMW, int *pRc){
  int i;                          /* Iterator variable */
  int rc = *pRc;
  MultiCursor *pCsr = pMW->pCsr;

  /* Unless the merge has finished, save the cursor position in the
  ** Merge.aInput[] array. See function mergeWorkerInit() for the 
  ** code to restore a cursor position based on aInput[].  */
  if( rc==LSM_OK && pCsr ){
    Merge *pMerge = pMW->pLevel->pMerge;
    if( lsmMCursorValid(pCsr) ){
      int bBtree = (pCsr->pBtCsr!=0);
      int iPtr;

      /* pMerge->nInput==0 indicates that this is a FlushTree() operation. */
      assert( pMerge->nInput==0 || pMW->pLevel->nRight>0 );
      assert( pMerge->nInput==0 || pMerge->nInput==(pCsr->nPtr+bBtree) );

      for(i=0; i<(pMerge->nInput-bBtree); i++){
        SegmentPtr *pPtr = &pCsr->aPtr[i];
        if( pPtr->pPg ){
          pMerge->aInput[i].iPg = lsmFsPageNumber(pPtr->pPg);
          pMerge->aInput[i].iCell = pPtr->iCell;
        }else{
          pMerge->aInput[i].iPg = 0;
          pMerge->aInput[i].iCell = 0;
        }
      }
      if( bBtree && pMerge->nInput ){
        assert( i==pCsr->nPtr );
        btreeCursorPosition(pCsr->pBtCsr, &pMerge->aInput[i]);
      }

      /* Store the location of the split-key */
      iPtr = pCsr->aTree[1] - CURSOR_DATA_SEGMENT;
      if( iPtr<pCsr->nPtr ){
        pMerge->splitkey = pMerge->aInput[iPtr];
      }else{
        btreeCursorSplitkey(pCsr->pBtCsr, &pMerge->splitkey);
      }
    }

    /* Zero any free space left on the final page. This helps with
    ** compression if using a compression hook. And prevents valgrind
    ** from complaining about uninitialized byte passed to write(). */
    if( pMW->pPage ){
      int nData;
      u8 *aData = fsPageData(pMW->pPage, &nData);
      int iOff = pMerge->iOutputOff;
      int iEof = SEGMENT_EOF(nData, pageGetNRec(aData, nData));
      memset(&aData[iOff], 0, iEof - iOff);
    }
    
    pMerge->iOutputOff = -1;
  }

  lsmMCursorClose(pCsr, 0);

  /* Persist and release the output page. */
  if( rc==LSM_OK ) rc = mergeWorkerPersistAndRelease(pMW);
  if( rc==LSM_OK ) rc = mergeWorkerBtreeIndirect(pMW);
  if( rc==LSM_OK ) rc = mergeWorkerFinishHierarchy(pMW);
  if( rc==LSM_OK ) rc = mergeWorkerAddPadding(pMW);
  lsmFsFlushWaiting(pMW->pDb->pFS, &rc);
  mergeWorkerReleaseAll(pMW);

  lsmFree(pMW->pDb->pEnv, pMW->aGobble);
  pMW->aGobble = 0;
  pMW->pCsr = 0;

  *pRc = rc;
}

/*
** The cursor passed as the first argument is being used as the input for
** a merge operation. When this function is called, *piFlags contains the
** database entry flags for the current entry. The entry about to be written
** to the output.
**
** Note that this function only has to work for cursors configured to 
** iterate forwards (not backwards).
*/
static void mergeRangeDeletes(MultiCursor *pCsr, int *piVal, int *piFlags){
  int f = *piFlags;
  int iKey = pCsr->aTree[1];
  int i;

  assert( pCsr->flags & CURSOR_NEXT_OK );
  if( pCsr->flags & CURSOR_IGNORE_DELETE ){
    /* The ignore-delete flag is set when the output of the merge will form
    ** the oldest level in the database. In this case there is no point in
    ** retaining any range-delete flags.  */
    assert( (f & LSM_POINT_DELETE)==0 );
    f &= ~(LSM_START_DELETE|LSM_END_DELETE);
  }else{
    for(i=0; i<(CURSOR_DATA_SEGMENT + pCsr->nPtr); i++){
      if( i!=iKey ){
        int eType;
        void *pKey;
        int nKey;
        int res;
        multiCursorGetKey(pCsr, i, &eType, &pKey, &nKey);

        if( pKey ){
          res = sortedKeyCompare(pCsr->pDb->xCmp, 
              rtTopic(pCsr->eType), pCsr->key.pData, pCsr->key.nData,
              rtTopic(eType), pKey, nKey
          );
          assert( res<=0 );
          if( res==0 ){
            if( (f & (LSM_INSERT|LSM_POINT_DELETE))==0 ){
              if( eType & LSM_INSERT ){
                f |= LSM_INSERT;
                *piVal = i;
              }
              else if( eType & LSM_POINT_DELETE ){
                f |= LSM_POINT_DELETE;
              }
            }
            f |= (eType & (LSM_END_DELETE|LSM_START_DELETE));
          }

          if( i>iKey && (eType & LSM_END_DELETE) && res<0 ){
            if( f & (LSM_INSERT|LSM_POINT_DELETE) ){
              f |= (LSM_END_DELETE|LSM_START_DELETE);
            }else{
              f = 0;
            }
            break;
          }
        }
      }
    }

    assert( (f & LSM_INSERT)==0 || (f & LSM_POINT_DELETE)==0 );
    if( (f & LSM_START_DELETE) 
     && (f & LSM_END_DELETE) 
     && (f & LSM_POINT_DELETE )
    ){
      f = 0;
    }
  }

  *piFlags = f;
}

static int mergeWorkerStep(MergeWorker *pMW){
  lsm_db *pDb = pMW->pDb;       /* Database handle */
  MultiCursor *pCsr;            /* Cursor to read input data from */
  int rc = LSM_OK;              /* Return code */
  int eType;                    /* SORTED_SEPARATOR, WRITE or DELETE */
  void *pKey; int nKey;         /* Key */
  LsmPgno iPtr;
  int iVal;

  pCsr = pMW->pCsr;

  /* Pull the next record out of the source cursor. */
  lsmMCursorKey(pCsr, &pKey, &nKey);
  eType = pCsr->eType;

  /* Figure out if the output record may have a different pointer value
  ** than the previous. This is the case if the current key is identical to
  ** a key that appears in the lowest level run being merged. If so, set 
  ** iPtr to the absolute pointer value. If not, leave iPtr set to zero, 
  ** indicating that the output pointer value should be a copy of the pointer 
  ** value written with the previous key.  */
  iPtr = (pCsr->pPrevMergePtr ? *pCsr->pPrevMergePtr : 0);
  if( pCsr->pBtCsr ){
    BtreeCursor *pBtCsr = pCsr->pBtCsr;
    if( pBtCsr->pKey ){
      int res = rtTopic(pBtCsr->eType) - rtTopic(eType);
      if( res==0 ) res = pDb->xCmp(pBtCsr->pKey, pBtCsr->nKey, pKey, nKey);
      if( 0==res ) iPtr = pBtCsr->iPtr;
      assert( res>=0 );
    }
  }else if( pCsr->nPtr ){
    SegmentPtr *pPtr = &pCsr->aPtr[pCsr->nPtr-1];
    if( pPtr->pPg
     && 0==pDb->xCmp(pPtr->pKey, pPtr->nKey, pKey, nKey)
    ){
      iPtr = pPtr->iPtr+pPtr->iPgPtr;
    }
  }

  iVal = pCsr->aTree[1];
  mergeRangeDeletes(pCsr, &iVal, &eType);

  if( eType!=0 ){
    if( pMW->aGobble ){
      int iGobble = pCsr->aTree[1] - CURSOR_DATA_SEGMENT;
      if( iGobble<pCsr->nPtr && iGobble>=0 ){
        SegmentPtr *pGobble = &pCsr->aPtr[iGobble];
        if( (pGobble->flags & PGFTR_SKIP_THIS_FLAG)==0 ){
          pMW->aGobble[iGobble] = lsmFsPageNumber(pGobble->pPg);
        }
      }
    }

    /* If this is a separator key and we know that the output pointer has not
    ** changed, there is no point in writing an output record. Otherwise,
    ** proceed. */
    if( rc==LSM_OK && (rtIsSeparator(eType)==0 || iPtr!=0) ){
      /* Write the record into the main run. */
      void *pVal; int nVal;
      rc = multiCursorGetVal(pCsr, iVal, &pVal, &nVal);
      if( pVal && rc==LSM_OK ){
        assert( nVal>=0 );
        rc = sortedBlobSet(pDb->pEnv, &pCsr->val, pVal, nVal);
        pVal = pCsr->val.pData;
      }
      if( rc==LSM_OK ){
        rc = mergeWorkerWrite(pMW, eType, pKey, nKey, pVal, nVal, iPtr);
      }
    }
  }

  /* Advance the cursor to the next input record (assuming one exists). */
  assert( lsmMCursorValid(pMW->pCsr) );
  if( rc==LSM_OK ) rc = lsmMCursorNext(pMW->pCsr);

  return rc;
}

static int mergeWorkerDone(MergeWorker *pMW){
  return pMW->pCsr==0 || !lsmMCursorValid(pMW->pCsr);
}

static void sortedFreeLevel(lsm_env *pEnv, Level *p){
  if( p ){
    lsmFree(pEnv, p->pSplitKey);
    lsmFree(pEnv, p->pMerge);
    lsmFree(pEnv, p->aRhs);
    lsmFree(pEnv, p);
  }
}

static void sortedInvokeWorkHook(lsm_db *pDb){
  if( pDb->xWork ){
    pDb->xWork(pDb, pDb->pWorkCtx);
  }
}

static int sortedNewToplevel(
  lsm_db *pDb,                    /* Connection handle */
  int eTree,                      /* One of the TREE_XXX constants */
  int *pnWrite                    /* OUT: Number of database pages written */
){
  int rc = LSM_OK;                /* Return Code */
  MultiCursor *pCsr = 0;
  Level *pNext = 0;               /* The current top level */
  Level *pNew;                    /* The new level itself */
  Segment *pLinked = 0;           /* Delete separators from this segment */
  Level *pDel = 0;                /* Delete this entire level */
  int nWrite = 0;                 /* Number of database pages written */
  Freelist freelist;

  if( eTree!=TREE_NONE ){
    rc = lsmShmCacheChunks(pDb, pDb->treehdr.nChunk);
  }

  assert( pDb->bUseFreelist==0 );
  pDb->pFreelist = &freelist;
  pDb->bUseFreelist = 1;
  memset(&freelist, 0, sizeof(freelist));

  /* Allocate the new level structure to write to. */
  pNext = lsmDbSnapshotLevel(pDb->pWorker);
  pNew = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);
  if( pNew ){
    pNew->pNext = pNext;
    lsmDbSnapshotSetLevel(pDb->pWorker, pNew);
  }

  /* Create a cursor to gather the data required by the new segment. The new
  ** segment contains everything in the tree and pointers to the next segment
  ** in the database (if any).  */
  pCsr = multiCursorNew(pDb, &rc);
  if( pCsr ){
    pCsr->pDb = pDb;
    rc = multiCursorVisitFreelist(pCsr);
    if( rc==LSM_OK ){
      rc = multiCursorAddTree(pCsr, pDb->pWorker, eTree);
    }
    if( rc==LSM_OK && pNext && pNext->pMerge==0 ){
      if( (pNext->flags & LEVEL_FREELIST_ONLY) ){
        pDel = pNext;
        pCsr->aPtr = lsmMallocZeroRc(pDb->pEnv, sizeof(SegmentPtr), &rc);
        multiCursorAddOne(pCsr, pNext, &rc);
      }else if( eTree!=TREE_NONE && pNext->lhs.iRoot ){
        pLinked = &pNext->lhs;
        rc = btreeCursorNew(pDb, pLinked, &pCsr->pBtCsr);
      }
    }

    /* If this will be the only segment in the database, discard any delete
    ** markers present in the in-memory tree.  */
    if( pNext==0 ){
      multiCursorIgnoreDelete(pCsr);
    }
  }

  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr, 0);
  }else{
    LsmPgno iLeftPtr = 0;
    Merge merge;                  /* Merge object used to create new level */
    MergeWorker mergeworker;      /* MergeWorker object for the same purpose */

    memset(&merge, 0, sizeof(Merge));
    memset(&mergeworker, 0, sizeof(MergeWorker));

    pNew->pMerge = &merge;
    pNew->flags |= LEVEL_INCOMPLETE;
    mergeworker.pDb = pDb;
    mergeworker.pLevel = pNew;
    mergeworker.pCsr = pCsr;
    pCsr->pPrevMergePtr = &iLeftPtr;

    /* Mark the separators array for the new level as a "phantom". */
    mergeworker.bFlush = 1;

    /* Do the work to create the new merged segment on disk */
    if( rc==LSM_OK ) rc = lsmMCursorFirst(pCsr);
    while( rc==LSM_OK && mergeWorkerDone(&mergeworker)==0 ){
      rc = mergeWorkerStep(&mergeworker);
    }
    mergeWorkerShutdown(&mergeworker, &rc);
    assert( rc!=LSM_OK || mergeworker.nWork==0 || pNew->lhs.iFirst );
    if( rc==LSM_OK && pNew->lhs.iFirst ){
      rc = lsmFsSortedFinish(pDb->pFS, &pNew->lhs);
    }
    nWrite = mergeworker.nWork;
    pNew->flags &= ~LEVEL_INCOMPLETE;
    if( eTree==TREE_NONE ){
      pNew->flags |= LEVEL_FREELIST_ONLY;
    }
    pNew->pMerge = 0;
  }

  if( rc!=LSM_OK || pNew->lhs.iFirst==0 ){
    assert( rc!=LSM_OK || pDb->pWorker->freelist.nEntry==0 );
    lsmDbSnapshotSetLevel(pDb->pWorker, pNext);
    sortedFreeLevel(pDb->pEnv, pNew);
  }else{
    if( pLinked ){
      pLinked->iRoot = 0;
    }else if( pDel ){
      assert( pNew->pNext==pDel );
      pNew->pNext = pDel->pNext;
      lsmFsSortedDelete(pDb->pFS, pDb->pWorker, 1, &pDel->lhs);
      sortedFreeLevel(pDb->pEnv, pDel);
    }

#if LSM_LOG_STRUCTURE
    lsmSortedDumpStructure(pDb, pDb->pWorker, LSM_LOG_DATA, 0, "new-toplevel");
#endif

    if( freelist.nEntry ){
      Freelist *p = &pDb->pWorker->freelist;
      lsmFree(pDb->pEnv, p->aEntry);
      memcpy(p, &freelist, sizeof(freelist));
      freelist.aEntry = 0;
    }else{
      pDb->pWorker->freelist.nEntry = 0;
    }

    assertBtreeOk(pDb, &pNew->lhs);
    sortedInvokeWorkHook(pDb);
  }

  if( pnWrite ) *pnWrite = nWrite;
  pDb->pWorker->nWrite += nWrite;
  pDb->pFreelist = 0;
  pDb->bUseFreelist = 0;
  lsmFree(pDb->pEnv, freelist.aEntry);
  return rc;
}

/*
** The nMerge levels in the LSM beginning with pLevel consist of a
** left-hand-side segment only. Replace these levels with a single new
** level consisting of a new empty segment on the left-hand-side and the
** nMerge segments from the replaced levels on the right-hand-side.
**
** Also, allocate and populate a Merge object and set Level.pMerge to
** point to it.
*/
static int sortedMergeSetup(
  lsm_db *pDb,                    /* Database handle */
  Level *pLevel,                  /* First level to merge */
  int nMerge,                     /* Merge this many levels together */
  Level **ppNew                   /* New, merged, level */
){
  int rc = LSM_OK;                /* Return Code */
  Level *pNew;                    /* New Level object */
  int bUseNext = 0;               /* True to link in next separators */
  Merge *pMerge;                  /* New Merge object */
  int nByte;                      /* Bytes of space allocated at pMerge */

#ifdef LSM_DEBUG
  int iLevel;
  Level *pX = pLevel;
  for(iLevel=0; iLevel<nMerge; iLevel++){
    assert( pX->nRight==0 );
    pX = pX->pNext;
  }
#endif

  /* Allocate the new Level object */
  pNew = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);
  if( pNew ){
    pNew->aRhs = (Segment *)lsmMallocZeroRc(pDb->pEnv, 
                                        nMerge * sizeof(Segment), &rc);
  }

  /* Populate the new Level object */
  if( rc==LSM_OK ){
    Level *pNext = 0;             /* Level following pNew */
    int i;
    int bFreeOnly = 1;
    Level *pTopLevel;
    Level *p = pLevel;
    Level **pp;
    pNew->nRight = nMerge;
    pNew->iAge = pLevel->iAge+1;
    for(i=0; i<nMerge; i++){
      assert( p->nRight==0 );
      pNext = p->pNext;
      pNew->aRhs[i] = p->lhs;
      if( (p->flags & LEVEL_FREELIST_ONLY)==0 ) bFreeOnly = 0;
      sortedFreeLevel(pDb->pEnv, p);
      p = pNext;
    }

    if( bFreeOnly ) pNew->flags |= LEVEL_FREELIST_ONLY;

    /* Replace the old levels with the new. */
    pTopLevel = lsmDbSnapshotLevel(pDb->pWorker);
    pNew->pNext = p;
    for(pp=&pTopLevel; *pp!=pLevel; pp=&((*pp)->pNext));
    *pp = pNew;
    lsmDbSnapshotSetLevel(pDb->pWorker, pTopLevel);

    /* Determine whether or not the next separators will be linked in */
    if( pNext && pNext->pMerge==0 && pNext->lhs.iRoot && pNext 
     && (bFreeOnly==0 || (pNext->flags & LEVEL_FREELIST_ONLY))
    ){
      bUseNext = 1;
    }
  }

  /* Allocate the merge object */
  nByte = sizeof(Merge) + sizeof(MergeInput) * (nMerge + bUseNext);
  pMerge = (Merge *)lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
  if( pMerge ){
    pMerge->aInput = (MergeInput *)&pMerge[1];
    pMerge->nInput = nMerge + bUseNext;
    pNew->pMerge = pMerge;
  }

  *ppNew = pNew;
  return rc;
}

static int mergeWorkerInit(
  lsm_db *pDb,                    /* Db connection to do merge work */
  Level *pLevel,                  /* Level to work on merging */
  MergeWorker *pMW                /* Object to initialize */
){
  int rc = LSM_OK;                /* Return code */
  Merge *pMerge = pLevel->pMerge; /* Persistent part of merge state */
  MultiCursor *pCsr = 0;          /* Cursor opened for pMW */
  Level *pNext = pLevel->pNext;   /* Next level in LSM */

  assert( pDb->pWorker );
  assert( pLevel->pMerge );
  assert( pLevel->nRight>0 );

  memset(pMW, 0, sizeof(MergeWorker));
  pMW->pDb = pDb;
  pMW->pLevel = pLevel;
  pMW->aGobble = lsmMallocZeroRc(pDb->pEnv, sizeof(LsmPgno)*pLevel->nRight,&rc);

  /* Create a multi-cursor to read the data to write to the new
  ** segment. The new segment contains:
  **
  **   1. Records from LHS of each of the nMerge levels being merged.
  **   2. Separators from either the last level being merged, or the
  **      separators attached to the LHS of the following level, or neither.
  **
  ** If the new level is the lowest (oldest) in the db, discard any
  ** delete keys. Key annihilation.
  */
  pCsr = multiCursorNew(pDb, &rc);
  if( pCsr ){
    pCsr->flags |= CURSOR_NEXT_OK;
    rc = multiCursorAddRhs(pCsr, pLevel);
  }
  if( rc==LSM_OK && pMerge->nInput > pLevel->nRight ){
    rc = btreeCursorNew(pDb, &pNext->lhs, &pCsr->pBtCsr);
  }else if( pNext ){
    multiCursorReadSeparators(pCsr);
  }else{
    multiCursorIgnoreDelete(pCsr);
  }

  assert( rc!=LSM_OK || pMerge->nInput==(pCsr->nPtr+(pCsr->pBtCsr!=0)) );
  pMW->pCsr = pCsr;

  /* Load the b-tree hierarchy into memory. */
  if( rc==LSM_OK ) rc = mergeWorkerLoadHierarchy(pMW);
  if( rc==LSM_OK && pMW->hier.nHier==0 ){
    pMW->aSave[0].iPgno = pLevel->lhs.iFirst;
  }

  /* Position the cursor. */
  if( rc==LSM_OK ){
    pCsr->pPrevMergePtr = &pMerge->iCurrentPtr;
    if( pLevel->lhs.iFirst==0 ){
      /* The output array is still empty. So position the cursor at the very 
      ** start of the input.  */
      rc = multiCursorEnd(pCsr, 0);
    }else{
      /* The output array is non-empty. Position the cursor based on the
      ** page/cell data saved in the Merge.aInput[] array.  */
      int i;
      for(i=0; rc==LSM_OK && i<pCsr->nPtr; i++){
        MergeInput *pInput = &pMerge->aInput[i];
        if( pInput->iPg ){
          SegmentPtr *pPtr;
          assert( pCsr->aPtr[i].pPg==0 );
          pPtr = &pCsr->aPtr[i];
          rc = segmentPtrLoadPage(pDb->pFS, pPtr, pInput->iPg);
          if( rc==LSM_OK && pPtr->nCell>0 ){
            rc = segmentPtrLoadCell(pPtr, pInput->iCell);
          }
        }
      }

      if( rc==LSM_OK && pCsr->pBtCsr ){
        int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
        assert( i==pCsr->nPtr );
        rc = btreeCursorRestore(pCsr->pBtCsr, xCmp, &pMerge->aInput[i]);
      }

      if( rc==LSM_OK ){
        rc = multiCursorSetupTree(pCsr, 0);
      }
    }
    pCsr->flags |= CURSOR_NEXT_OK;
  }

  return rc;
}

static int sortedBtreeGobble(
  lsm_db *pDb,                    /* Worker connection */
  MultiCursor *pCsr,              /* Multi-cursor being used for a merge */
  int iGobble                     /* pCsr->aPtr[] entry to operate on */
){
  int rc = LSM_OK;
  if( rtTopic(pCsr->eType)==0 ){
    Segment *pSeg = pCsr->aPtr[iGobble].pSeg;
    LsmPgno *aPg;
    int nPg;

    /* Seek from the root of the b-tree to the segment leaf that may contain
    ** a key equal to the one multi-cursor currently points to. Record the
    ** page number of each b-tree page and the leaf. The segment may be
    ** gobbled up to (but not including) the first of these page numbers.
    */
    assert( pSeg->iRoot>0 );
    aPg = lsmMallocZeroRc(pDb->pEnv, sizeof(LsmPgno)*32, &rc);
    if( rc==LSM_OK ){
      rc = seekInBtree(pCsr, pSeg, 
          rtTopic(pCsr->eType), pCsr->key.pData, pCsr->key.nData, aPg, 0
      ); 
    }

    if( rc==LSM_OK ){
      for(nPg=0; aPg[nPg]; nPg++);
      lsmFsGobble(pDb, pSeg, aPg, nPg);
    }

    lsmFree(pDb->pEnv, aPg);
  }
  return rc;
}

/*
** Argument p points to a level of age N. Return the number of levels in
** the linked list starting at p that have age=N (always at least 1).
*/
static int sortedCountLevels(Level *p){
  int iAge = p->iAge;
  int nRet = 0;
  do {
    nRet++;
    p = p->pNext;
  }while( p && p->iAge==iAge );
  return nRet;
}

static int sortedSelectLevel(lsm_db *pDb, int nMerge, Level **ppOut){
  Level *pTopLevel = lsmDbSnapshotLevel(pDb->pWorker);
  int rc = LSM_OK;
  Level *pLevel = 0;            /* Output value */
  Level *pBest = 0;             /* Best level to work on found so far */
  int nBest;                    /* Number of segments merged at pBest */
  Level *pThis = 0;             /* First in run of levels with age=iAge */
  int nThis = 0;                /* Number of levels starting at pThis */

  assert( nMerge>=1 );
  nBest = LSM_MAX(1, nMerge-1);

  /* Find the longest contiguous run of levels not currently undergoing a 
  ** merge with the same age in the structure. Or the level being merged
  ** with the largest number of right-hand segments. Work on it. */
  for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
    if( pLevel->nRight==0 && pThis && pLevel->iAge==pThis->iAge ){
      nThis++;
    }else{
      if( nThis>nBest ){
        if( (pLevel->iAge!=pThis->iAge+1)
         || (pLevel->nRight==0 && sortedCountLevels(pLevel)<=pDb->nMerge)
        ){
          pBest = pThis;
          nBest = nThis;
        }
      }
      if( pLevel->nRight ){
        if( pLevel->nRight>nBest ){
          nBest = pLevel->nRight;
          pBest = pLevel;
        }
        nThis = 0;
        pThis = 0;
      }else{
        pThis = pLevel;
        nThis = 1;
      }
    }
  }
  if( nThis>nBest ){
    assert( pThis );
    pBest = pThis;
    nBest = nThis;
  }

  if( pBest==0 && nMerge==1 ){
    int nFree = 0;
    int nUsr = 0;
    for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
      assert( !pLevel->nRight );
      if( pLevel->flags & LEVEL_FREELIST_ONLY ){
        nFree++;
      }else{
        nUsr++;
      }
    }
    if( nUsr>1 ){
      pBest = pTopLevel;
      nBest = nFree + nUsr;
    }
  }

  if( pBest ){
    if( pBest->nRight==0 ){
      rc = sortedMergeSetup(pDb, pBest, nBest, ppOut);
    }else{
      *ppOut = pBest;
    }
  }

  return rc;
}

static int sortedDbIsFull(lsm_db *pDb){
  Level *pTop = lsmDbSnapshotLevel(pDb->pWorker);

  if( lsmDatabaseFull(pDb) ) return 1;
  if( pTop && pTop->iAge==0
   && (pTop->nRight || sortedCountLevels(pTop)>=pDb->nMerge)
  ){
    return 1;
  }
  return 0;
}

typedef struct MoveBlockCtx MoveBlockCtx;
struct MoveBlockCtx {
  int iSeen;                      /* Previous free block on list */
  int iFrom;                      /* Total number of blocks in file */
};

static int moveBlockCb(void *pCtx, int iBlk, i64 iSnapshot){
  MoveBlockCtx *p = (MoveBlockCtx *)pCtx;
  assert( p->iFrom==0 );
  if( iBlk==(p->iSeen-1) ){
    p->iSeen = iBlk;
    return 0;
  }
  p->iFrom = p->iSeen-1;
  return 1;
}

/*
** This function is called to further compact a database for which all 
** of the content has already been merged into a single segment. If 
** possible, it moves the contents of a single block from the end of the
** file to a free-block that lies closer to the start of the file (allowing
** the file to be eventually truncated).
*/
static int sortedMoveBlock(lsm_db *pDb, int *pnWrite){
  Snapshot *p = pDb->pWorker;
  Level *pLvl = lsmDbSnapshotLevel(p);
  int iFrom;                      /* Block to move */
  int iTo;                        /* Destination to move block to */
  int rc;                         /* Return code */

  MoveBlockCtx sCtx;

  assert( pLvl->pNext==0 && pLvl->nRight==0 );
  assert( p->redirect.n<=LSM_MAX_BLOCK_REDIRECTS );

  *pnWrite = 0;

  /* Check that the redirect array is not already full. If it is, return
  ** without moving any database content.  */
  if( p->redirect.n>=LSM_MAX_BLOCK_REDIRECTS ) return LSM_OK;

  /* Find the last block of content in the database file. Do this by 
  ** traversing the free-list in reverse (descending block number) order.
  ** The first block not on the free list is the one that will be moved.
  ** Since the db consists of a single segment, there is no ambiguity as
  ** to which segment the block belongs to.  */
  sCtx.iSeen = p->nBlock+1;
  sCtx.iFrom = 0;
  rc = lsmWalkFreelist(pDb, 1, moveBlockCb, &sCtx);
  if( rc!=LSM_OK || sCtx.iFrom==0 ) return rc;
  iFrom = sCtx.iFrom;

  /* Find the first free block in the database, ignoring block 1. Block
  ** 1 is tricky as it is smaller than the other blocks.  */
  rc = lsmBlockAllocate(pDb, iFrom, &iTo);
  if( rc!=LSM_OK || iTo==0 ) return rc;
  assert( iTo!=1 && iTo<iFrom );

  rc = lsmFsMoveBlock(pDb->pFS, &pLvl->lhs, iTo, iFrom);
  if( rc==LSM_OK ){
    if( p->redirect.a==0 ){
      int nByte = sizeof(struct RedirectEntry) * LSM_MAX_BLOCK_REDIRECTS;
      p->redirect.a = lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
    }
    if( rc==LSM_OK ){

      /* Check if the block just moved was already redirected. */
      int i;
      for(i=0; i<p->redirect.n; i++){
        if( p->redirect.a[i].iTo==iFrom ) break;
      }

      if( i==p->redirect.n ){
        /* Block iFrom was not already redirected. Add a new array entry. */
        memmove(&p->redirect.a[1], &p->redirect.a[0], 
            sizeof(struct RedirectEntry) * p->redirect.n
            );
        p->redirect.a[0].iFrom = iFrom;
        p->redirect.a[0].iTo = iTo;
        p->redirect.n++;
      }else{
        /* Block iFrom was already redirected. Overwrite existing entry. */
        p->redirect.a[i].iTo = iTo;
      }

      rc = lsmBlockFree(pDb, iFrom);

      *pnWrite = lsmFsBlockSize(pDb->pFS) / lsmFsPageSize(pDb->pFS);
      pLvl->lhs.pRedirect = &p->redirect;
    }
  }

#if LSM_LOG_STRUCTURE
  if( rc==LSM_OK ){
    char aBuf[64];
    sprintf(aBuf, "move-block %d/%d", p->redirect.n-1, LSM_MAX_BLOCK_REDIRECTS);
    lsmSortedDumpStructure(pDb, pDb->pWorker, LSM_LOG_DATA, 0, aBuf);
  }
#endif
  return rc;
}

/*
*/
static int mergeInsertFreelistSegments(
  lsm_db *pDb, 
  int nFree,
  MergeWorker *pMW
){
  int rc = LSM_OK;
  if( nFree>0 ){
    MultiCursor *pCsr = pMW->pCsr;
    Level *pLvl = pMW->pLevel;
    SegmentPtr *aNew1;
    Segment *aNew2;

    Level *pIter;
    Level *pNext;
    int i = 0;

    aNew1 = (SegmentPtr *)lsmMallocZeroRc(
        pDb->pEnv, sizeof(SegmentPtr) * (pCsr->nPtr+nFree), &rc
    );
    if( rc ) return rc;
    memcpy(&aNew1[nFree], pCsr->aPtr, sizeof(SegmentPtr)*pCsr->nPtr);
    pCsr->nPtr += nFree;
    lsmFree(pDb->pEnv, pCsr->aTree);
    lsmFree(pDb->pEnv, pCsr->aPtr);
    pCsr->aTree = 0;
    pCsr->aPtr = aNew1;

    aNew2 = (Segment *)lsmMallocZeroRc(
        pDb->pEnv, sizeof(Segment) * (pLvl->nRight+nFree), &rc
    );
    if( rc ) return rc;
    memcpy(&aNew2[nFree], pLvl->aRhs, sizeof(Segment)*pLvl->nRight);
    pLvl->nRight += nFree;
    lsmFree(pDb->pEnv, pLvl->aRhs);
    pLvl->aRhs = aNew2;

    for(pIter=pDb->pWorker->pLevel; rc==LSM_OK && pIter!=pLvl; pIter=pNext){
      Segment *pSeg = &pLvl->aRhs[i];
      memcpy(pSeg, &pIter->lhs, sizeof(Segment));

      pCsr->aPtr[i].pSeg = pSeg;
      pCsr->aPtr[i].pLevel = pLvl;
      rc = segmentPtrEnd(pCsr, &pCsr->aPtr[i], 0);

      pDb->pWorker->pLevel = pNext = pIter->pNext;
      sortedFreeLevel(pDb->pEnv, pIter);
      i++;
    }
    assert( i==nFree );
    assert( rc!=LSM_OK || pDb->pWorker->pLevel==pLvl );

    for(i=nFree; i<pCsr->nPtr; i++){
      pCsr->aPtr[i].pSeg = &pLvl->aRhs[i];
    }

    lsmFree(pDb->pEnv, pMW->aGobble);
    pMW->aGobble = 0;
  }
  return rc;
}

static int sortedWork(
  lsm_db *pDb,                    /* Database handle. Must be worker. */
  int nWork,                      /* Number of pages of work to do */
  int nMerge,                     /* Try to merge this many levels at once */
  int bFlush,                     /* Set if call is to make room for a flush */
  int *pnWrite                    /* OUT: Actual number of pages written */
){
  int rc = LSM_OK;                /* Return Code */
  int nRemaining = nWork;         /* Units of work to do before returning */
  Snapshot *pWorker = pDb->pWorker;

  assert( pWorker );
  if( lsmDbSnapshotLevel(pWorker)==0 ) return LSM_OK;

  while( nRemaining>0 ){
    Level *pLevel = 0;

    /* Find a level to work on. */
    rc = sortedSelectLevel(pDb, nMerge, &pLevel);
    assert( rc==LSM_OK || pLevel==0 );

    if( pLevel==0 ){
      int nDone = 0;
      Level *pTopLevel = lsmDbSnapshotLevel(pDb->pWorker);
      if( bFlush==0 && nMerge==1 && pTopLevel && pTopLevel->pNext==0 ){
        rc = sortedMoveBlock(pDb, &nDone);
      }
      nRemaining -= nDone;

      /* Could not find any work to do. Finished. */
      if( nDone==0 ) break;
    }else{
      int bSave = 0;
      Freelist freelist = {0, 0, 0};
      MergeWorker mergeworker;    /* State used to work on the level merge */

      assert( pDb->bIncrMerge==0 );
      assert( pDb->pFreelist==0 && pDb->bUseFreelist==0 );

      pDb->bIncrMerge = 1;
      rc = mergeWorkerInit(pDb, pLevel, &mergeworker);
      assert( mergeworker.nWork==0 );
      
      while( rc==LSM_OK 
          && 0==mergeWorkerDone(&mergeworker) 
          && (mergeworker.nWork<nRemaining || pDb->bUseFreelist)
      ){
        int eType = rtTopic(mergeworker.pCsr->eType);
        rc = mergeWorkerStep(&mergeworker);

        /* If the cursor now points at the first entry past the end of the
        ** user data (i.e. either to EOF or to the first free-list entry
        ** that will be added to the run), then check if it is possible to
        ** merge in any free-list entries that are either in-memory or in
        ** free-list-only blocks.  */
        if( rc==LSM_OK && nMerge==1 && eType==0
         && (rtTopic(mergeworker.pCsr->eType) || mergeWorkerDone(&mergeworker))
        ){
          int nFree = 0;          /* Number of free-list-only levels to merge */
          Level *pLvl;
          assert( pDb->pFreelist==0 && pDb->bUseFreelist==0 );

          /* Now check if all levels containing data newer than this one
          ** are single-segment free-list only levels. If so, they will be
          ** merged in now.  */
          for(pLvl=pDb->pWorker->pLevel; 
              pLvl!=mergeworker.pLevel && (pLvl->flags & LEVEL_FREELIST_ONLY); 
              pLvl=pLvl->pNext
          ){
            assert( pLvl->nRight==0 );
            nFree++;
          }
          if( pLvl==mergeworker.pLevel ){

            rc = mergeInsertFreelistSegments(pDb, nFree, &mergeworker);
            if( rc==LSM_OK ){
              rc = multiCursorVisitFreelist(mergeworker.pCsr);
            }
            if( rc==LSM_OK ){
              rc = multiCursorSetupTree(mergeworker.pCsr, 0);
              pDb->pFreelist = &freelist;
              pDb->bUseFreelist = 1;
            }
          }
        }
      }
      nRemaining -= LSM_MAX(mergeworker.nWork, 1);

      if( rc==LSM_OK ){
        /* Check if the merge operation is completely finished. If not,
        ** gobble up (declare eligible for recycling) any pages from rhs
        ** segments for which the content has been completely merged into 
        ** the lhs of the level.  */
        if( mergeWorkerDone(&mergeworker)==0 ){
          int i;
          for(i=0; i<pLevel->nRight; i++){
            SegmentPtr *pGobble = &mergeworker.pCsr->aPtr[i];
            if( pGobble->pSeg->iRoot ){
              rc = sortedBtreeGobble(pDb, mergeworker.pCsr, i);
            }else if( mergeworker.aGobble[i] ){
              lsmFsGobble(pDb, pGobble->pSeg, &mergeworker.aGobble[i], 1);
            }
          }
        }else{
          int i;
          int bEmpty;
          mergeWorkerShutdown(&mergeworker, &rc);
          bEmpty = (pLevel->lhs.iFirst==0);

          if( bEmpty==0 && rc==LSM_OK ){
            rc = lsmFsSortedFinish(pDb->pFS, &pLevel->lhs);
          }

          if( pDb->bUseFreelist ){
            Freelist *p = &pDb->pWorker->freelist;
            lsmFree(pDb->pEnv, p->aEntry);
            memcpy(p, &freelist, sizeof(freelist));
            pDb->bUseFreelist = 0;
            pDb->pFreelist = 0;
            bSave = 1;
          }

          for(i=0; i<pLevel->nRight; i++){
            lsmFsSortedDelete(pDb->pFS, pWorker, 1, &pLevel->aRhs[i]);
          }

          if( bEmpty ){
            /* If the new level is completely empty, remove it from the 
            ** database snapshot. This can only happen if all input keys were
            ** annihilated. Since keys are only annihilated if the new level
            ** is the last in the linked list (contains the most ancient of
            ** database content), this guarantees that pLevel->pNext==0.  */ 
            Level *pTop;          /* Top level of worker snapshot */
            Level **pp;           /* Read/write iterator for Level.pNext list */

            assert( pLevel->pNext==0 );

            /* Remove the level from the worker snapshot. */
            pTop = lsmDbSnapshotLevel(pWorker);
            for(pp=&pTop; *pp!=pLevel; pp=&((*pp)->pNext));
            *pp = pLevel->pNext;
            lsmDbSnapshotSetLevel(pWorker, pTop);

            /* Free the Level structure. */
            sortedFreeLevel(pDb->pEnv, pLevel);
          }else{

            /* Free the separators of the next level, if required. */
            if( pLevel->pMerge->nInput > pLevel->nRight ){
              assert( pLevel->pNext->lhs.iRoot );
              pLevel->pNext->lhs.iRoot = 0;
            }

            /* Zero the right-hand-side of pLevel */
            lsmFree(pDb->pEnv, pLevel->aRhs);
            pLevel->nRight = 0;
            pLevel->aRhs = 0;

            /* Free the Merge object */
            lsmFree(pDb->pEnv, pLevel->pMerge);
            pLevel->pMerge = 0;
          }

          if( bSave && rc==LSM_OK ){
            pDb->bIncrMerge = 0;
            rc = lsmSaveWorker(pDb, 0);
          }
        }
      }

      /* Clean up the MergeWorker object initialized above. If no error
      ** has occurred, invoke the work-hook to inform the application that
      ** the database structure has changed. */
      mergeWorkerShutdown(&mergeworker, &rc);
      pDb->bIncrMerge = 0;
      if( rc==LSM_OK ) sortedInvokeWorkHook(pDb);

#if LSM_LOG_STRUCTURE
      lsmSortedDumpStructure(pDb, pDb->pWorker, LSM_LOG_DATA, 0, "work");
#endif
      assertBtreeOk(pDb, &pLevel->lhs);
      assertRunInOrder(pDb, &pLevel->lhs);

      /* If bFlush is true and the database is no longer considered "full",
      ** break out of the loop even if nRemaining is still greater than
      ** zero. The caller has an in-memory tree to flush to disk.  */
      if( bFlush && sortedDbIsFull(pDb)==0 ) break;
    }
  }

  if( pnWrite ) *pnWrite = (nWork - nRemaining);
  pWorker->nWrite += (nWork - nRemaining);

#ifdef LSM_LOG_WORK
  lsmLogMessage(pDb, rc, "sortedWork(): %d pages", (nWork-nRemaining));
#endif
  return rc;
}

/*
** The database connection passed as the first argument must be a worker
** connection. This function checks if there exists an "old" in-memory tree
** ready to be flushed to disk. If so, true is returned. Otherwise false.
**
** If an error occurs, *pRc is set to an LSM error code before returning.
** It is assumed that *pRc is set to LSM_OK when this function is called.
*/
static int sortedTreeHasOld(lsm_db *pDb, int *pRc){
  int rc = LSM_OK;
  int bRet = 0;

  assert( pDb->pWorker );
  if( *pRc==LSM_OK ){
    if( rc==LSM_OK 
        && pDb->treehdr.iOldShmid
        && pDb->treehdr.iOldLog!=pDb->pWorker->iLogOff 
      ){
      bRet = 1;
    }else{
      bRet = 0;
    }
    *pRc = rc;
  }
  assert( *pRc==LSM_OK || bRet==0 );
  return bRet;
}

/*
** Create a new free-list only top-level segment. Return LSM_OK if successful
** or an LSM error code if some error occurs.
*/
static int sortedNewFreelistOnly(lsm_db *pDb){
  return sortedNewToplevel(pDb, TREE_NONE, 0);
}

static int lsmSaveWorker(lsm_db *pDb, int bFlush){
  Snapshot *p = pDb->pWorker;
  if( p->freelist.nEntry>pDb->nMaxFreelist ){
    int rc = sortedNewFreelistOnly(pDb);
    if( rc!=LSM_OK ) return rc;
  }
  return lsmCheckpointSaveWorker(pDb, bFlush);
}

static int doLsmSingleWork(
  lsm_db *pDb, 
  int bShutdown,
  int nMerge,                     /* Minimum segments to merge together */
  int nPage,                      /* Number of pages to write to disk */
  int *pnWrite,                   /* OUT: Pages actually written to disk */
  int *pbCkpt                     /* OUT: True if an auto-checkpoint is req. */
){
  Snapshot *pWorker;              /* Worker snapshot */
  int rc = LSM_OK;                /* Return code */
  int bDirty = 0;
  int nMax = nPage;               /* Maximum pages to write to disk */
  int nRem = nPage;
  int bCkpt = 0;

  assert( nPage>0 );

  /* Open the worker 'transaction'. It will be closed before this function
  ** returns.  */
  assert( pDb->pWorker==0 );
  rc = lsmBeginWork(pDb);
  if( rc!=LSM_OK ) return rc;
  pWorker = pDb->pWorker;

  /* If this connection is doing auto-checkpoints, set nMax (and nRem) so
  ** that this call stops writing when the auto-checkpoint is due. The
  ** caller will do the checkpoint, then possibly call this function again. */
  if( bShutdown==0 && pDb->nAutockpt ){
    u32 nSync;
    u32 nUnsync;
    int nPgsz;

    lsmCheckpointSynced(pDb, 0, 0, &nSync);
    nUnsync = lsmCheckpointNWrite(pDb->pShmhdr->aSnap1, 0);
    nPgsz = lsmCheckpointPgsz(pDb->pShmhdr->aSnap1);

    nMax = (int)LSM_MIN(nMax, (pDb->nAutockpt/nPgsz) - (int)(nUnsync-nSync));
    if( nMax<nRem ){
      bCkpt = 1;
      nRem = LSM_MAX(nMax, 0);
    }
  }

  /* If there exists in-memory data ready to be flushed to disk, attempt
  ** to flush it now.  */
  if( pDb->nTransOpen==0 ){
    rc = lsmTreeLoadHeader(pDb, 0);
  }
  if( sortedTreeHasOld(pDb, &rc) ){
    /* sortedDbIsFull() returns non-zero if either (a) there are too many
    ** levels in total in the db, or (b) there are too many levels with the
    ** the same age in the db. Either way, call sortedWork() to merge 
    ** existing segments together until this condition is cleared.  */
    if( sortedDbIsFull(pDb) ){
      int nPg = 0;
      rc = sortedWork(pDb, nRem, nMerge, 1, &nPg);
      nRem -= nPg;
      assert( rc!=LSM_OK || nRem<=0 || !sortedDbIsFull(pDb) );
      bDirty = 1;
    }

    if( rc==LSM_OK && nRem>0 ){
      int nPg = 0;
      rc = sortedNewToplevel(pDb, TREE_OLD, &nPg);
      nRem -= nPg;
      if( rc==LSM_OK ){
        if( pDb->nTransOpen>0 ){
          lsmTreeDiscardOld(pDb);
        }
        rc = lsmSaveWorker(pDb, 1);
        bDirty = 0;
      }
    }
  }

  /* If nPage is still greater than zero, do some merging. */
  if( rc==LSM_OK && nRem>0 && bShutdown==0 ){
    int nPg = 0;
    rc = sortedWork(pDb, nRem, nMerge, 0, &nPg);
    nRem -= nPg;
    if( nPg ) bDirty = 1;
  }

  /* If the in-memory part of the free-list is too large, write a new 
  ** top-level containing just the in-memory free-list entries to disk. */
  if( rc==LSM_OK && pDb->pWorker->freelist.nEntry > pDb->nMaxFreelist ){
    while( rc==LSM_OK && lsmDatabaseFull(pDb) ){
      int nPg = 0;
      rc = sortedWork(pDb, 16, nMerge, 1, &nPg);
      nRem -= nPg;
    }
    if( rc==LSM_OK ){
      rc = sortedNewFreelistOnly(pDb);
    }
    bDirty = 1;
  }

  if( rc==LSM_OK ){
    *pnWrite = (nMax - nRem);
    *pbCkpt = (bCkpt && nRem<=0);
    if( nMerge==1 && pDb->nAutockpt>0 && *pnWrite>0
     && pWorker->pLevel 
     && pWorker->pLevel->nRight==0 
     && pWorker->pLevel->pNext==0 
    ){
      *pbCkpt = 1;
    }
  }

  if( rc==LSM_OK && bDirty ){
    lsmFinishWork(pDb, 0, &rc);
  }else{
    int rcdummy = LSM_BUSY;
    lsmFinishWork(pDb, 0, &rcdummy);
    *pnWrite = 0;
  }
  assert( pDb->pWorker==0 );
  return rc;
}

static int doLsmWork(lsm_db *pDb, int nMerge, int nPage, int *pnWrite){
  int rc = LSM_OK;                /* Return code */
  int nWrite = 0;                 /* Number of pages written */

  assert( nMerge>=1 );

  if( nPage!=0 ){
    int bCkpt = 0;
    do {
      int nThis = 0;
      int nReq = (nPage>=0) ? (nPage-nWrite) : ((int)0x7FFFFFFF);

      bCkpt = 0;
      rc = doLsmSingleWork(pDb, 0, nMerge, nReq, &nThis, &bCkpt);
      nWrite += nThis;
      if( rc==LSM_OK && bCkpt ){
        rc = lsm_checkpoint(pDb, 0);
      }
    }while( rc==LSM_OK && bCkpt && (nWrite<nPage || nPage<0) );
  }

  if( pnWrite ){
    if( rc==LSM_OK ){
      *pnWrite = nWrite;
    }else{
      *pnWrite = 0;
    }
  }
  return rc;
}

/*
** Perform work to merge database segments together.
*/
int lsm_work(lsm_db *pDb, int nMerge, int nKB, int *pnWrite){
  int rc;                         /* Return code */
  int nPgsz;                      /* Nominal page size in bytes */
  int nPage;                      /* Equivalent of nKB in pages */
  int nWrite = 0;                 /* Number of pages written */

  /* This function may not be called if pDb has an open read or write
  ** transaction. Return LSM_MISUSE if an application attempts this.  */
  if( pDb->nTransOpen || pDb->pCsr ) return LSM_MISUSE_BKPT;
  if( nMerge<=0 ) nMerge = pDb->nMerge;

  lsmFsPurgeCache(pDb->pFS);

  /* Convert from KB to pages */
  nPgsz = lsmFsPageSize(pDb->pFS);
  if( nKB>=0 ){
    nPage = ((i64)nKB * 1024 + nPgsz - 1) / nPgsz;
  }else{
    nPage = -1;
  }

  rc = doLsmWork(pDb, nMerge, nPage, &nWrite);
  
  if( pnWrite ){
    /* Convert back from pages to KB */
    *pnWrite = (int)(((i64)nWrite * 1024 + nPgsz - 1) / nPgsz);
  }
  return rc;
}

int lsm_flush(lsm_db *db){
  int rc;

  if( db->nTransOpen>0 || db->pCsr ){
    rc = LSM_MISUSE_BKPT;
  }else{
    rc = lsmBeginWriteTrans(db);
    if( rc==LSM_OK ){
      lsmFlushTreeToDisk(db);
      lsmTreeDiscardOld(db);
      lsmTreeMakeOld(db);
      lsmTreeDiscardOld(db);
    }

    if( rc==LSM_OK ){
      rc = lsmFinishWriteTrans(db, 1);
    }else{
      lsmFinishWriteTrans(db, 0);
    }
    lsmFinishReadTrans(db);
  }

  return rc;
}

/*
** This function is called in auto-work mode to perform merging work on
** the data structure. It performs enough merging work to prevent the
** height of the tree from growing indefinitely assuming that roughly
** nUnit database pages worth of data have been written to the database
** (i.e. the in-memory tree) since the last call.
*/
static int lsmSortedAutoWork(
  lsm_db *pDb,                    /* Database handle */
  int nUnit                       /* Pages of data written to in-memory tree */
){
  int rc = LSM_OK;                /* Return code */
  int nDepth = 0;                 /* Current height of tree (longest path) */
  Level *pLevel;                  /* Used to iterate through levels */
  int bRestore = 0;

  assert( pDb->pWorker==0 );
  assert( pDb->nTransOpen>0 );

  /* Determine how many units of work to do before returning. One unit of
  ** work is achieved by writing one page (~4KB) of merged data.  */
  for(pLevel=lsmDbSnapshotLevel(pDb->pClient); pLevel; pLevel=pLevel->pNext){
    /* nDepth += LSM_MAX(1, pLevel->nRight); */
    nDepth += 1;
  }
  if( lsmTreeHasOld(pDb) ){
    nDepth += 1;
    bRestore = 1;
    rc = lsmSaveCursors(pDb);
    if( rc!=LSM_OK ) return rc;
  }

  if( nDepth>0 ){
    int nRemaining;               /* Units of work to do before returning */

    nRemaining = nUnit * nDepth;
#ifdef LSM_LOG_WORK
    lsmLogMessage(pDb, rc, "lsmSortedAutoWork(): %d*%d = %d pages", 
        nUnit, nDepth, nRemaining);
#endif
    assert( nRemaining>=0 );
    rc = doLsmWork(pDb, pDb->nMerge, nRemaining, 0);
    if( rc==LSM_BUSY ) rc = LSM_OK;

    if( bRestore && pDb->pCsr ){
      lsmMCursorFreeCache(pDb);
      lsmFreeSnapshot(pDb->pEnv, pDb->pClient);
      pDb->pClient = 0;
      if( rc==LSM_OK ){
        rc = lsmCheckpointLoad(pDb, 0);
      }
      if( rc==LSM_OK ){
        rc = lsmCheckpointDeserialize(pDb, 0, pDb->aSnapshot, &pDb->pClient);
      }
      if( rc==LSM_OK ){
        rc = lsmRestoreCursors(pDb);
      }
    }
  }

  return rc;
}

/*
** This function is only called during system shutdown. The contents of
** any in-memory trees present (old or current) are written out to disk.
*/
static int lsmFlushTreeToDisk(lsm_db *pDb){
  int rc;

  rc = lsmBeginWork(pDb);
  while( rc==LSM_OK && sortedDbIsFull(pDb) ){
    rc = sortedWork(pDb, 256, pDb->nMerge, 1, 0);
  }

  if( rc==LSM_OK ){
    rc = sortedNewToplevel(pDb, TREE_BOTH, 0);
  }

  lsmFinishWork(pDb, 1, &rc);
  return rc;
}

/*
** Return a string representation of the segment passed as the only argument.
** Space for the returned string is allocated using lsmMalloc(), and should
** be freed by the caller using lsmFree().
*/
static char *segToString(lsm_env *pEnv, Segment *pSeg, int nMin){
  LsmPgno nSize = pSeg->nSize;
  LsmPgno iRoot = pSeg->iRoot;
  LsmPgno iFirst = pSeg->iFirst;
  LsmPgno iLast = pSeg->iLastPg;
  char *z;

  char *z1;
  char *z2;
  int nPad;

  z1 = lsmMallocPrintf(pEnv, "%d.%d", iFirst, iLast);
  if( iRoot ){
    z2 = lsmMallocPrintf(pEnv, "root=%lld", iRoot);
  }else{
    z2 = lsmMallocPrintf(pEnv, "size=%lld", nSize);
  }

  nPad = nMin - 2 - strlen(z1) - 1 - strlen(z2);
  nPad = LSM_MAX(0, nPad);

  if( iRoot ){
    z = lsmMallocPrintf(pEnv, "/%s %*s%s\\", z1, nPad, "", z2);
  }else{
    z = lsmMallocPrintf(pEnv, "|%s %*s%s|", z1, nPad, "", z2);
  }
  lsmFree(pEnv, z1);
  lsmFree(pEnv, z2);

  return z;
}

static int fileToString(
  lsm_db *pDb,                    /* For xMalloc() */
  char *aBuf, 
  int nBuf, 
  int nMin,
  Segment *pSeg
){
  int i = 0;
  if( pSeg ){
    char *zSeg;

    zSeg = segToString(pDb->pEnv, pSeg, nMin);
    snprintf(&aBuf[i], nBuf-i, "%s", zSeg);
    i += strlen(&aBuf[i]);
    lsmFree(pDb->pEnv, zSeg);

#ifdef LSM_LOG_FREELIST
    lsmInfoArrayStructure(pDb, 1, pSeg->iFirst, &zSeg);
    snprintf(&aBuf[i], nBuf-1, "    (%s)", zSeg);
    i += strlen(&aBuf[i]);
    lsmFree(pDb->pEnv, zSeg);
#endif
    aBuf[nBuf] = 0;
  }else{
    aBuf[0] = '\0';
  }

  return i;
}

void sortedDumpPage(lsm_db *pDb, Segment *pRun, Page *pPg, int bVals){
  LsmBlob blob = {0, 0, 0};       /* LsmBlob used for keys */
  LsmString s;
  int i;

  int nRec;
  LsmPgno iPtr;
  int flags;
  u8 *aData;
  int nData;

  aData = fsPageData(pPg, &nData);

  nRec = pageGetNRec(aData, nData);
  iPtr = pageGetPtr(aData, nData);
  flags = pageGetFlags(aData, nData);

  lsmStringInit(&s, pDb->pEnv);
  lsmStringAppendf(&s,"nCell=%d iPtr=%lld flags=%d {", nRec, iPtr, flags);
  if( flags&SEGMENT_BTREE_FLAG ) iPtr = 0;

  for(i=0; i<nRec; i++){
    Page *pRef = 0;               /* Pointer to page iRef */
    int iChar;
    u8 *aKey; int nKey = 0;       /* Key */
    u8 *aVal = 0; int nVal = 0;   /* Value */
    int iTopic;
    u8 *aCell;
    i64 iPgPtr;
    int eType;

    aCell = pageGetCell(aData, nData, i);
    eType = *aCell++;
    assert( (flags & SEGMENT_BTREE_FLAG) || eType!=0 );
    aCell += lsmVarintGet64(aCell, &iPgPtr);

    if( eType==0 ){
      LsmPgno iRef;               /* Page number of referenced page */
      aCell += lsmVarintGet64(aCell, &iRef);
      lsmFsDbPageGet(pDb->pFS, pRun, iRef, &pRef);
      aKey = pageGetKey(pRun, pRef, 0, &iTopic, &nKey, &blob);
    }else{
      aCell += lsmVarintGet32(aCell, &nKey);
      if( rtIsWrite(eType) ) aCell += lsmVarintGet32(aCell, &nVal);
      sortedReadData(0, pPg, (aCell-aData), nKey+nVal, (void **)&aKey, &blob);
      aVal = &aKey[nKey];
      iTopic = eType;
    }

    lsmStringAppendf(&s, "%s%2X:", (i==0?"":" "), iTopic);
    for(iChar=0; iChar<nKey; iChar++){
      lsmStringAppendf(&s, "%c", isalnum(aKey[iChar]) ? aKey[iChar] : '.');
    }
    if( nVal>0 && bVals ){
      lsmStringAppendf(&s, "##");
      for(iChar=0; iChar<nVal; iChar++){
        lsmStringAppendf(&s, "%c", isalnum(aVal[iChar]) ? aVal[iChar] : '.');
      }
    }

    lsmStringAppendf(&s, " %lld", iPgPtr+iPtr);
    lsmFsPageRelease(pRef);
  }
  lsmStringAppend(&s, "}", 1);

  lsmLogMessage(pDb, LSM_OK, "      Page %d: %s", lsmFsPageNumber(pPg), s.z);
  lsmStringClear(&s);

  sortedBlobFree(&blob);
}

static void infoCellDump(
  lsm_db *pDb,                    /* Database handle */
  Segment *pSeg,                  /* Segment page belongs to */
  int bIndirect,                  /* True to follow indirect refs */
  Page *pPg,
  int iCell,
  int *peType,
  int *piPgPtr,
  u8 **paKey, int *pnKey,
  u8 **paVal, int *pnVal,
  LsmBlob *pBlob
){
  u8 *aData; int nData;           /* Page data */
  u8 *aKey; int nKey = 0;         /* Key */
  u8 *aVal = 0; int nVal = 0;     /* Value */
  int eType;
  int iPgPtr;
  Page *pRef = 0;                 /* Pointer to page iRef */
  u8 *aCell;

  aData = fsPageData(pPg, &nData);

  aCell = pageGetCell(aData, nData, iCell);
  eType = *aCell++;
  aCell += lsmVarintGet32(aCell, &iPgPtr);

  if( eType==0 ){
    int dummy;
    LsmPgno iRef;                 /* Page number of referenced page */
    aCell += lsmVarintGet64(aCell, &iRef);
    if( bIndirect ){
      lsmFsDbPageGet(pDb->pFS, pSeg, iRef, &pRef);
      pageGetKeyCopy(pDb->pEnv, pSeg, pRef, 0, &dummy, pBlob);
      aKey = (u8 *)pBlob->pData;
      nKey = pBlob->nData;
      lsmFsPageRelease(pRef);
    }else{
      aKey = (u8 *)"<indirect>";
      nKey = 11;
    }
  }else{
    aCell += lsmVarintGet32(aCell, &nKey);
    if( rtIsWrite(eType) ) aCell += lsmVarintGet32(aCell, &nVal);
    sortedReadData(pSeg, pPg, (aCell-aData), nKey+nVal, (void **)&aKey, pBlob);
    aVal = &aKey[nKey];
  }

  if( peType ) *peType = eType;
  if( piPgPtr ) *piPgPtr = iPgPtr;
  if( paKey ) *paKey = aKey;
  if( paVal ) *paVal = aVal;
  if( pnKey ) *pnKey = nKey;
  if( pnVal ) *pnVal = nVal;
}

static int infoAppendBlob(LsmString *pStr, int bHex, u8 *z, int n){
  int iChar;
  for(iChar=0; iChar<n; iChar++){
    if( bHex ){
      lsmStringAppendf(pStr, "%02X", z[iChar]);
    }else{
      lsmStringAppendf(pStr, "%c", isalnum(z[iChar]) ?z[iChar] : '.');
    }
  }
  return LSM_OK;
}

#define INFO_PAGE_DUMP_DATA     0x01
#define INFO_PAGE_DUMP_VALUES   0x02
#define INFO_PAGE_DUMP_HEX      0x04
#define INFO_PAGE_DUMP_INDIRECT 0x08

static int infoPageDump(
  lsm_db *pDb,                    /* Database handle */
  LsmPgno iPg,                    /* Page number of page to dump */
  int flags,
  char **pzOut                    /* OUT: lsmMalloc'd string */
){
  int rc = LSM_OK;                /* Return code */
  Page *pPg = 0;                  /* Handle for page iPg */
  int i, j;                       /* Loop counters */
  const int perLine = 16;         /* Bytes per line in the raw hex dump */
  Segment *pSeg = 0;
  Snapshot *pSnap;

  int bValues = (flags & INFO_PAGE_DUMP_VALUES);
  int bHex = (flags & INFO_PAGE_DUMP_HEX);
  int bData = (flags & INFO_PAGE_DUMP_DATA);
  int bIndirect = (flags & INFO_PAGE_DUMP_INDIRECT);

  *pzOut = 0;
  if( iPg==0 ) return LSM_ERROR;

  assert( pDb->pClient || pDb->pWorker );
  pSnap = pDb->pClient;
  if( pSnap==0 ) pSnap = pDb->pWorker;
  if( pSnap->redirect.n>0 ){
    Level *pLvl;
    int bUse = 0;
    for(pLvl=pSnap->pLevel; pLvl->pNext; pLvl=pLvl->pNext);
    pSeg = (pLvl->nRight==0 ? &pLvl->lhs : &pLvl->aRhs[pLvl->nRight-1]);
    rc = lsmFsSegmentContainsPg(pDb->pFS, pSeg, iPg, &bUse);
    if( bUse==0 ){
      pSeg = 0;
    }
  }

  /* iPg is a real page number (not subject to redirection). So it is safe 
  ** to pass a NULL in place of the segment pointer as the second argument
  ** to lsmFsDbPageGet() here.  */
  if( rc==LSM_OK ){
    rc = lsmFsDbPageGet(pDb->pFS, 0, iPg, &pPg);
  }

  if( rc==LSM_OK ){
    LsmBlob blob = {0, 0, 0, 0};
    int nKeyWidth = 0;
    LsmString str;
    int nRec;
    LsmPgno iPtr;
    int flags2;
    int iCell;
    u8 *aData; int nData;         /* Page data and size thereof */

    aData = fsPageData(pPg, &nData);
    nRec = pageGetNRec(aData, nData);
    iPtr = pageGetPtr(aData, nData);
    flags2 = pageGetFlags(aData, nData);

    lsmStringInit(&str, pDb->pEnv);
    lsmStringAppendf(&str, "Page : %lld  (%d bytes)\n", iPg, nData);
    lsmStringAppendf(&str, "nRec : %d\n", nRec);
    lsmStringAppendf(&str, "iPtr : %lld\n", iPtr);
    lsmStringAppendf(&str, "flags: %04x\n", flags2);
    lsmStringAppendf(&str, "\n");

    for(iCell=0; iCell<nRec; iCell++){
      int nKey;
      infoCellDump(
          pDb, pSeg, bIndirect, pPg, iCell, 0, 0, 0, &nKey, 0, 0, &blob
      );
      if( nKey>nKeyWidth ) nKeyWidth = nKey;
    }
    if( bHex ) nKeyWidth = nKeyWidth * 2;

    for(iCell=0; iCell<nRec; iCell++){
      u8 *aKey; int nKey = 0;       /* Key */
      u8 *aVal; int nVal = 0;       /* Value */
      int iPgPtr;
      int eType;
      LsmPgno iAbsPtr;
      char zFlags[8];

      infoCellDump(pDb, pSeg, bIndirect, pPg, iCell, &eType, &iPgPtr,
          &aKey, &nKey, &aVal, &nVal, &blob
      );
      iAbsPtr = iPgPtr + ((flags2 & SEGMENT_BTREE_FLAG) ? 0 : iPtr);

      lsmFlagsToString(eType, zFlags);
      lsmStringAppendf(&str, "%s %d (%s) ", 
          zFlags, iAbsPtr, (rtTopic(eType) ? "sys" : "usr")
      );
      infoAppendBlob(&str, bHex, aKey, nKey); 
      if( nVal>0 && bValues ){
        lsmStringAppendf(&str, "%*s", nKeyWidth - (nKey*(1+bHex)), "");
        lsmStringAppendf(&str, " ");
        infoAppendBlob(&str, bHex, aVal, nVal); 
      }
      if( rtTopic(eType) ){
        int iBlk = (int)~lsmGetU32(aKey);
        lsmStringAppendf(&str, "  (block=%d", iBlk);
        if( nVal>0 ){
          i64 iSnap = lsmGetU64(aVal);
          lsmStringAppendf(&str, " snapshot=%lld", iSnap);
        }
        lsmStringAppendf(&str, ")");
      }
      lsmStringAppendf(&str, "\n");
    }

    if( bData ){
      lsmStringAppendf(&str, "\n-------------------" 
          "-------------------------------------------------------------\n");
      lsmStringAppendf(&str, "Page %d\n",
          iPg, (iPg-1)*nData, iPg*nData - 1);
      for(i=0; i<nData; i += perLine){
        lsmStringAppendf(&str, "%04x: ", i);
        for(j=0; j<perLine; j++){
          if( i+j>nData ){
            lsmStringAppendf(&str, "   ");
          }else{
            lsmStringAppendf(&str, "%02x ", aData[i+j]);
          }
        }
        lsmStringAppendf(&str, "  ");
        for(j=0; j<perLine; j++){
          if( i+j>nData ){
            lsmStringAppendf(&str, " ");
          }else{
            lsmStringAppendf(&str,"%c", isprint(aData[i+j]) ? aData[i+j] : '.');
          }
        }
        lsmStringAppendf(&str,"\n");
      }
    }

    *pzOut = str.z;
    sortedBlobFree(&blob);
    lsmFsPageRelease(pPg);
  }

  return rc;
}

static int lsmInfoPageDump(
  lsm_db *pDb,                    /* Database handle */
  LsmPgno iPg,                    /* Page number of page to dump */
  int bHex,                       /* True to output key/value in hex form */
  char **pzOut                    /* OUT: lsmMalloc'd string */
){
  int flags = INFO_PAGE_DUMP_DATA | INFO_PAGE_DUMP_VALUES;
  if( bHex ) flags |= INFO_PAGE_DUMP_HEX;
  return infoPageDump(pDb, iPg, flags, pzOut);
}

void sortedDumpSegment(lsm_db *pDb, Segment *pRun, int bVals){
  assert( pDb->xLog );
  if( pRun && pRun->iFirst ){
    int flags = (bVals ? INFO_PAGE_DUMP_VALUES : 0);
    char *zSeg;
    Page *pPg;

    zSeg = segToString(pDb->pEnv, pRun, 0);
    lsmLogMessage(pDb, LSM_OK, "Segment: %s", zSeg);
    lsmFree(pDb->pEnv, zSeg);

    lsmFsDbPageGet(pDb->pFS, pRun, pRun->iFirst, &pPg);
    while( pPg ){
      Page *pNext;
      char *z = 0;
      infoPageDump(pDb, lsmFsPageNumber(pPg), flags, &z);
      lsmLogMessage(pDb, LSM_OK, "%s", z);
      lsmFree(pDb->pEnv, z);
#if 0
      sortedDumpPage(pDb, pRun, pPg, bVals);
#endif
      lsmFsDbPageNext(pRun, pPg, 1, &pNext);
      lsmFsPageRelease(pPg);
      pPg = pNext;
    }
  }
}

/*
** Invoke the log callback zero or more times with messages that describe
** the current database structure.
*/
static void lsmSortedDumpStructure(
  lsm_db *pDb,                    /* Database handle (used for xLog callback) */
  Snapshot *pSnap,                /* Snapshot to dump */
  int bKeys,                      /* Output the keys from each segment */
  int bVals,                      /* Output the values from each segment */
  const char *zWhy                /* Caption to print near top of dump */
){
  Snapshot *pDump = pSnap;
  Level *pTopLevel;
  char *zFree = 0;

  assert( pSnap );
  pTopLevel = lsmDbSnapshotLevel(pDump);
  if( pDb->xLog && pTopLevel ){
    static int nCall = 0;
    Level *pLevel;
    int iLevel = 0;

    nCall++;
    lsmLogMessage(pDb, LSM_OK, "Database structure %d (%s)", nCall, zWhy);

#if 0
    if( nCall==1031 || nCall==1032 ) bKeys=1;
#endif

    for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
      char zLeft[1024];
      char zRight[1024];
      int i = 0;

      Segment *aLeft[24];  
      Segment *aRight[24];

      int nLeft = 0;
      int nRight = 0;

      Segment *pSeg = &pLevel->lhs;
      aLeft[nLeft++] = pSeg;

      for(i=0; i<pLevel->nRight; i++){
        aRight[nRight++] = &pLevel->aRhs[i];
      }

#ifdef LSM_LOG_FREELIST
      if( nRight ){
        memmove(&aRight[1], aRight, sizeof(aRight[0])*nRight);
        aRight[0] = 0;
        nRight++;
      }
#endif

      for(i=0; i<nLeft || i<nRight; i++){
        int iPad = 0;
        char zLevel[32];
        zLeft[0] = '\0';
        zRight[0] = '\0';

        if( i<nLeft ){ 
          fileToString(pDb, zLeft, sizeof(zLeft), 24, aLeft[i]); 
        }
        if( i<nRight ){ 
          fileToString(pDb, zRight, sizeof(zRight), 24, aRight[i]); 
        }

        if( i==0 ){
          snprintf(zLevel, sizeof(zLevel), "L%d: (age=%d) (flags=%.4x)",
              iLevel, (int)pLevel->iAge, (int)pLevel->flags
          );
        }else{
          zLevel[0] = '\0';
        }

        if( nRight==0 ){
          iPad = 10;
        }

        lsmLogMessage(pDb, LSM_OK, "% 25s % *s% -35s %s", 
            zLevel, iPad, "", zLeft, zRight
        );
      }

      iLevel++;
    }

    if( bKeys ){
      for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
        int i;
        sortedDumpSegment(pDb, &pLevel->lhs, bVals);
        for(i=0; i<pLevel->nRight; i++){
          sortedDumpSegment(pDb, &pLevel->aRhs[i], bVals);
        }
      }
    }
  }

  lsmInfoFreelist(pDb, &zFree);
  lsmLogMessage(pDb, LSM_OK, "Freelist: %s", zFree);
  lsmFree(pDb->pEnv, zFree);

  assert( lsmFsIntegrityCheck(pDb) );
}

static void lsmSortedFreeLevel(lsm_env *pEnv, Level *pLevel){
  Level *pNext;
  Level *p;

  for(p=pLevel; p; p=pNext){
    pNext = p->pNext;
    sortedFreeLevel(pEnv, p);
  }
}

static void lsmSortedSaveTreeCursors(lsm_db *pDb){
  MultiCursor *pCsr;
  for(pCsr=pDb->pCsr; pCsr; pCsr=pCsr->pNext){
    lsmTreeCursorSave(pCsr->apTreeCsr[0]);
    lsmTreeCursorSave(pCsr->apTreeCsr[1]);
  }
}

static void lsmSortedExpandBtreePage(Page *pPg, int nOrig){
  u8 *aData;
  int nData;
  int nEntry;
  int iHdr;

  aData = lsmFsPageData(pPg, &nData);
  nEntry = pageGetNRec(aData, nOrig);
  iHdr = SEGMENT_EOF(nOrig, nEntry);
  memmove(&aData[iHdr + (nData-nOrig)], &aData[iHdr], nOrig-iHdr);
}

#ifdef LSM_DEBUG_EXPENSIVE
static void assertRunInOrder(lsm_db *pDb, Segment *pSeg){
  Page *pPg = 0;
  LsmBlob blob1 = {0, 0, 0, 0};
  LsmBlob blob2 = {0, 0, 0, 0};

  lsmFsDbPageGet(pDb->pFS, pSeg, pSeg->iFirst, &pPg);
  while( pPg ){
    u8 *aData; int nData;
    Page *pNext;

    aData = lsmFsPageData(pPg, &nData);
    if( 0==(pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG) ){
      int i;
      int nRec = pageGetNRec(aData, nData);
      for(i=0; i<nRec; i++){
        int iTopic1, iTopic2;
        pageGetKeyCopy(pDb->pEnv, pSeg, pPg, i, &iTopic1, &blob1);

        if( i==0 && blob2.nData ){
          assert( sortedKeyCompare(
                pDb->xCmp, iTopic2, blob2.pData, blob2.nData,
                iTopic1, blob1.pData, blob1.nData
          )<0 );
        }

        if( i<(nRec-1) ){
          pageGetKeyCopy(pDb->pEnv, pSeg, pPg, i+1, &iTopic2, &blob2);
          assert( sortedKeyCompare(
                pDb->xCmp, iTopic1, blob1.pData, blob1.nData,
                iTopic2, blob2.pData, blob2.nData
          )<0 );
        }
      }
    }

    lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
    lsmFsPageRelease(pPg);
    pPg = pNext;
  }

  sortedBlobFree(&blob1);
  sortedBlobFree(&blob2);
}
#endif

#ifdef LSM_DEBUG_EXPENSIVE
/*
** This function is only included in the build if LSM_DEBUG_EXPENSIVE is 
** defined. Its only purpose is to evaluate various assert() statements to 
** verify that the database is well formed in certain respects.
**
** More specifically, it checks that the array pOne contains the required 
** pointers to pTwo. Array pTwo must be a main array. pOne may be either a 
** separators array or another main array. If pOne does not contain the 
** correct set of pointers, an assert() statement fails.
*/
static int assertPointersOk(
  lsm_db *pDb,                    /* Database handle */
  Segment *pOne,                  /* Segment containing pointers */
  Segment *pTwo,                  /* Segment containing pointer targets */
  int bRhs                        /* True if pTwo may have been Gobble()d */
){
  int rc = LSM_OK;                /* Error code */
  SegmentPtr ptr1;                /* Iterates through pOne */
  SegmentPtr ptr2;                /* Iterates through pTwo */
  LsmPgno iPrev;

  assert( pOne && pTwo );

  memset(&ptr1, 0, sizeof(ptr1));
  memset(&ptr2, 0, sizeof(ptr1));
  ptr1.pSeg = pOne;
  ptr2.pSeg = pTwo;
  segmentPtrEndPage(pDb->pFS, &ptr1, 0, &rc);
  segmentPtrEndPage(pDb->pFS, &ptr2, 0, &rc);

  /* Check that the footer pointer of the first page of pOne points to
  ** the first page of pTwo. */
  iPrev = pTwo->iFirst;
  if( ptr1.iPtr!=iPrev && !bRhs ){
    assert( 0 );
  }

  if( rc==LSM_OK && ptr1.nCell>0 ){
    rc = segmentPtrLoadCell(&ptr1, 0);
  }
      
  while( rc==LSM_OK && ptr2.pPg ){
    LsmPgno iThis;

    /* Advance to the next page of segment pTwo that contains at least
    ** one cell. Break out of the loop if the iterator reaches EOF.  */
    do{
      rc = segmentPtrNextPage(&ptr2, 1);
      assert( rc==LSM_OK );
    }while( rc==LSM_OK && ptr2.pPg && ptr2.nCell==0 );
    if( rc!=LSM_OK || ptr2.pPg==0 ) break;
    iThis = lsmFsPageNumber(ptr2.pPg);

    if( (ptr2.flags & (PGFTR_SKIP_THIS_FLAG|SEGMENT_BTREE_FLAG))==0 ){

      /* Load the first cell in the array pTwo page. */
      rc = segmentPtrLoadCell(&ptr2, 0);

      /* Iterate forwards through pOne, searching for a key that matches the
      ** key ptr2.pKey/nKey. This key should have a pointer to the page that
      ** ptr2 currently points to. */
      while( rc==LSM_OK ){
        int res = rtTopic(ptr1.eType) - rtTopic(ptr2.eType);
        if( res==0 ){
          res = pDb->xCmp(ptr1.pKey, ptr1.nKey, ptr2.pKey, ptr2.nKey);
        }

        if( res<0 ){
          assert( bRhs || ptr1.iPtr+ptr1.iPgPtr==iPrev );
        }else if( res>0 ){
          assert( 0 );
        }else{
          assert( ptr1.iPtr+ptr1.iPgPtr==iThis );
          iPrev = iThis;
          break;
        }

        rc = segmentPtrAdvance(0, &ptr1, 0);
        if( ptr1.pPg==0 ){
          assert( 0 );
        }
      }
    }
  }

  segmentPtrReset(&ptr1, 0);
  segmentPtrReset(&ptr2, 0);
  return LSM_OK;
}

/*
** This function is only included in the build if LSM_DEBUG_EXPENSIVE is 
** defined. Its only purpose is to evaluate various assert() statements to 
** verify that the database is well formed in certain respects.
**
** More specifically, it checks that the b-tree embedded in array pRun
** contains the correct keys. If not, an assert() fails.
*/
static int assertBtreeOk(
  lsm_db *pDb,
  Segment *pSeg
){
  int rc = LSM_OK;                /* Return code */
  if( pSeg->iRoot ){
    LsmBlob blob = {0, 0, 0};     /* Buffer used to cache overflow keys */
    FileSystem *pFS = pDb->pFS;   /* File system to read from */
    Page *pPg = 0;                /* Main run page */
    BtreeCursor *pCsr = 0;        /* Btree cursor */

    rc = btreeCursorNew(pDb, pSeg, &pCsr);
    if( rc==LSM_OK ){
      rc = btreeCursorFirst(pCsr);
    }
    if( rc==LSM_OK ){
      rc = lsmFsDbPageGet(pFS, pSeg, pSeg->iFirst, &pPg);
    }

    while( rc==LSM_OK ){
      Page *pNext;
      u8 *aData;
      int nData;
      int flags;

      rc = lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
      lsmFsPageRelease(pPg);
      pPg = pNext;
      if( pPg==0 ) break;
      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( rc==LSM_OK 
       && 0==((SEGMENT_BTREE_FLAG|PGFTR_SKIP_THIS_FLAG) & flags)
       && 0!=pageGetNRec(aData, nData)
      ){
        u8 *pKey;
        int nKey;
        int iTopic;
        pKey = pageGetKey(pSeg, pPg, 0, &iTopic, &nKey, &blob);
        assert( nKey==pCsr->nKey && 0==memcmp(pKey, pCsr->pKey, nKey) );
        assert( lsmFsPageNumber(pPg)==pCsr->iPtr );
        rc = btreeCursorNext(pCsr);
      }
    }
    assert( rc!=LSM_OK || pCsr->pKey==0 );

    if( pPg ) lsmFsPageRelease(pPg);

    btreeCursorFree(pCsr);
    sortedBlobFree(&blob);
  }

  return rc;
}
#endif /* ifdef LSM_DEBUG_EXPENSIVE */

#line 1 "lsm_str.c"
/*
** 2012-04-27
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Dynamic string functions.
*/
/* #include "lsmInt.h" */

/*
** Turn bulk and uninitialized memory into an LsmString object
*/
static void lsmStringInit(LsmString *pStr, lsm_env *pEnv){
  memset(pStr, 0, sizeof(pStr[0]));
  pStr->pEnv = pEnv;
}

/*
** Increase the memory allocated for holding the string.  Realloc as needed.
**
** If a memory allocation error occurs, set pStr->n to -1 and free the existing
** allocation.  If a prior memory allocation has occurred, this routine is a
** no-op.
*/
static int lsmStringExtend(LsmString *pStr, int nNew){
  assert( nNew>0 );
  if( pStr->n<0 ) return LSM_NOMEM;
  if( pStr->n + nNew >= pStr->nAlloc ){
    int nAlloc = pStr->n + nNew + 100;
    char *zNew = lsmRealloc(pStr->pEnv, pStr->z, nAlloc);
    if( zNew==0 ){
      lsmFree(pStr->pEnv, pStr->z);
      nAlloc = 0;
      pStr->n = -1;
    }
    pStr->nAlloc = nAlloc;
    pStr->z = zNew;
  }
  return (pStr->z ? LSM_OK : LSM_NOMEM_BKPT);
}

/*
** Clear an LsmString object, releasing any allocated memory that it holds.
** This also clears the error indication (if any).
*/
static void lsmStringClear(LsmString *pStr){
  lsmFree(pStr->pEnv, pStr->z);
  lsmStringInit(pStr, pStr->pEnv);
}

/*
** Append N bytes of text to the end of an LsmString object.  If
** N is negative, append the entire string.
**
** If the string is in an error state, this routine is a no-op.
*/
static int lsmStringAppend(LsmString *pStr, const char *z, int N){
  int rc;
  if( N<0 ) N = (int)strlen(z);
  rc = lsmStringExtend(pStr, N+1);
  if( pStr->nAlloc ){
    memcpy(pStr->z+pStr->n, z, N+1);
    pStr->n += N;
  }
  return rc;
}

static int lsmStringBinAppend(LsmString *pStr, const u8 *a, int n){
  int rc;
  rc = lsmStringExtend(pStr, n);
  if( pStr->nAlloc ){
    memcpy(pStr->z+pStr->n, a, n);
    pStr->n += n;
  }
  return rc;
}

/*
** Append printf-formatted content to an LsmString.
*/
static void lsmStringVAppendf(
  LsmString *pStr, 
  const char *zFormat, 
  va_list ap1,
  va_list ap2
){
#if (!defined(__STDC_VERSION__) || (__STDC_VERSION__<199901L)) && \
    !defined(__APPLE__)
  extern int vsnprintf(char *str, size_t size, const char *format, va_list ap)
    /* Compatibility crutch for C89 compilation mode. sqlite3_vsnprintf()
       does not work identically and causes test failures if used here.
       For the time being we are assuming that the target has vsnprintf(),
       but that is not guaranteed to be the case for pure C89 platforms.
    */;
#endif
  int nWrite;
  int nAvail;

  nAvail = pStr->nAlloc - pStr->n;
  nWrite = vsnprintf(pStr->z + pStr->n, nAvail, zFormat, ap1);

  if( nWrite>=nAvail ){
    lsmStringExtend(pStr, nWrite+1);
    if( pStr->nAlloc==0 ) return;
    nWrite = vsnprintf(pStr->z + pStr->n, nWrite+1, zFormat, ap2);
  }

  pStr->n += nWrite;
  pStr->z[pStr->n] = 0;
}

static void lsmStringAppendf(LsmString *pStr, const char *zFormat, ...){
  va_list ap, ap2;
  va_start(ap, zFormat);
  va_start(ap2, zFormat);
  lsmStringVAppendf(pStr, zFormat, ap, ap2);
  va_end(ap);
  va_end(ap2);
}

static int lsmStrlen(const char *zName){
  int nRet = 0;
  while( zName[nRet] ) nRet++;
  return nRet;
}

/*
** Write into memory obtained from lsm_malloc().
*/
static char *lsmMallocPrintf(lsm_env *pEnv, const char *zFormat, ...){
  LsmString s;
  va_list ap, ap2;
  lsmStringInit(&s, pEnv);
  va_start(ap, zFormat);
  va_start(ap2, zFormat);
  lsmStringVAppendf(&s, zFormat, ap, ap2);
  va_end(ap);
  va_end(ap2);
  if( s.n<0 ) return 0;
  return (char *)lsmReallocOrFree(pEnv, s.z, s.n+1);
}

#line 1 "lsm_tree.c"
/*
** 2011-08-18
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains the implementation of an in-memory tree structure.
**
** Technically the tree is a B-tree of order 4 (in the Knuth sense - each 
** node may have up to 4 children). Keys are stored within B-tree nodes by
** reference. This may be slightly slower than a conventional red-black
** tree, but it is simpler. It is also an easier structure to modify to 
** create a version that supports nested transaction rollback.
**
** This tree does not currently support a delete operation. One is not 
** required. When LSM deletes a key from a database, it inserts a DELETE
** marker into the data structure. As a result, although the value associated
** with a key stored in the in-memory tree structure may be modified, no
** keys are ever removed. 
*/

/*
** MVCC NOTES
**
**   The in-memory tree structure supports SQLite-style MVCC. This means
**   that while one client is writing to the tree structure, other clients
**   may still be querying an older snapshot of the tree.
**
**   One way to implement this is to use an append-only b-tree. In this 
**   case instead of modifying nodes in-place, a copy of the node is made
**   and the required modifications made to the copy. The parent of the
**   node is then modified (to update the pointer so that it points to
**   the new copy), which causes a copy of the parent to be made, and so on.
**   This means that each time the tree is written to a new root node is
**   created. A snapshot is identified by the root node that it uses.
**
**   The problem with the above is that each time the tree is written to,
**   a copy of the node structure modified and all of its ancestor nodes
**   is made. This may prove excessive with large tree structures.
**
**   To reduce this overhead, the data structure used for a tree node is
**   designed so that it may be edited in place exactly once without 
**   affecting existing users. In other words, the node structure is capable
**   of storing two separate versions of the node at the same time.
**   When a node is to be edited, if the node structure already contains 
**   two versions, a copy is made as in the append-only approach. Or, if
**   it only contains a single version, it is edited in place.
**
**   This reduces the overhead so that, roughly, one new node structure
**   must be allocated for each write (on top of those allocations that 
**   would have been required by a non-MVCC tree). Logic: Assume that at 
**   any time, 50% of nodes in the tree already contain 2 versions. When
**   a new entry is written to a node, there is a 50% chance that a copy
**   of the node will be required. And a 25% chance that a copy of its 
**   parent is required. And so on.
**
** ROLLBACK
**
**   The in-memory tree also supports transaction and sub-transaction 
**   rollback. In order to rollback to point in time X, the following is
**   necessary:
**
**     1. All memory allocated since X must be freed, and 
**     2. All "v2" data adding to nodes that existed at X should be zeroed.
**     3. The root node must be restored to its X value.
**
**   The Mempool object used to allocate memory for the tree supports 
**   operation (1) - see the lsmPoolMark() and lsmPoolRevert() functions.
**
**   To support (2), all nodes that have v2 data are part of a singly linked 
**   list, sorted by the age of the v2 data (nodes that have had data added 
**   most recently are at the end of the list). So to zero all v2 data added
**   since X, the linked list is traversed from the first node added following
**   X onwards.
**
*/

#ifndef _LSM_INT_H
/* # include "lsmInt.h" */
#endif

#include <string.h>

#define MAX_DEPTH 32

typedef struct TreeKey TreeKey;
typedef struct TreeNode TreeNode;
typedef struct TreeLeaf TreeLeaf;
typedef struct NodeVersion NodeVersion;

struct TreeOld {
  u32 iShmid;                     /* Last shared-memory chunk in use by old */
  u32 iRoot;                      /* Offset of root node in shm file */
  u32 nHeight;                    /* Height of tree structure */
};

#if 0
/*
** assert() that a TreeKey.flags value is sane. Usage:
**
**   assert( lsmAssertFlagsOk(pTreeKey->flags) );
*/
static int lsmAssertFlagsOk(u8 keyflags){
  /* At least one flag must be set. Otherwise, what is this key doing? */
  assert( keyflags!=0 );

  /* The POINT_DELETE and INSERT flags cannot both be set. */
  assert( (keyflags & LSM_POINT_DELETE)==0 || (keyflags & LSM_INSERT)==0 );

  /* If both the START_DELETE and END_DELETE flags are set, then the INSERT
  ** flag must also be set. In other words - the three DELETE flags cannot
  ** all be set */
  assert( (keyflags & LSM_END_DELETE)==0 
       || (keyflags & LSM_START_DELETE)==0 
       || (keyflags & LSM_POINT_DELETE)==0 
  );

  return 1;
}
#endif
static int assert_delete_ranges_match(lsm_db *);
static int treeCountEntries(lsm_db *db);

/*
** Container for a key-value pair. Within the *-shm file, each key/value
** pair is stored in a single allocation (which may not actually be 
** contiguous in memory). Layout is the TreeKey structure, followed by
** the nKey bytes of key blob, followed by the nValue bytes of value blob
** (if nValue is non-negative).
*/
struct TreeKey {
  int nKey;                       /* Size of pKey in bytes */
  int nValue;                     /* Size of pValue. Or negative. */
  u8 flags;                       /* Various LSM_XXX flags */
};

#define TKV_KEY(p) ((void *)&(p)[1])
#define TKV_VAL(p) ((void *)(((u8 *)&(p)[1]) + (p)->nKey))

/*
** A single tree node. A node structure may contain up to 3 key/value
** pairs. Internal (non-leaf) nodes have up to 4 children.
**
** TODO: Update the format of this to be more compact. Get it working
** first though...
*/
struct TreeNode {
  u32 aiKeyPtr[3];                /* Array of pointers to TreeKey objects */

  /* The following fields are present for interior nodes only, not leaves. */
  u32 aiChildPtr[4];              /* Array of pointers to child nodes */

  /* The extra child pointer slot. */
  u32 iV2;                        /* Transaction number of v2 */
  u8 iV2Child;                    /* apChild[] entry replaced by pV2Ptr */
  u32 iV2Ptr;                     /* Substitute pointer */
};

struct TreeLeaf {
  u32 aiKeyPtr[3];                /* Array of pointers to TreeKey objects */
};

typedef struct TreeBlob TreeBlob;
struct TreeBlob {
  int n;
  u8 *a;
};

/*
** Cursor for searching a tree structure.
**
** If a cursor does not point to any element (a.k.a. EOF), then the
** TreeCursor.iNode variable is set to a negative value. Otherwise, the
** cursor currently points to key aiCell[iNode] on node apTreeNode[iNode].
**
** Entries in the apTreeNode[] and aiCell[] arrays contain the node and
** index of the TreeNode.apChild[] pointer followed to descend to the 
** current element. Hence apTreeNode[0] always contains the root node of
** the tree.
*/
struct TreeCursor {
  lsm_db *pDb;                    /* Database handle for this cursor */
  TreeRoot *pRoot;                /* Root node and height of tree to access */
  int iNode;                      /* Cursor points at apTreeNode[iNode] */
  TreeNode *apTreeNode[MAX_DEPTH];/* Current position in tree */
  u8 aiCell[MAX_DEPTH];           /* Current position in tree */
  TreeKey *pSave;                 /* Saved key */
  TreeBlob blob;                  /* Dynamic storage for a key */
};

/*
** A value guaranteed to be larger than the largest possible transaction
** id (TreeHeader.iTransId).
*/
#define WORKING_VERSION (1<<30)

static int tblobGrow(lsm_db *pDb, TreeBlob *p, int n, int *pRc){
  if( n>p->n ){
    lsmFree(pDb->pEnv, p->a);
    p->a = lsmMallocRc(pDb->pEnv, n, pRc);
    p->n = n;
  }
  return (p->a==0);
}
static void tblobFree(lsm_db *pDb, TreeBlob *p){
  lsmFree(pDb->pEnv, p->a);
}


/***********************************************************************
** Start of IntArray methods.  */
/*
** Append value iVal to the contents of IntArray *p. Return LSM_OK if 
** successful, or LSM_NOMEM if an OOM condition is encountered.
*/
static int intArrayAppend(lsm_env *pEnv, IntArray *p, u32 iVal){
  assert( p->nArray<=p->nAlloc );
  if( p->nArray>=p->nAlloc ){
    u32 *aNew;
    int nNew = p->nArray ? p->nArray*2 : 128;
    aNew = lsmRealloc(pEnv, p->aArray, nNew*sizeof(u32));
    if( !aNew ) return LSM_NOMEM_BKPT;
    p->aArray = aNew;
    p->nAlloc = nNew;
  }

  p->aArray[p->nArray++] = iVal;
  return LSM_OK;
}

/*
** Zero the IntArray object.
*/
static void intArrayFree(lsm_env *pEnv, IntArray *p){
  p->nArray = 0;
}

/*
** Return the number of entries currently in the int-array object.
*/
static int intArraySize(IntArray *p){
  return p->nArray;
}

/*
** Return a copy of the iIdx'th entry in the int-array.
*/
static u32 intArrayEntry(IntArray *p, int iIdx){
  return p->aArray[iIdx];
}

/*
** Truncate the int-array so that all but the first nVal values are 
** discarded.
*/
static void intArrayTruncate(IntArray *p, int nVal){
  p->nArray = nVal;
}
/* End of IntArray methods.
***********************************************************************/

static int treeKeycmp(void *p1, int n1, void *p2, int n2){
  int res;
  res = memcmp(p1, p2, LSM_MIN(n1, n2));
  if( res==0 ) res = (n1-n2);
  return res;
}

/*
** The pointer passed as the first argument points to an interior node,
** not a leaf. This function returns the offset of the iCell'th child
** sub-tree of the node.
*/
static u32 getChildPtr(TreeNode *p, int iVersion, int iCell){
  assert( iVersion>=0 );
  assert( iCell>=0 && iCell<=array_size(p->aiChildPtr) );
  if( p->iV2 && p->iV2<=(u32)iVersion && iCell==p->iV2Child ) return p->iV2Ptr;
  return p->aiChildPtr[iCell];
}

/*
** Given an offset within the *-shm file, return the associated chunk number.
*/
static int treeOffsetToChunk(u32 iOff){
  assert( LSM_SHM_CHUNK_SIZE==(1<<15) );
  return (int)(iOff>>15);
}

#define treeShmptrUnsafe(pDb, iPtr) \
(&((u8*)((pDb)->apShm[(iPtr)>>15]))[(iPtr) & (LSM_SHM_CHUNK_SIZE-1)])

/*
** Return a pointer to the mapped memory location associated with *-shm 
** file offset iPtr.
*/
static void *treeShmptr(lsm_db *pDb, u32 iPtr){

  assert( (iPtr>>15)<(u32)pDb->nShm );
  assert( pDb->apShm[iPtr>>15] );

  return iPtr ? treeShmptrUnsafe(pDb, iPtr) : 0;
}

static ShmChunk * treeShmChunk(lsm_db *pDb, int iChunk){
  return (ShmChunk *)(pDb->apShm[iChunk]);
}

static ShmChunk * treeShmChunkRc(lsm_db *pDb, int iChunk, int *pRc){
  assert( *pRc==LSM_OK );
  if( iChunk<pDb->nShm || LSM_OK==(*pRc = lsmShmCacheChunks(pDb, iChunk+1)) ){
    return (ShmChunk *)(pDb->apShm[iChunk]);
  }
  return 0;
}


#ifndef NDEBUG
static void assertIsWorkingChild(
  lsm_db *db, 
  TreeNode *pNode, 
  TreeNode *pParent, 
  int iCell
){
  TreeNode *p;
  u32 iPtr = getChildPtr(pParent, WORKING_VERSION, iCell);
  p = treeShmptr(db, iPtr);
  assert( p==pNode );
}
#else
# define assertIsWorkingChild(w,x,y,z)
#endif

/* Values for the third argument to treeShmkey(). */
#define TKV_LOADKEY  1
#define TKV_LOADVAL  2

static TreeKey *treeShmkey(
  lsm_db *pDb,                    /* Database handle */
  u32 iPtr,                       /* Shmptr to TreeKey struct */
  int eLoad,                      /* Either zero or a TREEKEY_LOADXXX value */
  TreeBlob *pBlob,                /* Used if dynamic memory is required */
  int *pRc                        /* IN/OUT: Error code */
){
  TreeKey *pRet;

  assert( eLoad==TKV_LOADKEY || eLoad==TKV_LOADVAL );
  pRet = (TreeKey *)treeShmptr(pDb, iPtr);
  if( pRet ){
    int nReq;                     /* Bytes of space required at pRet */
    int nAvail;                   /* Bytes of space available at pRet */

    nReq = sizeof(TreeKey) + pRet->nKey;
    if( eLoad==TKV_LOADVAL && pRet->nValue>0 ){
      nReq += pRet->nValue;
    }
    assert( LSM_SHM_CHUNK_SIZE==(1<<15) );
    nAvail = LSM_SHM_CHUNK_SIZE - (iPtr & (LSM_SHM_CHUNK_SIZE-1));

    if( nAvail<nReq ){
      if( tblobGrow(pDb, pBlob, nReq, pRc)==0 ){
        int nLoad = 0;
        while( *pRc==LSM_OK ){
          ShmChunk *pChunk;
          void *p = treeShmptr(pDb, iPtr);
          int n = LSM_MIN(nAvail, nReq-nLoad);

          memcpy(&pBlob->a[nLoad], p, n);
          nLoad += n;
          if( nLoad==nReq ) break;

          pChunk = treeShmChunk(pDb, treeOffsetToChunk(iPtr));
          assert( pChunk );
          iPtr = (pChunk->iNext * LSM_SHM_CHUNK_SIZE) + LSM_SHM_CHUNK_HDR;
          nAvail = LSM_SHM_CHUNK_SIZE - LSM_SHM_CHUNK_HDR;
        }
      }
      pRet = (TreeKey *)(pBlob->a);
    }
  }

  return pRet;
}

#if defined(LSM_DEBUG) && defined(LSM_EXPENSIVE_ASSERT)
void assert_leaf_looks_ok(TreeNode *pNode){
  assert( pNode->apKey[1] );
}

void assert_node_looks_ok(TreeNode *pNode, int nHeight){
  if( pNode ){
    assert( pNode->apKey[1] );
    if( nHeight>1 ){
      int i;
      assert( getChildPtr(pNode, WORKING_VERSION, 1) );
      assert( getChildPtr(pNode, WORKING_VERSION, 2) );
      for(i=0; i<4; i++){
        assert_node_looks_ok(getChildPtr(pNode, WORKING_VERSION, i), nHeight-1);
      }
    }
  }
}

/*
** Run various assert() statements to check that the working-version of the
** tree is correct in the following respects:
**
**   * todo...
*/
void assert_tree_looks_ok(int rc, Tree *pTree){
}
#else
# define assert_tree_looks_ok(x,y)
#endif

static void lsmFlagsToString(int flags, char *zFlags){

  zFlags[0] = (flags & LSM_END_DELETE)   ? ']' : '.';

  /* Only one of LSM_POINT_DELETE, LSM_INSERT and LSM_SEPARATOR should ever
  ** be set. If this is not true, write a '?' to the output.  */
  switch( flags & (LSM_POINT_DELETE|LSM_INSERT|LSM_SEPARATOR) ){
    case 0:                zFlags[1] = '.'; break;
    case LSM_POINT_DELETE: zFlags[1] = '-'; break;
    case LSM_INSERT:       zFlags[1] = '+'; break;
    case LSM_SEPARATOR:    zFlags[1] = '^'; break;
    default:               zFlags[1] = '?'; break;
  }

  zFlags[2] = (flags & LSM_SYSTEMKEY)    ? '*' : '.';
  zFlags[3] = (flags & LSM_START_DELETE) ? '[' : '.';
  zFlags[4] = '\0';
}

#ifdef LSM_DEBUG

/*
** Pointer pBlob points to a buffer containing a blob of binary data
** nBlob bytes long. Append the contents of this blob to *pStr, with
** each octet represented by a 2-digit hexadecimal number. For example,
** if the input blob is three bytes in size and contains {0x01, 0x44, 0xFF},
** then "0144ff" is appended to *pStr.
*/
static void lsmAppendStrBlob(LsmString *pStr, void *pBlob, int nBlob){
  int i;
  lsmStringExtend(pStr, nBlob*2);
  if( pStr->nAlloc==0 ) return;
  for(i=0; i<nBlob; i++){
    u8 c = ((u8*)pBlob)[i];
    if( c>='a' && c<='z' ){
      pStr->z[pStr->n++] = c;
    }else if( c!=0 || nBlob==1 || i!=(nBlob-1) ){
      pStr->z[pStr->n++] = "0123456789abcdef"[(c>>4)&0xf];
      pStr->z[pStr->n++] = "0123456789abcdef"[c&0xf];
    }
  }
  pStr->z[pStr->n] = 0;
}

#if 0  /* NOT USED */
/*
** Append nIndent space (0x20) characters to string *pStr.
*/
static void lsmAppendIndent(LsmString *pStr, int nIndent){
  int i;
  lsmStringExtend(pStr, nIndent);
  for(i=0; i<nIndent; i++) lsmStringAppend(pStr, " ", 1);
}
#endif

static void strAppendFlags(LsmString *pStr, u8 flags){
  char zFlags[8];

  lsmFlagsToString(flags, zFlags);
  zFlags[4] = ':';

  lsmStringAppend(pStr, zFlags, 5);
}

void dump_node_contents(
  lsm_db *pDb,
  u32 iNode,                      /* Print out the contents of this node */
  char *zPath,                    /* Path from root to this node */
  int nPath,                      /* Number of bytes in zPath */
  int nHeight                     /* Height: (0==leaf) (1==parent-of-leaf) */
){
  const char *zSpace = "                                           ";
  int i;
  int rc = LSM_OK;
  LsmString s;
  TreeNode *pNode;
  TreeBlob b = {0, 0};

  pNode = (TreeNode *)treeShmptr(pDb, iNode);

  if( nHeight==0 ){
    /* Append the nIndent bytes of space to string s. */
    lsmStringInit(&s, pDb->pEnv);

    /* Append each key to string s. */
    for(i=0; i<3; i++){
      u32 iPtr = pNode->aiKeyPtr[i];
      if( iPtr ){
        TreeKey *pKey = treeShmkey(pDb, pNode->aiKeyPtr[i],TKV_LOADKEY, &b,&rc);
        strAppendFlags(&s, pKey->flags);
        lsmAppendStrBlob(&s, TKV_KEY(pKey), pKey->nKey);
        lsmStringAppend(&s, "     ", -1);
      }
    }

    printf("% 6d %.*sleaf%.*s: %s\n", 
        iNode, nPath, zPath, 20-nPath-4, zSpace, s.z
    );
    lsmStringClear(&s);
  }else{
    for(i=0; i<4 && nHeight>0; i++){
      u32 iPtr = getChildPtr(pNode, pDb->treehdr.root.iTransId, i);
      zPath[nPath] = (char)(i+'0');
      zPath[nPath+1] = '/';

      if( iPtr ){
        dump_node_contents(pDb, iPtr, zPath, nPath+2, nHeight-1);
      }
      if( i!=3 && pNode->aiKeyPtr[i] ){
        TreeKey *pKey = treeShmkey(pDb, pNode->aiKeyPtr[i], TKV_LOADKEY,&b,&rc);
        lsmStringInit(&s, pDb->pEnv);
        strAppendFlags(&s, pKey->flags);
        lsmAppendStrBlob(&s, TKV_KEY(pKey), pKey->nKey);
        printf("% 6d %.*s%.*s: %s\n", 
            iNode, nPath+1, zPath, 20-nPath-1, zSpace, s.z);
        lsmStringClear(&s);
      }
    }
  }

  tblobFree(pDb, &b);
}

void dump_tree_contents(lsm_db *pDb, const char *zCaption){
  char zPath[64];
  TreeRoot *p = &pDb->treehdr.root;
  printf("\n%s\n", zCaption);
  zPath[0] = '/';
  if( p->iRoot ){
    dump_node_contents(pDb, p->iRoot, zPath, 1, p->nHeight-1);
  }
  fflush(stdout);
}

#endif

/*
** Initialize a cursor object, the space for which has already been
** allocated.
*/
static void treeCursorInit(lsm_db *pDb, int bOld, TreeCursor *pCsr){
  memset(pCsr, 0, sizeof(TreeCursor));
  pCsr->pDb = pDb;
  if( bOld ){
    pCsr->pRoot = &pDb->treehdr.oldroot;
  }else{
    pCsr->pRoot = &pDb->treehdr.root;
  }
  pCsr->iNode = -1;
}

/*
** Return a pointer to the mapping of the TreeKey object that the cursor
** is pointing to. 
*/
static TreeKey *csrGetKey(TreeCursor *pCsr, TreeBlob *pBlob, int *pRc){
  TreeKey *pRet;
  lsm_db *pDb = pCsr->pDb;
  u32 iPtr = pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[pCsr->aiCell[pCsr->iNode]];

  assert( iPtr );
  pRet = (TreeKey*)treeShmptrUnsafe(pDb, iPtr);
  if( !(pRet->flags & LSM_CONTIGUOUS) ){
    pRet = treeShmkey(pDb, iPtr, TKV_LOADVAL, pBlob, pRc);
  }

  return pRet;
}

/*
** Save the current position of tree cursor pCsr.
*/
static int lsmTreeCursorSave(TreeCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr && pCsr->pSave==0 ){
    int iNode = pCsr->iNode;
    if( iNode>=0 ){
      pCsr->pSave = csrGetKey(pCsr, &pCsr->blob, &rc);
    }
    pCsr->iNode = -1;
  }
  return rc;
}

/*
** Restore the position of a saved tree cursor.
*/
static int treeCursorRestore(TreeCursor *pCsr, int *pRes){
  int rc = LSM_OK;
  if( pCsr->pSave ){
    TreeKey *pKey = pCsr->pSave;
    pCsr->pSave = 0;
    if( pRes ){
      rc = lsmTreeCursorSeek(pCsr, TKV_KEY(pKey), pKey->nKey, pRes);
    }
  }
  return rc;
}

/*
** Allocate nByte bytes of space within the *-shm file. If successful, 
** return LSM_OK and set *piPtr to the offset within the file at which
** the allocated space is located.
*/
static u32 treeShmalloc(lsm_db *pDb, int bAlign, int nByte, int *pRc){
  u32 iRet = 0;
  if( *pRc==LSM_OK ){
    const static int CHUNK_SIZE = LSM_SHM_CHUNK_SIZE;
    const static int CHUNK_HDR = LSM_SHM_CHUNK_HDR;
    u32 iWrite;                   /* Current write offset */
    u32 iEof;                     /* End of current chunk */
    int iChunk;                   /* Current chunk */

    assert( nByte <= (CHUNK_SIZE-CHUNK_HDR) );

    /* Check if there is enough space on the current chunk to fit the
    ** new allocation. If not, link in a new chunk and put the new
    ** allocation at the start of it.  */
    iWrite = pDb->treehdr.iWrite;
    if( bAlign ){
      iWrite = (iWrite + 3) & ~0x0003;
      assert( (iWrite % 4)==0 );
    }

    assert( iWrite );
    iChunk = treeOffsetToChunk(iWrite-1);
    iEof = (iChunk+1) * CHUNK_SIZE;
    assert( iEof>=iWrite && (iEof-iWrite)<(u32)CHUNK_SIZE );
    if( (iWrite+nByte)>iEof ){
      ShmChunk *pHdr;           /* Header of chunk just finished (iChunk) */
      ShmChunk *pFirst;         /* Header of chunk treehdr.iFirst */
      ShmChunk *pNext;          /* Header of new chunk */
      int iNext = 0;            /* Next chunk */
      int rc = LSM_OK;

      pFirst = treeShmChunk(pDb, pDb->treehdr.iFirst);

      assert( shm_sequence_ge(pDb->treehdr.iUsedShmid, pFirst->iShmid) );
      assert( (pDb->treehdr.iNextShmid+1-pDb->treehdr.nChunk)==pFirst->iShmid );

      /* Check if the chunk at the start of the linked list is still in
      ** use. If not, reuse it. If so, allocate a new chunk by appending
      ** to the *-shm file.  */
      if( pDb->treehdr.iUsedShmid!=pFirst->iShmid ){
        int bInUse;
        rc = lsmTreeInUse(pDb, pFirst->iShmid, &bInUse);
        if( rc!=LSM_OK ){
          *pRc = rc;
          return 0;
        }
        if( bInUse==0 ){
          iNext = pDb->treehdr.iFirst;
          pDb->treehdr.iFirst = pFirst->iNext;
          assert( pDb->treehdr.iFirst );
        }
      }
      if( iNext==0 ) iNext = pDb->treehdr.nChunk++;

      /* Set the header values for the new chunk */
      pNext = treeShmChunkRc(pDb, iNext, &rc);
      if( pNext ){
        pNext->iNext = 0;
        pNext->iShmid = (pDb->treehdr.iNextShmid++);
      }else{
        *pRc = rc;
        return 0;
      }

      /* Set the header values for the chunk just finished */
      pHdr = (ShmChunk *)treeShmptr(pDb, iChunk*CHUNK_SIZE);
      pHdr->iNext = iNext;

      /* Advance to the next chunk */
      iWrite = iNext * CHUNK_SIZE + CHUNK_HDR;
    }

    /* Allocate space at iWrite. */
    iRet = iWrite;
    pDb->treehdr.iWrite = iWrite + nByte;
    pDb->treehdr.root.nByte += nByte;
  }
  return iRet;
}

/*
** Allocate and zero nByte bytes of space within the *-shm file.
*/
static void *treeShmallocZero(lsm_db *pDb, int nByte, u32 *piPtr, int *pRc){
  u32 iPtr;
  void *p;
  iPtr = treeShmalloc(pDb, 1, nByte, pRc);
  p = treeShmptr(pDb, iPtr);
  if( p ){
    assert( *pRc==LSM_OK );
    memset(p, 0, nByte);
    *piPtr = iPtr;
  }
  return p;
}

static TreeNode *newTreeNode(lsm_db *pDb, u32 *piPtr, int *pRc){
  return treeShmallocZero(pDb, sizeof(TreeNode), piPtr, pRc);
}

static TreeLeaf *newTreeLeaf(lsm_db *pDb, u32 *piPtr, int *pRc){
  return treeShmallocZero(pDb, sizeof(TreeLeaf), piPtr, pRc);
}

static TreeKey *newTreeKey(
  lsm_db *pDb, 
  u32 *piPtr, 
  void *pKey, int nKey,           /* Key data */
  void *pVal, int nVal,           /* Value data (or nVal<0 for delete) */
  int *pRc
){
  TreeKey *p;
  u32 iPtr;
  u32 iEnd;
  int nRem;
  u8 *a;
  int n;

  /* Allocate space for the TreeKey structure itself */
  *piPtr = iPtr = treeShmalloc(pDb, 1, sizeof(TreeKey), pRc);
  p = treeShmptr(pDb, iPtr);
  if( *pRc ) return 0;
  p->nKey = nKey;
  p->nValue = nVal;

  /* Allocate and populate the space required for the key and value. */
  n = nRem = nKey;
  a = (u8 *)pKey;
  while( a ){
    while( nRem>0 ){
      u8 *aAlloc;
      int nAlloc;
      u32 iWrite;

      iWrite = (pDb->treehdr.iWrite & (LSM_SHM_CHUNK_SIZE-1));
      iWrite = LSM_MAX(iWrite, LSM_SHM_CHUNK_HDR);
      nAlloc = LSM_MIN((LSM_SHM_CHUNK_SIZE-iWrite), (u32)nRem);

      aAlloc = treeShmptr(pDb, treeShmalloc(pDb, 0, nAlloc, pRc));
      if( aAlloc==0 ) break;
      memcpy(aAlloc, &a[n-nRem], nAlloc);
      nRem -= nAlloc;
    }
    a = pVal;
    n = nRem = nVal;
    pVal = 0;
  }

  iEnd = iPtr + sizeof(TreeKey) + nKey + LSM_MAX(0, nVal);
  if( (iPtr & ~(LSM_SHM_CHUNK_SIZE-1))!=(iEnd & ~(LSM_SHM_CHUNK_SIZE-1)) ){
    p->flags = 0;
  }else{
    p->flags = LSM_CONTIGUOUS;
  }

  if( *pRc ) return 0;
#if 0
  printf("store: %d %s\n", (int)iPtr, (char *)pKey);
#endif
  return p;
}

static TreeNode *copyTreeNode(
  lsm_db *pDb, 
  TreeNode *pOld, 
  u32 *piNew, 
  int *pRc
){
  TreeNode *pNew;

  pNew = newTreeNode(pDb, piNew, pRc);
  if( pNew ){
    memcpy(pNew->aiKeyPtr, pOld->aiKeyPtr, sizeof(pNew->aiKeyPtr));
    memcpy(pNew->aiChildPtr, pOld->aiChildPtr, sizeof(pNew->aiChildPtr));
    if( pOld->iV2 ) pNew->aiChildPtr[pOld->iV2Child] = pOld->iV2Ptr;
  }
  return pNew;
}

static TreeNode *copyTreeLeaf(
  lsm_db *pDb, 
  TreeLeaf *pOld, 
  u32 *piNew, 
  int *pRc
){
  TreeLeaf *pNew;
  pNew = newTreeLeaf(pDb, piNew, pRc);
  if( pNew ){
    memcpy(pNew, pOld, sizeof(TreeLeaf));
  }
  return (TreeNode *)pNew;
}

/*
** The tree cursor passed as the second argument currently points to an 
** internal node (not a leaf). Specifically, to a sub-tree pointer. This
** function replaces the sub-tree that the cursor currently points to
** with sub-tree pNew.
**
** The sub-tree may be replaced either by writing the "v2 data" on the
** internal node, or by allocating a new TreeNode structure and then 
** calling this function on the parent of the internal node.
*/
static int treeUpdatePtr(lsm_db *pDb, TreeCursor *pCsr, u32 iNew){
  int rc = LSM_OK;
  if( pCsr->iNode<0 ){
    /* iNew is the new root node */
    pDb->treehdr.root.iRoot = iNew;
  }else{
    /* If this node already has version 2 content, allocate a copy and
    ** update the copy with the new pointer value. Otherwise, store the
    ** new pointer as v2 data within the current node structure.  */

    TreeNode *p;                  /* The node to be modified */
    int iChildPtr;                /* apChild[] entry to modify */

    p = pCsr->apTreeNode[pCsr->iNode];
    iChildPtr = pCsr->aiCell[pCsr->iNode];

    if( p->iV2 ){
      /* The "allocate new TreeNode" option */
      u32 iCopy;
      TreeNode *pCopy;
      pCopy = copyTreeNode(pDb, p, &iCopy, &rc);
      if( pCopy ){
        assert( rc==LSM_OK );
        pCopy->aiChildPtr[iChildPtr] = iNew;
        pCsr->iNode--;
        rc = treeUpdatePtr(pDb, pCsr, iCopy);
      }
    }else{
      /* The "v2 data" option */
      u32 iPtr;
      assert( pDb->treehdr.root.iTransId>0 );

      if( pCsr->iNode ){
        iPtr = getChildPtr(
            pCsr->apTreeNode[pCsr->iNode-1], 
            pDb->treehdr.root.iTransId, pCsr->aiCell[pCsr->iNode-1]
        );
      }else{
        iPtr = pDb->treehdr.root.iRoot;
      }
      rc = intArrayAppend(pDb->pEnv, &pDb->rollback, iPtr);

      if( rc==LSM_OK ){
        p->iV2 = pDb->treehdr.root.iTransId;
        p->iV2Child = (u8)iChildPtr;
        p->iV2Ptr = iNew;
      }
    }
  }

  return rc;
}

/*
** Cursor pCsr points at a node that is part of pTree. This function
** inserts a new key and optionally child node pointer into that node.
**
** The position into which the new key and pointer are inserted is
** determined by the iSlot parameter. The new key will be inserted to
** the left of the key currently stored in apKey[iSlot]. Or, if iSlot is
** greater than the index of the rightmost key in the node.
**
** Pointer pLeftPtr points to a child tree that contains keys that are
** smaller than pTreeKey.
*/
static int treeInsert(
  lsm_db *pDb,                    /* Database handle */
  TreeCursor *pCsr,               /* Cursor indicating path to insert at */
  u32 iLeftPtr,                   /* Left child pointer */
  u32 iTreeKey,                   /* Location of key to insert */
  u32 iRightPtr,                  /* Right child pointer */
  int iSlot                       /* Position to insert key into */
){
  int rc = LSM_OK;
  TreeNode *pNode = pCsr->apTreeNode[pCsr->iNode];

  /* Check if the node is currently full. If so, split pNode in two and
  ** call this function recursively to add a key to the parent. Otherwise, 
  ** insert the new key directly into pNode.  */
  assert( pNode->aiKeyPtr[1] );
  if( pNode->aiKeyPtr[0] && pNode->aiKeyPtr[2] ){
    u32 iLeft; TreeNode *pLeft;   /* New left-hand sibling node */
    u32 iRight; TreeNode *pRight; /* New right-hand sibling node */

    pLeft = newTreeNode(pDb, &iLeft, &rc);
    pRight = newTreeNode(pDb, &iRight, &rc);
    if( rc ) return rc;

    pLeft->aiChildPtr[1] = getChildPtr(pNode, WORKING_VERSION, 0);
    pLeft->aiKeyPtr[1] = pNode->aiKeyPtr[0];
    pLeft->aiChildPtr[2] = getChildPtr(pNode, WORKING_VERSION, 1);

    pRight->aiChildPtr[1] = getChildPtr(pNode, WORKING_VERSION, 2);
    pRight->aiKeyPtr[1] = pNode->aiKeyPtr[2];
    pRight->aiChildPtr[2] = getChildPtr(pNode, WORKING_VERSION, 3);

    if( pCsr->iNode==0 ){
      /* pNode is the root of the tree. Grow the tree by one level. */
      u32 iRoot; TreeNode *pRoot; /* New root node */

      pRoot = newTreeNode(pDb, &iRoot, &rc);
      pRoot->aiKeyPtr[1] = pNode->aiKeyPtr[1];
      pRoot->aiChildPtr[1] = iLeft;
      pRoot->aiChildPtr[2] = iRight;

      pDb->treehdr.root.iRoot = iRoot;
      pDb->treehdr.root.nHeight++;
    }else{

      pCsr->iNode--;
      rc = treeInsert(pDb, pCsr, 
          iLeft, pNode->aiKeyPtr[1], iRight, pCsr->aiCell[pCsr->iNode]
      );
    }

    assert( pLeft->iV2==0 );
    assert( pRight->iV2==0 );
    switch( iSlot ){
      case 0:
        pLeft->aiKeyPtr[0] = iTreeKey;
        pLeft->aiChildPtr[0] = iLeftPtr;
        if( iRightPtr ) pLeft->aiChildPtr[1] = iRightPtr;
        break;
      case 1:
        pLeft->aiChildPtr[3] = (iRightPtr ? iRightPtr : pLeft->aiChildPtr[2]);
        pLeft->aiKeyPtr[2] = iTreeKey;
        pLeft->aiChildPtr[2] = iLeftPtr;
        break;
      case 2:
        pRight->aiKeyPtr[0] = iTreeKey;
        pRight->aiChildPtr[0] = iLeftPtr;
        if( iRightPtr ) pRight->aiChildPtr[1] = iRightPtr;
        break;
      case 3:
        pRight->aiChildPtr[3] = (iRightPtr ? iRightPtr : pRight->aiChildPtr[2]);
        pRight->aiKeyPtr[2] = iTreeKey;
        pRight->aiChildPtr[2] = iLeftPtr;
        break;
    }

  }else{
    TreeNode *pNew;
    u32 *piKey;
    u32 *piChild;
    u32 iStore = 0;
    u32 iNew = 0;
    int i;

    /* Allocate a new version of node pNode. */
    pNew = newTreeNode(pDb, &iNew, &rc);
    if( rc ) return rc;

    piKey = pNew->aiKeyPtr;
    piChild = pNew->aiChildPtr;

    for(i=0; i<iSlot; i++){
      if( pNode->aiKeyPtr[i] ){
        *(piKey++) = pNode->aiKeyPtr[i];
        *(piChild++) = getChildPtr(pNode, WORKING_VERSION, i);
      }
    }

    *piKey++ = iTreeKey;
    *piChild++ = iLeftPtr;

    iStore = iRightPtr;
    for(i=iSlot; i<3; i++){
      if( pNode->aiKeyPtr[i] ){
        *(piKey++) = pNode->aiKeyPtr[i];
        *(piChild++) = iStore ? iStore : getChildPtr(pNode, WORKING_VERSION, i);
        iStore = 0;
      }
    }

    if( iStore ){
      *piChild = iStore;
    }else{
      *piChild = getChildPtr(pNode, WORKING_VERSION, 
          (pNode->aiKeyPtr[2] ? 3 : 2)
      );
    }
    pCsr->iNode--;
    rc = treeUpdatePtr(pDb, pCsr, iNew);
  }

  return rc;
}

static int treeInsertLeaf(
  lsm_db *pDb,                    /* Database handle */
  TreeCursor *pCsr,               /* Cursor structure */
  u32 iTreeKey,                   /* Key pointer to insert */
  int iSlot                       /* Insert key to the left of this */
){
  int rc = LSM_OK;                /* Return code */
  TreeNode *pLeaf = pCsr->apTreeNode[pCsr->iNode];
  TreeLeaf *pNew;
  u32 iNew;

  assert( iSlot>=0 && iSlot<=4 );
  assert( pCsr->iNode>0 );
  assert( pLeaf->aiKeyPtr[1] );

  pCsr->iNode--;

  pNew = newTreeLeaf(pDb, &iNew, &rc);
  if( pNew ){
    if( pLeaf->aiKeyPtr[0] && pLeaf->aiKeyPtr[2] ){
      /* The leaf is full. Split it in two. */
      TreeLeaf *pRight;
      u32 iRight;
      pRight = newTreeLeaf(pDb, &iRight, &rc);
      if( pRight ){
        assert( rc==LSM_OK );
        pNew->aiKeyPtr[1] = pLeaf->aiKeyPtr[0];
        pRight->aiKeyPtr[1] = pLeaf->aiKeyPtr[2];
        switch( iSlot ){
          case 0: pNew->aiKeyPtr[0] = iTreeKey; break;
          case 1: pNew->aiKeyPtr[2] = iTreeKey; break;
          case 2: pRight->aiKeyPtr[0] = iTreeKey; break;
          case 3: pRight->aiKeyPtr[2] = iTreeKey; break;
        }

        rc = treeInsert(pDb, pCsr, iNew, pLeaf->aiKeyPtr[1], iRight, 
            pCsr->aiCell[pCsr->iNode]
        );
      }
    }else{
      int iOut = 0;
      int i;
      for(i=0; i<4; i++){
        if( i==iSlot ) pNew->aiKeyPtr[iOut++] = iTreeKey;
        if( i<3 && pLeaf->aiKeyPtr[i] ){
          pNew->aiKeyPtr[iOut++] = pLeaf->aiKeyPtr[i];
        }
      }
      rc = treeUpdatePtr(pDb, pCsr, iNew);
    }
  }

  return rc;
}

static void lsmTreeMakeOld(lsm_db *pDb){

  /* A write transaction must be open. Otherwise the code below that
  ** assumes (pDb->pClient->iLogOff) is current may malfunction. 
  **
  ** Update: currently this assert fails due to lsm_flush(), which does
  ** not set nTransOpen.
  */
  assert( /* pDb->nTransOpen>0 && */ pDb->iReader>=0 );

  if( pDb->treehdr.iOldShmid==0 ){
    pDb->treehdr.iOldLog = (pDb->treehdr.log.aRegion[2].iEnd << 1);
    pDb->treehdr.iOldLog |= (~(pDb->pClient->iLogOff) & (i64)0x0001);

    pDb->treehdr.oldcksum0 = pDb->treehdr.log.cksum0;
    pDb->treehdr.oldcksum1 = pDb->treehdr.log.cksum1;
    pDb->treehdr.iOldShmid = pDb->treehdr.iNextShmid-1;
    memcpy(&pDb->treehdr.oldroot, &pDb->treehdr.root, sizeof(TreeRoot));

    pDb->treehdr.root.iTransId = 1;
    pDb->treehdr.root.iRoot = 0;
    pDb->treehdr.root.nHeight = 0;
    pDb->treehdr.root.nByte = 0;
  }
}

static void lsmTreeDiscardOld(lsm_db *pDb){
  assert( lsmShmAssertLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_EXCL) 
       || lsmShmAssertLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_EXCL) 
  );
  pDb->treehdr.iUsedShmid = pDb->treehdr.iOldShmid;
  pDb->treehdr.iOldShmid = 0;
}

static int lsmTreeHasOld(lsm_db *pDb){
  return pDb->treehdr.iOldShmid!=0;
}

/*
** This function is called during recovery to initialize the 
** tree header. Only the database connections private copy of the tree-header
** is initialized here - it will be copied into shared memory if log file
** recovery is successful.
*/
static int lsmTreeInit(lsm_db *pDb){
  ShmChunk *pOne;
  int rc = LSM_OK;

  memset(&pDb->treehdr, 0, sizeof(TreeHeader));
  pDb->treehdr.root.iTransId = 1;
  pDb->treehdr.iFirst = 1;
  pDb->treehdr.nChunk = 2;
  pDb->treehdr.iWrite = LSM_SHM_CHUNK_SIZE + LSM_SHM_CHUNK_HDR;
  pDb->treehdr.iNextShmid = 2;
  pDb->treehdr.iUsedShmid = 1;

  pOne = treeShmChunkRc(pDb, 1, &rc);
  if( pOne ){
    pOne->iNext = 0;
    pOne->iShmid = 1;
  }
  return rc;
}

static void treeHeaderChecksum(
  TreeHeader *pHdr, 
  u32 *aCksum
){
  u32 cksum1 = 0x12345678;
  u32 cksum2 = 0x9ABCDEF0;
  u32 *a = (u32 *)pHdr;
  int i;

  assert( (offsetof(TreeHeader, aCksum) + sizeof(u32)*2)==sizeof(TreeHeader) );
  assert( (sizeof(TreeHeader) % (sizeof(u32)*2))==0 );

  for(i=0; i<(offsetof(TreeHeader, aCksum) / sizeof(u32)); i+=2){
    cksum1 += a[i];
    cksum2 += (cksum1 + a[i+1]);
  }
  aCksum[0] = cksum1;
  aCksum[1] = cksum2;
}

/*
** Return true if the checksum stored in TreeHeader object *pHdr is 
** consistent with the contents of its other fields.
*/
static int treeHeaderChecksumOk(TreeHeader *pHdr){
  u32 aCksum[2];
  treeHeaderChecksum(pHdr, aCksum);
  return (0==memcmp(aCksum, pHdr->aCksum, sizeof(aCksum)));
}

/*
** This type is used by functions lsmTreeRepair() and treeSortByShmid() to
** make relinking the linked list of shared-memory chunks easier.
*/
typedef struct ShmChunkLoc ShmChunkLoc;
struct ShmChunkLoc {
  ShmChunk *pShm;
  u32 iLoc;
};

/*
** This function checks that the linked list of shared memory chunks 
** that starts at chunk db->treehdr.iFirst:
**
**   1) Includes all chunks in the shared-memory region, and
**   2) Links them together in order of ascending shm-id.
**
** If no error occurs and the conditions above are met, LSM_OK is returned.
**
** If either of the conditions are untrue, LSM_CORRUPT is returned. Or, if
** an error is encountered before the checks are completed, another LSM error
** code (i.e. LSM_IOERR or LSM_NOMEM) may be returned.
*/
static int treeCheckLinkedList(lsm_db *db){
  int rc = LSM_OK;
  int nVisit = 0;
  ShmChunk *p;

  p = treeShmChunkRc(db, db->treehdr.iFirst, &rc);
  while( rc==LSM_OK && p ){
    if( p->iNext ){
      if( p->iNext>=db->treehdr.nChunk ){
        rc = LSM_CORRUPT_BKPT;
      }else{
        ShmChunk *pNext = treeShmChunkRc(db, p->iNext, &rc);
        if( rc==LSM_OK ){
          if( pNext->iShmid!=p->iShmid+1 ){
            rc = LSM_CORRUPT_BKPT;
          }
          p = pNext;
        }
      }
    }else{
      p = 0;
    }
    nVisit++;
  }

  if( rc==LSM_OK && (u32)nVisit!=db->treehdr.nChunk-1 ){
    rc = LSM_CORRUPT_BKPT;
  }
  return rc;
}

/*
** Iterate through the current in-memory tree. If there are any v2-pointers
** with transaction ids larger than db->treehdr.iTransId, zero them.
*/
static int treeRepairPtrs(lsm_db *db){
  int rc = LSM_OK;

  if( db->treehdr.root.nHeight>1 ){
    TreeCursor csr;               /* Cursor used to iterate through tree */
    u32 iTransId = db->treehdr.root.iTransId;

    /* Initialize the cursor structure. Also decrement the nHeight variable
    ** in the tree-header. This will prevent the cursor from visiting any
    ** leaf nodes.  */
    db->treehdr.root.nHeight--;
    treeCursorInit(db, 0, &csr);

    rc = lsmTreeCursorEnd(&csr, 0);
    while( rc==LSM_OK && lsmTreeCursorValid(&csr) ){
      TreeNode *pNode = csr.apTreeNode[csr.iNode];
      if( pNode->iV2>iTransId ){
        pNode->iV2Child = 0;
        pNode->iV2Ptr = 0;
        pNode->iV2 = 0;
      }
      rc = lsmTreeCursorNext(&csr);
    }
    tblobFree(csr.pDb, &csr.blob);

    db->treehdr.root.nHeight++;
  }

  return rc;
}

static int treeRepairList(lsm_db *db){
  int rc = LSM_OK;
  int i;
  ShmChunk *p;
  ShmChunk *pMin = 0;
  u32 iMin = 0;

  /* Iterate through all shm chunks. Find the smallest shm-id present in
  ** the shared-memory region. */
  for(i=1; rc==LSM_OK && (u32)i<db->treehdr.nChunk; i++){
    p = treeShmChunkRc(db, i, &rc);
    if( p && (pMin==0 || shm_sequence_ge(pMin->iShmid, p->iShmid)) ){
      pMin = p;
      iMin = i;
    }
  }

  /* Fix the shm-id values on any chunks with a shm-id greater than or 
  ** equal to treehdr.iNextShmid. Then do a merge-sort of all chunks to 
  ** fix the ShmChunk.iNext pointers.
  */
  if( rc==LSM_OK ){
    int nSort;
    int nByte;
    u32 iPrevShmid;
    ShmChunkLoc *aSort;

    /* Allocate space for a merge sort. */
    nSort = 1;
    while( (u32)nSort < (db->treehdr.nChunk-1) ) nSort = nSort * 2;
    nByte = sizeof(ShmChunkLoc) * nSort * 2;
    aSort = lsmMallocZeroRc(db->pEnv, nByte, &rc);
    iPrevShmid = pMin->iShmid;

    /* Fix all shm-ids, if required. */
    if( rc==LSM_OK ){
      iPrevShmid = pMin->iShmid-1;
      for(i=1; (u32)i<db->treehdr.nChunk; i++){
        p = treeShmChunk(db, i);
        aSort[i-1].pShm = p;
        aSort[i-1].iLoc = i;
        if( (u32)i!=db->treehdr.iFirst ){
          if( shm_sequence_ge(p->iShmid, db->treehdr.iNextShmid) ){
            p->iShmid = iPrevShmid--;
          }
        }
      }
      if( iMin!=db->treehdr.iFirst ){
        p = treeShmChunk(db, db->treehdr.iFirst);
        p->iShmid = iPrevShmid;
      }
    }

    if( rc==LSM_OK ){
      ShmChunkLoc *aSpace = &aSort[nSort];
      for(i=0; i<nSort; i++){
        if( aSort[i].pShm ){
          assert( shm_sequence_ge(aSort[i].pShm->iShmid, iPrevShmid) );
          assert( aSpace[aSort[i].pShm->iShmid - iPrevShmid].pShm==0 );
          aSpace[aSort[i].pShm->iShmid - iPrevShmid] = aSort[i];
        }
      }

      if( aSpace[nSort-1].pShm ) aSpace[nSort-1].pShm->iNext = 0;
      for(i=0; i<nSort-1; i++){
        if( aSpace[i].pShm ){
          aSpace[i].pShm->iNext = aSpace[i+1].iLoc;
        }
      }

      rc = treeCheckLinkedList(db);
      lsmFree(db->pEnv, aSort);
    }
  }

  return rc;
}

/*
** This function is called as part of opening a write-transaction if the
** writer-flag is already set - indicating that the previous writer 
** failed before ending its transaction.
*/
static int lsmTreeRepair(lsm_db *db){
  int rc = LSM_OK;
  TreeHeader hdr;
  ShmHeader *pHdr = db->pShmhdr;

  /* Ensure that the two tree-headers are consistent. Copy one over the other
  ** if necessary. Prefer the data from a tree-header for which the checksum
  ** computes. Or, if they both compute, prefer tree-header-1.  */
  if( memcmp(&pHdr->hdr1, &pHdr->hdr2, sizeof(TreeHeader)) ){
    if( treeHeaderChecksumOk(&pHdr->hdr1) ){
      memcpy(&pHdr->hdr2, &pHdr->hdr1, sizeof(TreeHeader));
    }else{
      memcpy(&pHdr->hdr1, &pHdr->hdr2, sizeof(TreeHeader));
    }
  }

  /* Save the connections current copy of the tree-header. It will be 
  ** restored before returning.  */
  memcpy(&hdr, &db->treehdr, sizeof(TreeHeader));

  /* Walk the tree. Zero any v2 pointers with a transaction-id greater than
  ** the transaction-id currently in the tree-headers.  */
  rc = treeRepairPtrs(db);

  /* Repair the linked list of shared-memory chunks. */
  if( rc==LSM_OK ){
    rc = treeRepairList(db);
  }

  memcpy(&db->treehdr, &hdr, sizeof(TreeHeader));
  return rc;
}

static void treeOverwriteKey(lsm_db *db, TreeCursor *pCsr, u32 iKey, int *pRc){
  if( *pRc==LSM_OK ){
    TreeRoot *p = &db->treehdr.root;
    TreeNode *pNew;
    u32 iNew;
    TreeNode *pNode = pCsr->apTreeNode[pCsr->iNode];
    int iCell = pCsr->aiCell[pCsr->iNode];

    /* Create a copy of this node */
    if( (pCsr->iNode>0 && (u32)pCsr->iNode==(p->nHeight-1)) ){
      pNew = copyTreeLeaf(db, (TreeLeaf *)pNode, &iNew, pRc);
    }else{
      pNew = copyTreeNode(db, pNode, &iNew, pRc);
    }

    if( pNew ){
      /* Modify the value in the new version */
      pNew->aiKeyPtr[iCell] = iKey;

      /* Change the pointer in the parent (if any) to point at the new 
       ** TreeNode */
      pCsr->iNode--;
      treeUpdatePtr(db, pCsr, iNew);
    }
  }
}

static int treeNextIsEndDelete(lsm_db *db, TreeCursor *pCsr){
  int iNode = pCsr->iNode;
  int iCell = pCsr->aiCell[iNode]+1;

  /* Cursor currently points to a leaf node. */
  assert( (u32)pCsr->iNode==(db->treehdr.root.nHeight-1) );

  while( iNode>=0 ){
    TreeNode *pNode = pCsr->apTreeNode[iNode];
    if( iCell<3 && pNode->aiKeyPtr[iCell] ){
      int rc = LSM_OK;
      TreeKey *pKey = treeShmptr(db, pNode->aiKeyPtr[iCell]);
      assert( rc==LSM_OK );
      return ((pKey->flags & LSM_END_DELETE) ? 1 : 0);
    }
    iNode--;
    iCell = pCsr->aiCell[iNode];
  }

  return 0;
}

static int treePrevIsStartDelete(lsm_db *db, TreeCursor *pCsr){
  int iNode = pCsr->iNode;

  /* Cursor currently points to a leaf node. */
  assert( (u32)pCsr->iNode==(db->treehdr.root.nHeight-1) );

  while( iNode>=0 ){
    TreeNode *pNode = pCsr->apTreeNode[iNode];
    int iCell = pCsr->aiCell[iNode]-1;
    if( iCell>=0 && pNode->aiKeyPtr[iCell] ){
      int rc = LSM_OK;
      TreeKey *pKey = treeShmptr(db, pNode->aiKeyPtr[iCell]);
      assert( rc==LSM_OK );
      return ((pKey->flags & LSM_START_DELETE) ? 1 : 0);
    }
    iNode--;
  }

  return 0;
}


static int treeInsertEntry(
  lsm_db *pDb,                    /* Database handle */
  int flags,                      /* Flags associated with entry */
  void *pKey,                     /* Pointer to key data */
  int nKey,                       /* Size of key data in bytes */
  void *pVal,                     /* Pointer to value data (or NULL) */
  int nVal                        /* Bytes in value data (or -ve for delete) */
){
  int rc = LSM_OK;                /* Return Code */
  TreeKey *pTreeKey;              /* New key-value being inserted */
  u32 iTreeKey;
  TreeRoot *p = &pDb->treehdr.root;
  TreeCursor csr;                 /* Cursor to seek to pKey/nKey */
  int res = 0;                    /* Result of seek operation on csr */

  assert( nVal>=0 || pVal==0 );
  assert_tree_looks_ok(LSM_OK, pTree);
  assert( flags==LSM_INSERT       || flags==LSM_POINT_DELETE 
       || flags==LSM_START_DELETE || flags==LSM_END_DELETE 
  );
  assert( (flags & LSM_CONTIGUOUS)==0 );
#if 0
  dump_tree_contents(pDb, "before");
#endif

  if( p->iRoot ){
    TreeKey *pRes;                /* Key at end of seek operation */
    treeCursorInit(pDb, 0, &csr);

    /* Seek to the leaf (or internal node) that the new key belongs on */
    rc = lsmTreeCursorSeek(&csr, pKey, nKey, &res);
    pRes = csrGetKey(&csr, &csr.blob, &rc);
    if( rc!=LSM_OK ) return rc;
    assert( pRes );

    if( flags==LSM_START_DELETE ){
      /* When inserting a start-delete-range entry, if the key that
      ** occurs immediately before the new entry is already a START_DELETE,
      ** then the new entry is not required.  */
      if( (res<=0 && (pRes->flags & LSM_START_DELETE))
       || (res>0  && treePrevIsStartDelete(pDb, &csr))
      ){ 
        goto insert_entry_out;
      }
    }else if( flags==LSM_END_DELETE ){
      /* When inserting an start-delete-range entry, if the key that
      ** occurs immediately after the new entry is already an END_DELETE,
      ** then the new entry is not required.  */
      if( (res<0  && treeNextIsEndDelete(pDb, &csr))
       || (res>=0 && (pRes->flags & LSM_END_DELETE))
      ){
        goto insert_entry_out;
      }
    }

    if( res==0 && (flags & (LSM_END_DELETE|LSM_START_DELETE)) ){
      if( pRes->flags & LSM_INSERT ){
        nVal = pRes->nValue;
        pVal = TKV_VAL(pRes);
      }
      flags = flags | pRes->flags;
    }

    if( flags & (LSM_INSERT|LSM_POINT_DELETE) ){
      if( (res<0 && (pRes->flags & LSM_START_DELETE))
       || (res>0 && (pRes->flags & LSM_END_DELETE)) 
      ){
        flags = flags | (LSM_END_DELETE|LSM_START_DELETE);
      }else if( res==0 ){
        flags = flags | (pRes->flags & (LSM_END_DELETE|LSM_START_DELETE));
      }
    }
  }else{
    memset(&csr, 0, sizeof(TreeCursor));
  }

  /* Allocate and populate a new key-value pair structure */
  pTreeKey = newTreeKey(pDb, &iTreeKey, pKey, nKey, pVal, nVal, &rc);
  if( rc!=LSM_OK ) return rc;
  assert( pTreeKey->flags==0 || pTreeKey->flags==LSM_CONTIGUOUS );
  pTreeKey->flags |= flags;

  if( p->iRoot==0 ){
    /* The tree is completely empty. Add a new root node and install
    ** (pKey/nKey) as the middle entry. Even though it is a leaf at the
    ** moment, use newTreeNode() to allocate the node (i.e. allocate enough
    ** space for the fields used by interior nodes). This is because the
    ** treeInsert() routine may convert this node to an interior node. */
    TreeNode *pRoot = newTreeNode(pDb, &p->iRoot, &rc);
    if( rc==LSM_OK ){
      assert( p->nHeight==0 );
      pRoot->aiKeyPtr[1] = iTreeKey;
      p->nHeight = 1;
    }
  }else{
    if( res==0 ){
      /* The search found a match within the tree. */
      treeOverwriteKey(pDb, &csr, iTreeKey, &rc);
    }else{
      /* The cursor now points to the leaf node into which the new entry should
      ** be inserted. There may or may not be a free slot within the leaf for
      ** the new key-value pair. 
      **
      ** iSlot is set to the index of the key within pLeaf that the new key
      ** should be inserted to the left of (or to a value 1 greater than the
      ** index of the rightmost key if the new key is larger than all keys
      ** currently stored in the node).
      */
      int iSlot = csr.aiCell[csr.iNode] + (res<0);
      if( csr.iNode==0 ){
        rc = treeInsert(pDb, &csr, 0, iTreeKey, 0, iSlot);
      }else{
        rc = treeInsertLeaf(pDb, &csr, iTreeKey, iSlot);
      }
    }
  }

#if 0
  dump_tree_contents(pDb, "after");
#endif
 insert_entry_out:
  tblobFree(pDb, &csr.blob);
  assert_tree_looks_ok(rc, pTree);
  return rc;
}

/*
** Insert a new entry into the in-memory tree.
**
** If the value of the 5th parameter, nVal, is negative, then a delete-marker
** is inserted into the tree. In this case the value pointer, pVal, must be
** NULL.
*/
static int lsmTreeInsert(
  lsm_db *pDb,                    /* Database handle */
  void *pKey,                     /* Pointer to key data */
  int nKey,                       /* Size of key data in bytes */
  void *pVal,                     /* Pointer to value data (or NULL) */
  int nVal                        /* Bytes in value data (or -ve for delete) */
){
  int flags;
  if( nVal<0 ){
    flags = LSM_POINT_DELETE;
  }else{
    flags = LSM_INSERT;
  }

  return treeInsertEntry(pDb, flags, pKey, nKey, pVal, nVal);
}

static int treeDeleteEntry(lsm_db *db, TreeCursor *pCsr, u32 iNewptr){
  TreeRoot *p = &db->treehdr.root;
  TreeNode *pNode = pCsr->apTreeNode[pCsr->iNode];
  int iSlot = pCsr->aiCell[pCsr->iNode];
  int bLeaf;
  int rc = LSM_OK;

  assert( pNode->aiKeyPtr[1] );
  assert( pNode->aiKeyPtr[iSlot] );
  assert( iSlot==0 || iSlot==1 || iSlot==2 );
  assert( ((u32)pCsr->iNode==(db->treehdr.root.nHeight-1))==(iNewptr==0) );

  bLeaf = ((u32)pCsr->iNode==(p->nHeight-1) && p->nHeight>1);
  
  if( pNode->aiKeyPtr[0] || pNode->aiKeyPtr[2] ){
    /* There are currently at least 2 keys on this node. So just create
    ** a new copy of the node with one of the keys removed. If the node
    ** happens to be the root node of the tree, allocate an entire 
    ** TreeNode structure instead of just a TreeLeaf.  */
    TreeNode *pNew;
    u32 iNew;

    if( bLeaf ){
      pNew = (TreeNode *)newTreeLeaf(db, &iNew, &rc);
    }else{
      pNew = newTreeNode(db, &iNew, &rc);
    }
    if( pNew ){
      int i;
      int iOut = 1;
      for(i=0; i<4; i++){
        if( i==iSlot ){
          i++;
          if( bLeaf==0 ) pNew->aiChildPtr[iOut] = iNewptr;
          if( i<3 ) pNew->aiKeyPtr[iOut] = pNode->aiKeyPtr[i];
          iOut++;
        }else if( bLeaf || p->nHeight==1 ){
          if( i<3 && pNode->aiKeyPtr[i] ){
            pNew->aiKeyPtr[iOut++] = pNode->aiKeyPtr[i];
          }
        }else{
          if( getChildPtr(pNode, WORKING_VERSION, i) ){
            pNew->aiChildPtr[iOut] = getChildPtr(pNode, WORKING_VERSION, i);
            if( i<3 ) pNew->aiKeyPtr[iOut] = pNode->aiKeyPtr[i];
            iOut++;
          }
        }
      }
      assert( iOut<=4 );
      assert( bLeaf || pNew->aiChildPtr[0]==0 );
      pCsr->iNode--;
      rc = treeUpdatePtr(db, pCsr, iNew);
    }

  }else if( pCsr->iNode==0 ){
    /* Removing the only key in the root node. iNewptr is the new root. */
    assert( iSlot==1 );
    db->treehdr.root.iRoot = iNewptr;
    db->treehdr.root.nHeight--;

  }else{
    /* There is only one key on this node and the node is not the root
    ** node. Find a peer for this node. Then redistribute the contents of
    ** the peer and the parent cell between the parent and either one or
    ** two new nodes.  */
    TreeNode *pParent;            /* Parent tree node */
    int iPSlot;
    u32 iPeer;                    /* Pointer to peer leaf node */
    int iDir;
    TreeNode *pPeer;              /* The peer leaf node */
    TreeNode *pNew1; u32 iNew1;   /* First new leaf node */

    assert( iSlot==1 );

    pParent = pCsr->apTreeNode[pCsr->iNode-1];
    iPSlot = pCsr->aiCell[pCsr->iNode-1];

    if( iPSlot>0 && getChildPtr(pParent, WORKING_VERSION, iPSlot-1) ){
      iDir = -1;
    }else{
      iDir = +1;
    }
    iPeer = getChildPtr(pParent, WORKING_VERSION, iPSlot+iDir);
    pPeer = (TreeNode *)treeShmptr(db, iPeer);
    assertIsWorkingChild(db, pNode, pParent, iPSlot);

    /* Allocate the first new leaf node. This is always required. */
    if( bLeaf ){
      pNew1 = (TreeNode *)newTreeLeaf(db, &iNew1, &rc);
    }else{
      pNew1 = (TreeNode *)newTreeNode(db, &iNew1, &rc);
    }

    if( pPeer->aiKeyPtr[0] && pPeer->aiKeyPtr[2] ){
      /* Peer node is completely full. This means that two new leaf nodes
      ** and a new parent node are required. */

      TreeNode *pNew2; u32 iNew2; /* Second new leaf node */
      TreeNode *pNewP; u32 iNewP; /* New parent node */

      if( bLeaf ){
        pNew2 = (TreeNode *)newTreeLeaf(db, &iNew2, &rc);
      }else{
        pNew2 = (TreeNode *)newTreeNode(db, &iNew2, &rc);
      }
      pNewP = copyTreeNode(db, pParent, &iNewP, &rc);

      if( iDir==-1 ){
        pNew1->aiKeyPtr[1] = pPeer->aiKeyPtr[0];
        if( bLeaf==0 ){
          pNew1->aiChildPtr[1] = getChildPtr(pPeer, WORKING_VERSION, 0);
          pNew1->aiChildPtr[2] = getChildPtr(pPeer, WORKING_VERSION, 1);
        }

        pNewP->aiChildPtr[iPSlot-1] = iNew1;
        pNewP->aiKeyPtr[iPSlot-1] = pPeer->aiKeyPtr[1];
        pNewP->aiChildPtr[iPSlot] = iNew2;

        pNew2->aiKeyPtr[0] = pPeer->aiKeyPtr[2];
        pNew2->aiKeyPtr[1] = pParent->aiKeyPtr[iPSlot-1];
        if( bLeaf==0 ){
          pNew2->aiChildPtr[0] = getChildPtr(pPeer, WORKING_VERSION, 2);
          pNew2->aiChildPtr[1] = getChildPtr(pPeer, WORKING_VERSION, 3);
          pNew2->aiChildPtr[2] = iNewptr;
        }
      }else{
        pNew1->aiKeyPtr[1] = pParent->aiKeyPtr[iPSlot];
        if( bLeaf==0 ){
          pNew1->aiChildPtr[1] = iNewptr;
          pNew1->aiChildPtr[2] = getChildPtr(pPeer, WORKING_VERSION, 0);
        }

        pNewP->aiChildPtr[iPSlot] = iNew1;
        pNewP->aiKeyPtr[iPSlot] = pPeer->aiKeyPtr[0];
        pNewP->aiChildPtr[iPSlot+1] = iNew2;

        pNew2->aiKeyPtr[0] = pPeer->aiKeyPtr[1];
        pNew2->aiKeyPtr[1] = pPeer->aiKeyPtr[2];
        if( bLeaf==0 ){
          pNew2->aiChildPtr[0] = getChildPtr(pPeer, WORKING_VERSION, 1);
          pNew2->aiChildPtr[1] = getChildPtr(pPeer, WORKING_VERSION, 2);
          pNew2->aiChildPtr[2] = getChildPtr(pPeer, WORKING_VERSION, 3);
        }
      }
      assert( pCsr->iNode>=1 );
      pCsr->iNode -= 2;
      if( rc==LSM_OK ){
        assert( pNew1->aiKeyPtr[1] && pNew2->aiKeyPtr[1] );
        rc = treeUpdatePtr(db, pCsr, iNewP);
      }
    }else{
      int iKOut = 0;
      int iPOut = 0;
      int i;

      pCsr->iNode--;

      if( iDir==1 ){
        pNew1->aiKeyPtr[iKOut++] = pParent->aiKeyPtr[iPSlot];
        if( bLeaf==0 ) pNew1->aiChildPtr[iPOut++] = iNewptr;
      }
      for(i=0; i<3; i++){
        if( pPeer->aiKeyPtr[i] ){
          pNew1->aiKeyPtr[iKOut++] = pPeer->aiKeyPtr[i];
        }
      }
      if( bLeaf==0 ){
        for(i=0; i<4; i++){
          if( getChildPtr(pPeer, WORKING_VERSION, i) ){
            pNew1->aiChildPtr[iPOut++] = getChildPtr(pPeer, WORKING_VERSION, i);
          }
        }
      }
      if( iDir==-1 ){
        iPSlot--;
        pNew1->aiKeyPtr[iKOut++] = pParent->aiKeyPtr[iPSlot];
        if( bLeaf==0 ) pNew1->aiChildPtr[iPOut++] = iNewptr;
        pCsr->aiCell[pCsr->iNode] = (u8)iPSlot;
      }

      rc = treeDeleteEntry(db, pCsr, iNew1);
    }
  }

  return rc;
}

/*
** Delete a range of keys from the tree structure (i.e. the lsm_delete_range()
** function, not lsm_delete()).
**
** This is a two step process: 
**
**     1) Remove all entries currently stored in the tree that have keys
**        that fall into the deleted range.
**
**        TODO: There are surely good ways to optimize this step - removing 
**        a range of keys from a b-tree. But for now, this function removes
**        them one at a time using the usual approach.
**
**     2) Unless the largest key smaller than or equal to (pKey1/nKey1) is
**        already marked as START_DELETE, insert a START_DELETE key. 
**        Similarly, unless the smallest key greater than or equal to
**        (pKey2/nKey2) is already START_END, insert a START_END key.
*/
static int lsmTreeDelete(
  lsm_db *db,
  void *pKey1, int nKey1,         /* Start of range */
  void *pKey2, int nKey2          /* End of range */
){
  int rc = LSM_OK;
  int bDone = 0;
  TreeRoot *p = &db->treehdr.root;
  TreeBlob blob = {0, 0};

  /* The range must be sensible - that (key1 < key2). */
  assert( treeKeycmp(pKey1, nKey1, pKey2, nKey2)<0 );
  assert( assert_delete_ranges_match(db) );

#if 0
  static int nCall = 0;
  printf("\n");
  nCall++;
  printf("%d delete %s .. %s\n", nCall, (char *)pKey1, (char *)pKey2);
  dump_tree_contents(db, "before delete");
#endif

  /* Step 1. This loop runs until the tree contains no keys within the
  ** range being deleted. Or until an error occurs. */
  while( bDone==0 && rc==LSM_OK ){
    int res;
    TreeCursor csr;               /* Cursor to seek to first key in range */
    void *pDel; int nDel;         /* Key to (possibly) delete this iteration */
#ifndef NDEBUG
    int nEntry = treeCountEntries(db);
#endif

    /* Seek the cursor to the first entry in the tree greater than pKey1. */
    treeCursorInit(db, 0, &csr);
    lsmTreeCursorSeek(&csr, pKey1, nKey1, &res);
    if( res<=0 && lsmTreeCursorValid(&csr) ) lsmTreeCursorNext(&csr);

    /* If there is no such entry, or if it is greater than pKey2, then the
    ** tree now contains no keys in the range being deleted. In this case
    ** break out of the loop.  */
    bDone = 1;
    if( lsmTreeCursorValid(&csr) ){
      lsmTreeCursorKey(&csr, 0, &pDel, &nDel);
      if( treeKeycmp(pDel, nDel, pKey2, nKey2)<0 ) bDone = 0;
    }

    if( bDone==0 ){
      if( (u32)csr.iNode==(p->nHeight-1) ){
        /* The element to delete already lies on a leaf node */
        rc = treeDeleteEntry(db, &csr, 0);
      }else{
        /* 1. Overwrite the current key with a copy of the next key in the 
        **    tree (key N).
        **
        ** 2. Seek to key N (cursor will stop at the internal node copy of
        **    N). Move to the next key (original copy of N). Delete
        **    this entry. 
        */
        u32 iKey;
        TreeKey *pKey;
        int iNode = csr.iNode;
        lsmTreeCursorNext(&csr);
        assert( (u32)csr.iNode==(p->nHeight-1) );

        iKey = csr.apTreeNode[csr.iNode]->aiKeyPtr[csr.aiCell[csr.iNode]];
        lsmTreeCursorPrev(&csr);

        treeOverwriteKey(db, &csr, iKey, &rc);
        pKey = treeShmkey(db, iKey, TKV_LOADKEY, &blob, &rc);
        if( pKey ){
          rc = lsmTreeCursorSeek(&csr, TKV_KEY(pKey), pKey->nKey, &res);
        }
        if( rc==LSM_OK ){
          assert( res==0 && csr.iNode==iNode );
          rc = lsmTreeCursorNext(&csr);
          if( rc==LSM_OK ){
            rc = treeDeleteEntry(db, &csr, 0);
          }
        }
      }
    }

    /* Clean up any memory allocated by the cursor. */
    tblobFree(db, &csr.blob);
#if 0
    dump_tree_contents(db, "ddd delete");
#endif
    assert( bDone || treeCountEntries(db)==(nEntry-1) );
  }

#if 0
  dump_tree_contents(db, "during delete");
#endif

  /* Now insert the START_DELETE and END_DELETE keys. */
  if( rc==LSM_OK ){
    rc = treeInsertEntry(db, LSM_START_DELETE, pKey1, nKey1, 0, -1);
  }
#if 0
  dump_tree_contents(db, "during delete 2");
#endif
  if( rc==LSM_OK ){
    rc = treeInsertEntry(db, LSM_END_DELETE, pKey2, nKey2, 0, -1);
  }

#if 0
  dump_tree_contents(db, "after delete");
#endif

  tblobFree(db, &blob);
  assert( assert_delete_ranges_match(db) );
  return rc;
}

/*
** Return, in bytes, the amount of memory currently used by the tree 
** structure.
*/
static int lsmTreeSize(lsm_db *pDb){
  return pDb->treehdr.root.nByte;
}

/*
** Open a cursor on the in-memory tree pTree.
*/
static int lsmTreeCursorNew(lsm_db *pDb, int bOld, TreeCursor **ppCsr){
  TreeCursor *pCsr;
  *ppCsr = pCsr = lsmMalloc(pDb->pEnv, sizeof(TreeCursor));
  if( pCsr ){
    treeCursorInit(pDb, bOld, pCsr);
    return LSM_OK;
  }
  return LSM_NOMEM_BKPT;
}

/*
** Close an in-memory tree cursor.
*/
static void lsmTreeCursorDestroy(TreeCursor *pCsr){
  if( pCsr ){
    tblobFree(pCsr->pDb, &pCsr->blob);
    lsmFree(pCsr->pDb->pEnv, pCsr);
  }
}

static void lsmTreeCursorReset(TreeCursor *pCsr){
  if( pCsr ){
    pCsr->iNode = -1;
    pCsr->pSave = 0;
  }
}

#ifndef NDEBUG
static int treeCsrCompare(TreeCursor *pCsr, void *pKey, int nKey, int *pRc){
  TreeKey *p;
  int cmp = 0;
  assert( pCsr->iNode>=0 );
  p = csrGetKey(pCsr, &pCsr->blob, pRc);
  if( p ){
    cmp = treeKeycmp(TKV_KEY(p), p->nKey, pKey, nKey);
  }
  return cmp;
}
#endif


/*
** Attempt to seek the cursor passed as the first argument to key (pKey/nKey)
** in the tree structure. If an exact match for the key is found, leave the
** cursor pointing to it and set *pRes to zero before returning. If an
** exact match cannot be found, do one of the following:
**
**   * Leave the cursor pointing to the smallest element in the tree that 
**     is larger than the key and set *pRes to +1, or
**
**   * Leave the cursor pointing to the largest element in the tree that 
**     is smaller than the key and set *pRes to -1, or
**
**   * If the tree is empty, leave the cursor at EOF and set *pRes to -1.
*/
static int lsmTreeCursorSeek(TreeCursor *pCsr, void *pKey, int nKey, int *pRes){
  int rc = LSM_OK;                /* Return code */
  lsm_db *pDb = pCsr->pDb;
  TreeRoot *pRoot = pCsr->pRoot;
  u32 iNodePtr;                   /* Location of current node in search */

  /* Discard any saved position data */
  treeCursorRestore(pCsr, 0);

  iNodePtr = pRoot->iRoot;
  if( iNodePtr==0 ){
    /* Either an error occurred or the tree is completely empty. */
    assert( rc!=LSM_OK || pRoot->iRoot==0 );
    *pRes = -1;
    pCsr->iNode = -1;
  }else{
    TreeBlob b = {0, 0};
    int res = 0;                  /* Result of comparison function */
    int iNode = -1;
    while( iNodePtr ){
      TreeNode *pNode;            /* Node at location iNodePtr */
      int iTest;                  /* Index of second key to test (0 or 2) */
      u32 iTreeKey;
      TreeKey *pTreeKey;          /* Key to compare against */

      pNode = (TreeNode *)treeShmptrUnsafe(pDb, iNodePtr);
      iNode++;
      pCsr->apTreeNode[iNode] = pNode;

      /* Compare (pKey/nKey) with the key in the middle slot of B-tree node
      ** pNode. The middle slot is never empty. If the comparison is a match,
      ** then the search is finished. Break out of the loop. */
      pTreeKey = (TreeKey*)treeShmptrUnsafe(pDb, pNode->aiKeyPtr[1]);
      if( !(pTreeKey->flags & LSM_CONTIGUOUS) ){
        pTreeKey = treeShmkey(pDb, pNode->aiKeyPtr[1], TKV_LOADKEY, &b, &rc);
        if( rc!=LSM_OK ) break;
      }
      res = treeKeycmp((void *)&pTreeKey[1], pTreeKey->nKey, pKey, nKey);
      if( res==0 ){
        pCsr->aiCell[iNode] = 1;
        break;
      }

      /* Based on the results of the previous comparison, compare (pKey/nKey)
      ** to either the left or right key of the B-tree node, if such a key
      ** exists. */
      iTest = (res>0 ? 0 : 2);
      iTreeKey = pNode->aiKeyPtr[iTest];
      if( iTreeKey ){
        pTreeKey = (TreeKey*)treeShmptrUnsafe(pDb, iTreeKey);
        if( !(pTreeKey->flags & LSM_CONTIGUOUS) ){
          pTreeKey = treeShmkey(pDb, iTreeKey, TKV_LOADKEY, &b, &rc);
          if( rc ) break;
        }
        res = treeKeycmp((void *)&pTreeKey[1], pTreeKey->nKey, pKey, nKey);
        if( res==0 ){
          pCsr->aiCell[iNode] = (u8)iTest;
          break;
        }
      }else{
        iTest = 1;
      }

      if( (u32)iNode<(pRoot->nHeight-1) ){
        iNodePtr = getChildPtr(pNode, pRoot->iTransId, iTest + (res<0));
      }else{
        iNodePtr = 0;
      }
      pCsr->aiCell[iNode] = (u8)(iTest + (iNodePtr && (res<0)));
    }

    *pRes = res;
    pCsr->iNode = iNode;
    tblobFree(pDb, &b);
  }

  /* assert() that *pRes has been set properly */
#ifndef NDEBUG
  if( rc==LSM_OK && lsmTreeCursorValid(pCsr) ){
    int cmp = treeCsrCompare(pCsr, pKey, nKey, &rc);
    assert( rc!=LSM_OK || *pRes==cmp || (*pRes ^ cmp)>0 );
  }
#endif

  return rc;
}

static int lsmTreeCursorNext(TreeCursor *pCsr){
#ifndef NDEBUG
  TreeKey *pK1;
  TreeBlob key1 = {0, 0};
#endif
  lsm_db *pDb = pCsr->pDb;
  TreeRoot *pRoot = pCsr->pRoot;
  const int iLeaf = pRoot->nHeight-1;
  int iCell; 
  int rc = LSM_OK; 
  TreeNode *pNode; 

  /* Restore the cursor position, if required */
  int iRestore = 0;
  treeCursorRestore(pCsr, &iRestore);
  if( iRestore>0 ) return LSM_OK;

  /* Save a pointer to the current key. This is used in an assert() at the
  ** end of this function - to check that the 'next' key really is larger
  ** than the current key. */
#ifndef NDEBUG
  pK1 = csrGetKey(pCsr, &key1, &rc);
  if( rc!=LSM_OK ) return rc;
#endif

  assert( lsmTreeCursorValid(pCsr) );
  assert( pCsr->aiCell[pCsr->iNode]<3 );

  pNode = pCsr->apTreeNode[pCsr->iNode];
  iCell = ++pCsr->aiCell[pCsr->iNode];

  /* If the current node is not a leaf, and the current cell has sub-tree
  ** associated with it, descend to the left-most key on the left-most
  ** leaf of the sub-tree.  */
  if( pCsr->iNode<iLeaf && getChildPtr(pNode, pRoot->iTransId, iCell) ){
    do {
      u32 iNodePtr;
      pCsr->iNode++;
      iNodePtr = getChildPtr(pNode, pRoot->iTransId, iCell);
      pNode = (TreeNode *)treeShmptr(pDb, iNodePtr);
      pCsr->apTreeNode[pCsr->iNode] = pNode;
      iCell = pCsr->aiCell[pCsr->iNode] = (pNode->aiKeyPtr[0]==0);
    }while( pCsr->iNode < iLeaf );
  }

  /* Otherwise, the next key is found by following pointer up the tree 
  ** until there is a key immediately to the right of the pointer followed 
  ** to reach the sub-tree containing the current key. */
  else if( iCell>=3 || pNode->aiKeyPtr[iCell]==0 ){
    while( (--pCsr->iNode)>=0 ){
      iCell = pCsr->aiCell[pCsr->iNode];
      if( iCell<3 && pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[iCell] ) break;
    }
  }

#ifndef NDEBUG
  if( pCsr->iNode>=0 ){
    TreeKey *pK2 = csrGetKey(pCsr, &pCsr->blob, &rc);
    assert( rc||treeKeycmp(TKV_KEY(pK2),pK2->nKey,TKV_KEY(pK1),pK1->nKey)>=0 );
  }
  tblobFree(pDb, &key1);
#endif

  return rc;
}

static int lsmTreeCursorPrev(TreeCursor *pCsr){
#ifndef NDEBUG
  TreeKey *pK1;
  TreeBlob key1 = {0, 0};
#endif
  lsm_db *pDb = pCsr->pDb;
  TreeRoot *pRoot = pCsr->pRoot;
  const int iLeaf = pRoot->nHeight-1;
  int iCell; 
  int rc = LSM_OK; 
  TreeNode *pNode; 

  /* Restore the cursor position, if required */
  int iRestore = 0;
  treeCursorRestore(pCsr, &iRestore);
  if( iRestore<0 ) return LSM_OK;

  /* Save a pointer to the current key. This is used in an assert() at the
  ** end of this function - to check that the 'next' key really is smaller
  ** than the current key. */
#ifndef NDEBUG
  pK1 = csrGetKey(pCsr, &key1, &rc);
  if( rc!=LSM_OK ) return rc;
#endif

  assert( lsmTreeCursorValid(pCsr) );
  pNode = pCsr->apTreeNode[pCsr->iNode];
  iCell = pCsr->aiCell[pCsr->iNode];
  assert( iCell>=0 && iCell<3 );

  /* If the current node is not a leaf, and the current cell has sub-tree
  ** associated with it, descend to the right-most key on the right-most
  ** leaf of the sub-tree.  */
  if( pCsr->iNode<iLeaf && getChildPtr(pNode, pRoot->iTransId, iCell) ){
    do {
      u32 iNodePtr;
      pCsr->iNode++;
      iNodePtr = getChildPtr(pNode, pRoot->iTransId, iCell);
      pNode = (TreeNode *)treeShmptr(pDb, iNodePtr);
      if( rc!=LSM_OK ) break;
      pCsr->apTreeNode[pCsr->iNode] = pNode;
      iCell = 1 + (pNode->aiKeyPtr[2]!=0) + (pCsr->iNode < iLeaf);
      pCsr->aiCell[pCsr->iNode] = (u8)iCell;
    }while( pCsr->iNode < iLeaf );
  }

  /* Otherwise, the next key is found by following pointer up the tree until
  ** there is a key immediately to the left of the pointer followed to reach
  ** the sub-tree containing the current key. */
  else{
    do {
      iCell = pCsr->aiCell[pCsr->iNode]-1;
      if( iCell>=0 && pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[iCell] ) break;
    }while( (--pCsr->iNode)>=0 );
    pCsr->aiCell[pCsr->iNode] = (u8)iCell;
  }

#ifndef NDEBUG
  if( pCsr->iNode>=0 ){
    TreeKey *pK2 = csrGetKey(pCsr, &pCsr->blob, &rc);
    assert( rc || treeKeycmp(TKV_KEY(pK2),pK2->nKey,TKV_KEY(pK1),pK1->nKey)<0 );
  }
  tblobFree(pDb, &key1);
#endif

  return rc;
}

/*
** Move the cursor to the first (bLast==0) or last (bLast!=0) entry in the
** in-memory tree.
*/
static int lsmTreeCursorEnd(TreeCursor *pCsr, int bLast){
  lsm_db *pDb = pCsr->pDb;
  TreeRoot *pRoot = pCsr->pRoot;
  int rc = LSM_OK;

  u32 iNodePtr;
  pCsr->iNode = -1;

  /* Discard any saved position data */
  treeCursorRestore(pCsr, 0);

  iNodePtr = pRoot->iRoot;
  while( iNodePtr ){
    int iCell;
    TreeNode *pNode;

    pNode = (TreeNode *)treeShmptr(pDb, iNodePtr);
    if( rc ) break;

    if( bLast ){
      iCell = ((pNode->aiKeyPtr[2]==0) ? 2 : 3);
    }else{
      iCell = ((pNode->aiKeyPtr[0]==0) ? 1 : 0);
    }
    pCsr->iNode++;
    pCsr->apTreeNode[pCsr->iNode] = pNode;

    if( (u32)pCsr->iNode<pRoot->nHeight-1 ){
      iNodePtr = getChildPtr(pNode, pRoot->iTransId, iCell);
    }else{
      iNodePtr = 0;
    }
    pCsr->aiCell[pCsr->iNode] = (u8)(iCell - (iNodePtr==0 && bLast));
  }

  return rc;
}

static int lsmTreeCursorFlags(TreeCursor *pCsr){
  int flags = 0;
  if( pCsr && pCsr->iNode>=0 ){
    int rc = LSM_OK;
    TreeKey *pKey = (TreeKey *)treeShmptrUnsafe(pCsr->pDb,
        pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[pCsr->aiCell[pCsr->iNode]]
    );
    assert( rc==LSM_OK );
    flags = (pKey->flags & ~LSM_CONTIGUOUS);
  }
  return flags;
}

static int lsmTreeCursorKey(TreeCursor *pCsr, int *pFlags, void **ppKey, int *pnKey){
  TreeKey *pTreeKey;
  int rc = LSM_OK;

  assert( lsmTreeCursorValid(pCsr) );

  pTreeKey = pCsr->pSave;
  if( !pTreeKey ){
    pTreeKey = csrGetKey(pCsr, &pCsr->blob, &rc);
  }
  if( rc==LSM_OK ){
    *pnKey = pTreeKey->nKey;
    if( pFlags ) *pFlags = pTreeKey->flags;
    *ppKey = (void *)&pTreeKey[1];
  }

  return rc;
}

static int lsmTreeCursorValue(TreeCursor *pCsr, void **ppVal, int *pnVal){
  int res = 0;
  int rc;

  rc = treeCursorRestore(pCsr, &res);
  if( res==0 ){
    TreeKey *pTreeKey = csrGetKey(pCsr, &pCsr->blob, &rc);
    if( rc==LSM_OK ){
      if( pTreeKey->flags & LSM_INSERT ){
        *pnVal = pTreeKey->nValue;
        *ppVal = TKV_VAL(pTreeKey);
      }else{
        *ppVal = 0;
        *pnVal = -1;
      }
    }
  }else{
    *ppVal = 0;
    *pnVal = 0;
  }

  return rc;
}

/*
** Return true if the cursor currently points to a valid entry. 
*/
static int lsmTreeCursorValid(TreeCursor *pCsr){
  return (pCsr && (pCsr->pSave || pCsr->iNode>=0));
}

/*
** Store a mark in *pMark. Later on, a call to lsmTreeRollback() with a
** pointer to the same TreeMark structure may be used to roll the tree
** contents back to their current state.
*/
static void lsmTreeMark(lsm_db *pDb, TreeMark *pMark){
  pMark->iRoot = pDb->treehdr.root.iRoot;
  pMark->nHeight = pDb->treehdr.root.nHeight;
  pMark->iWrite = pDb->treehdr.iWrite;
  pMark->nChunk = pDb->treehdr.nChunk;
  pMark->iNextShmid = pDb->treehdr.iNextShmid;
  pMark->iRollback = intArraySize(&pDb->rollback);
}

/*
** Roll back to mark pMark. Structure *pMark should have been previously
** populated by a call to lsmTreeMark().
*/
static void lsmTreeRollback(lsm_db *pDb, TreeMark *pMark){
  int iIdx;
  int nIdx;
  u32 iNext;
  ShmChunk *pChunk;
  u32 iChunk;
  u32 iShmid;

  /* Revert all required v2 pointers. */
  nIdx = intArraySize(&pDb->rollback);
  for(iIdx = pMark->iRollback; iIdx<nIdx; iIdx++){
    TreeNode *pNode;
    pNode = treeShmptr(pDb, intArrayEntry(&pDb->rollback, iIdx));
    assert( pNode );
    pNode->iV2 = 0;
    pNode->iV2Child = 0;
    pNode->iV2Ptr = 0;
  }
  intArrayTruncate(&pDb->rollback, pMark->iRollback);

  /* Restore the free-chunk list. */
  assert( pMark->iWrite!=0 );
  iChunk = treeOffsetToChunk(pMark->iWrite-1);
  pChunk = treeShmChunk(pDb, iChunk);
  iNext = pChunk->iNext;
  pChunk->iNext = 0;

  pChunk = treeShmChunk(pDb, pDb->treehdr.iFirst);
  iShmid = pChunk->iShmid-1;

  while( iNext ){
    u32 iFree = iNext;            /* Current chunk being rollback-freed */
    ShmChunk *pFree;              /* Pointer to chunk iFree */

    pFree = treeShmChunk(pDb, iFree);
    iNext = pFree->iNext;

    if( iFree<pMark->nChunk ){
      pFree->iNext = pDb->treehdr.iFirst;
      pFree->iShmid = iShmid--;
      pDb->treehdr.iFirst = iFree;
    }
  }

  /* Restore the tree-header fields */
  pDb->treehdr.root.iRoot = pMark->iRoot;
  pDb->treehdr.root.nHeight = pMark->nHeight;
  pDb->treehdr.iWrite = pMark->iWrite;
  pDb->treehdr.nChunk = pMark->nChunk;
  pDb->treehdr.iNextShmid = pMark->iNextShmid;
}

/*
** Load the in-memory tree header from shared-memory into pDb->treehdr.
** If the header cannot be loaded, return LSM_PROTOCOL.
**
** If the header is successfully loaded and parameter piRead is not NULL,
** is is set to 1 if the header was loaded from ShmHeader.hdr1, or 2 if
** the header was loaded from ShmHeader.hdr2.
*/
static int lsmTreeLoadHeader(lsm_db *pDb, int *piRead){
  int nRem = LSM_ATTEMPTS_BEFORE_PROTOCOL;
  while( (nRem--)>0 ){
    ShmHeader *pShm = pDb->pShmhdr;

    memcpy(&pDb->treehdr, &pShm->hdr1, sizeof(TreeHeader));
    if( treeHeaderChecksumOk(&pDb->treehdr) ){
      if( piRead ) *piRead = 1;
      return LSM_OK;
    }
    memcpy(&pDb->treehdr, &pShm->hdr2, sizeof(TreeHeader));
    if( treeHeaderChecksumOk(&pDb->treehdr) ){
      if( piRead ) *piRead = 2;
      return LSM_OK;
    }

    lsmShmBarrier(pDb);
  }
  return LSM_PROTOCOL_BKPT;
}

static int lsmTreeLoadHeaderOk(lsm_db *pDb, int iRead){
  TreeHeader *p = (iRead==1) ? &pDb->pShmhdr->hdr1 : &pDb->pShmhdr->hdr2;
  assert( iRead==1 || iRead==2 );
  return (0==memcmp(pDb->treehdr.aCksum, p->aCksum, sizeof(u32)*2));
}

/*
** This function is called to conclude a transaction. If argument bCommit
** is true, the transaction is committed. Otherwise it is rolled back.
*/
static int lsmTreeEndTransaction(lsm_db *pDb, int bCommit){
  ShmHeader *pShm = pDb->pShmhdr;

  treeHeaderChecksum(&pDb->treehdr, pDb->treehdr.aCksum);
  memcpy(&pShm->hdr2, &pDb->treehdr, sizeof(TreeHeader));
  lsmShmBarrier(pDb);
  memcpy(&pShm->hdr1, &pDb->treehdr, sizeof(TreeHeader));
  pShm->bWriter = 0;
  intArrayFree(pDb->pEnv, &pDb->rollback);

  return LSM_OK;
}

#ifndef NDEBUG
static int assert_delete_ranges_match(lsm_db *db){
  int prev = 0;
  TreeBlob blob = {0, 0};
  TreeCursor csr;               /* Cursor used to iterate through tree */
  int rc;

  treeCursorInit(db, 0, &csr);
  for( rc = lsmTreeCursorEnd(&csr, 0);
       rc==LSM_OK && lsmTreeCursorValid(&csr);
       rc = lsmTreeCursorNext(&csr)
  ){
    TreeKey *pKey = csrGetKey(&csr, &blob, &rc);
    if( rc!=LSM_OK ) break;
    assert( ((prev&LSM_START_DELETE)==0)==((pKey->flags&LSM_END_DELETE)==0) );
    prev = pKey->flags;
  }

  tblobFree(csr.pDb, &csr.blob);
  tblobFree(csr.pDb, &blob);

  return 1;
}

static int treeCountEntries(lsm_db *db){
  TreeCursor csr;               /* Cursor used to iterate through tree */
  int rc;
  int nEntry = 0;

  treeCursorInit(db, 0, &csr);
  for( rc = lsmTreeCursorEnd(&csr, 0);
       rc==LSM_OK && lsmTreeCursorValid(&csr);
       rc = lsmTreeCursorNext(&csr)
  ){
    nEntry++;
  }

  tblobFree(csr.pDb, &csr.blob);

  return nEntry;
}
#endif

#line 1 "lsm_unix.c"
/*
** 2011-12-03
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Unix-specific run-time environment implementation for LSM.
*/

#ifndef _WIN32

#if defined(__GNUC__) || defined(__TINYC__)
/* workaround for ftruncate() visibility on gcc. */
# ifndef _XOPEN_SOURCE
#  define _XOPEN_SOURCE 500
# endif
#endif

#include <unistd.h>
#include <sys/types.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>

#include <unistd.h>
#include <errno.h>

#include <sys/mman.h>
/* #include "lsmInt.h" */

/* There is no fdatasync() call on Android */
#ifdef __ANDROID__
# define fdatasync(x) fsync(x)
#endif

/*
** An open file is an instance of the following object
*/
typedef struct PosixFile PosixFile;
struct PosixFile {
  lsm_env *pEnv;                  /* The run-time environment */
  const char *zName;              /* Full path to file */
  int fd;                         /* The open file descriptor */
  int shmfd;                      /* Shared memory file-descriptor */
  void *pMap;                     /* Pointer to mapping of file fd */
  off_t nMap;                     /* Size of mapping at pMap in bytes */
  int nShm;                       /* Number of entries in array apShm[] */
  void **apShm;                   /* Array of 32K shared memory segments */
};

static char *posixShmFile(PosixFile *p){
  char *zShm;
  int nName = strlen(p->zName);
  zShm = (char *)lsmMalloc(p->pEnv, nName+4+1);
  if( zShm ){
    memcpy(zShm, p->zName, nName);
    memcpy(&zShm[nName], "-shm", 5);
  }
  return zShm;
}

static int lsmPosixOsOpen(
  lsm_env *pEnv,
  const char *zFile,
  int flags,
  lsm_file **ppFile
){
  int rc = LSM_OK;
  PosixFile *p;

  p = lsm_malloc(pEnv, sizeof(PosixFile));
  if( p==0 ){
    rc = LSM_NOMEM;
  }else{
    int bReadonly = (flags & LSM_OPEN_READONLY);
    int oflags = (bReadonly ? O_RDONLY : (O_RDWR|O_CREAT));
    memset(p, 0, sizeof(PosixFile));
    p->zName = zFile;
    p->pEnv = pEnv;
    p->fd = open(zFile, oflags, 0644);
    if( p->fd<0 ){
      lsm_free(pEnv, p);
      p = 0;
      if( errno==ENOENT ){
        rc = lsmErrorBkpt(LSM_IOERR_NOENT);
      }else{
        rc = LSM_IOERR_BKPT;
      }
    }
  }

  *ppFile = (lsm_file *)p;
  return rc;
}

static int lsmPosixOsWrite(
  lsm_file *pFile,                /* File to write to */
  lsm_i64 iOff,                   /* Offset to write to */
  void *pData,                    /* Write data from this buffer */
  int nData                       /* Bytes of data to write */
){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = LSM_IOERR_BKPT;
  }else{
    ssize_t prc = write(p->fd, pData, (size_t)nData);
    if( prc<0 ) rc = LSM_IOERR_BKPT;
  }

  return rc;
}

static int lsmPosixOsTruncate(
  lsm_file *pFile,                /* File to write to */
  lsm_i64 nSize                   /* Size to truncate file to */
){
  PosixFile *p = (PosixFile *)pFile;
  int rc = LSM_OK;                /* Return code */
  int prc;                        /* Posix Return Code */
  struct stat sStat;              /* Result of fstat() invocation */
  
  prc = fstat(p->fd, &sStat);
  if( prc==0 && sStat.st_size>nSize ){
    prc = ftruncate(p->fd, (off_t)nSize);
  }
  if( prc<0 ) rc = LSM_IOERR_BKPT;

  return rc;
}

static int lsmPosixOsRead(
  lsm_file *pFile,                /* File to read from */
  lsm_i64 iOff,                   /* Offset to read from */
  void *pData,                    /* Read data into this buffer */
  int nData                       /* Bytes of data to read */
){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = LSM_IOERR_BKPT;
  }else{
    ssize_t prc = read(p->fd, pData, (size_t)nData);
    if( prc<0 ){ 
      rc = LSM_IOERR_BKPT;
    }else if( prc<nData ){
      memset(&((u8 *)pData)[prc], 0, nData - prc);
    }

  }

  return rc;
}

static int lsmPosixOsSync(lsm_file *pFile){
  int rc = LSM_OK;

#ifndef LSM_NO_SYNC
  PosixFile *p = (PosixFile *)pFile;
  int prc = 0;

  if( p->pMap ){
    prc = msync(p->pMap, p->nMap, MS_SYNC);
  }
  if( prc==0 ) prc = fsync(p->fd);
  if( prc<0 ) rc = LSM_IOERR_BKPT;
#else
  (void)pFile;
#endif

  return rc;
}

static int lsmPosixOsSectorSize(lsm_file *pFile){
  return 512;
}

static int lsmPosixOsRemap(
  lsm_file *pFile, 
  lsm_i64 iMin, 
  void **ppOut,
  lsm_i64 *pnOut
){
  off_t iSz;
  int prc;
  PosixFile *p = (PosixFile *)pFile;
  struct stat buf;

  /* If the file is between 0 and 2MB in size, extend it in chunks of 256K.
  ** Thereafter, in chunks of 1MB at a time.  */
  const int aIncrSz[] = {256*1024, 1024*1024};
  int nIncrSz = aIncrSz[iMin>(2*1024*1024)];

  if( p->pMap ){
    munmap(p->pMap, p->nMap);
    *ppOut = p->pMap = 0;
    *pnOut = p->nMap = 0;
  }

  if( iMin>=0 ){
    memset(&buf, 0, sizeof(buf));
    prc = fstat(p->fd, &buf);
    if( prc!=0 ) return LSM_IOERR_BKPT;
    iSz = buf.st_size;
    if( iSz<iMin ){
      iSz = ((iMin + nIncrSz-1) / nIncrSz) * nIncrSz;
      prc = ftruncate(p->fd, iSz);
      if( prc!=0 ) return LSM_IOERR_BKPT;
    }

    p->pMap = mmap(0, iSz, PROT_READ|PROT_WRITE, MAP_SHARED, p->fd, 0);
    if( p->pMap==MAP_FAILED ){
      p->pMap = 0;
      return LSM_IOERR_BKPT;
    }
    p->nMap = iSz;
  }

  *ppOut = p->pMap;
  *pnOut = p->nMap;
  return LSM_OK;
}

static int lsmPosixOsFullpath(
  lsm_env *pEnv,
  const char *zName,
  char *zOut,
  int *pnOut
){
  int nBuf = *pnOut;
  int nReq;

  if( zName[0]!='/' ){
    char *z;
    char *zTmp;
    int nTmp = 512;
    zTmp = lsmMalloc(pEnv, nTmp);
    while( zTmp ){
      z = getcwd(zTmp, nTmp);
      if( z || errno!=ERANGE ) break;
      nTmp = nTmp*2;
      zTmp = lsmReallocOrFree(pEnv, zTmp, nTmp);
    }
    if( zTmp==0 ) return LSM_NOMEM_BKPT;
    if( z==0 ) return LSM_IOERR_BKPT;
    assert( z==zTmp );

    nTmp = strlen(zTmp);
    nReq = nTmp + 1 + strlen(zName) + 1;
    if( nReq<=nBuf ){
      memcpy(zOut, zTmp, nTmp);
      zOut[nTmp] = '/';
      memcpy(&zOut[nTmp+1], zName, strlen(zName)+1);
    }
    lsmFree(pEnv, zTmp);
  }else{
    nReq = strlen(zName)+1;
    if( nReq<=nBuf ){
      memcpy(zOut, zName, strlen(zName)+1);
    }
  }

  *pnOut = nReq;
  return LSM_OK;
}

static int lsmPosixOsFileid(
  lsm_file *pFile, 
  void *pBuf,
  int *pnBuf
){
  int prc;
  int nBuf;
  int nReq;
  PosixFile *p = (PosixFile *)pFile;
  struct stat buf;

  nBuf = *pnBuf;
  nReq = (sizeof(buf.st_dev) + sizeof(buf.st_ino));
  *pnBuf = nReq;
  if( nReq>nBuf ) return LSM_OK;

  memset(&buf, 0, sizeof(buf));
  prc = fstat(p->fd, &buf);
  if( prc!=0 ) return LSM_IOERR_BKPT;

  memcpy(pBuf, &buf.st_dev, sizeof(buf.st_dev));
  memcpy(&(((u8 *)pBuf)[sizeof(buf.st_dev)]), &buf.st_ino, sizeof(buf.st_ino));
  return LSM_OK;
}

static int lsmPosixOsUnlink(lsm_env *pEnv, const char *zFile){
  int prc = unlink(zFile);
  return prc ? LSM_IOERR_BKPT : LSM_OK;
}

static int lsmPosixOsLock(lsm_file *pFile, int iLock, int eType){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  static const short aType[3] = { F_UNLCK, F_RDLCK, F_WRLCK };
  struct flock lock;

  assert( aType[LSM_LOCK_UNLOCK]==F_UNLCK );
  assert( aType[LSM_LOCK_SHARED]==F_RDLCK );
  assert( aType[LSM_LOCK_EXCL]==F_WRLCK );
  assert( eType>=0 && eType<array_size(aType) );
  assert( iLock>0 && iLock<=32 );

  memset(&lock, 0, sizeof(lock));
  lock.l_whence = SEEK_SET;
  lock.l_len = 1;
  lock.l_type = aType[eType];
  lock.l_start = (4096-iLock);

  if( fcntl(p->fd, F_SETLK, &lock) ){
    int e = errno;
    if( e==EACCES || e==EAGAIN ){
      rc = LSM_BUSY;
    }else{
      rc = LSM_IOERR_BKPT;
    }
  }

  return rc;
}

static int lsmPosixOsTestLock(lsm_file *pFile, int iLock, int nLock, int eType){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  static const short aType[3] = { 0, F_RDLCK, F_WRLCK };
  struct flock lock;

  assert( eType==LSM_LOCK_SHARED || eType==LSM_LOCK_EXCL );
  assert( aType[LSM_LOCK_SHARED]==F_RDLCK );
  assert( aType[LSM_LOCK_EXCL]==F_WRLCK );
  assert( eType>=0 && eType<array_size(aType) );
  assert( iLock>0 && iLock<=32 );

  memset(&lock, 0, sizeof(lock));
  lock.l_whence = SEEK_SET;
  lock.l_len = nLock;
  lock.l_type = aType[eType];
  lock.l_start = (4096-iLock-nLock+1);

  if( fcntl(p->fd, F_GETLK, &lock) ){
    rc = LSM_IOERR_BKPT;
  }else if( lock.l_type!=F_UNLCK ){
    rc = LSM_BUSY;
  }

  return rc;
}

static int lsmPosixOsShmMap(lsm_file *pFile, int iChunk, int sz, void **ppShm){
  PosixFile *p = (PosixFile *)pFile;

  *ppShm = 0;
  assert( sz==LSM_SHM_CHUNK_SIZE );
  if( iChunk>=p->nShm ){
    int i;
    void **apNew;
    int nNew = iChunk+1;
    off_t nReq = nNew * LSM_SHM_CHUNK_SIZE;
    struct stat sStat;

    /* If the shared-memory file has not been opened, open it now. */
    if( p->shmfd<=0 ){
      char *zShm = posixShmFile(p);
      if( !zShm ) return LSM_NOMEM_BKPT;
      p->shmfd = open(zShm, O_RDWR|O_CREAT, 0644);
      lsmFree(p->pEnv, zShm);
      if( p->shmfd<0 ){ 
        return LSM_IOERR_BKPT;
      }
    }

    /* If the shared-memory file is not large enough to contain the 
    ** requested chunk, cause it to grow.  */
    if( fstat(p->shmfd, &sStat) ){
      return LSM_IOERR_BKPT;
    }
    if( sStat.st_size<nReq ){
      if( ftruncate(p->shmfd, nReq) ){
        return LSM_IOERR_BKPT;
      }
    }

    apNew = (void **)lsmRealloc(p->pEnv, p->apShm, sizeof(void *) * nNew);
    if( !apNew ) return LSM_NOMEM_BKPT;
    for(i=p->nShm; i<nNew; i++){
      apNew[i] = 0;
    }
    p->apShm = apNew;
    p->nShm = nNew;
  }

  if( p->apShm[iChunk]==0 ){
    p->apShm[iChunk] = mmap(0, LSM_SHM_CHUNK_SIZE, 
        PROT_READ|PROT_WRITE, MAP_SHARED, p->shmfd, iChunk*LSM_SHM_CHUNK_SIZE
    );
    if( p->apShm[iChunk]==MAP_FAILED ){
      p->apShm[iChunk] = 0;
      return LSM_IOERR_BKPT;
    }
  }

  *ppShm = p->apShm[iChunk];
  return LSM_OK;
}

static void lsmPosixOsShmBarrier(void){
}

static int lsmPosixOsShmUnmap(lsm_file *pFile, int bDelete){
  PosixFile *p = (PosixFile *)pFile;
  if( p->shmfd>0 ){
    int i;
    for(i=0; i<p->nShm; i++){
      if( p->apShm[i] ){
        munmap(p->apShm[i], LSM_SHM_CHUNK_SIZE);
        p->apShm[i] = 0;
      }
    }
    close(p->shmfd);
    p->shmfd = 0;
    if( bDelete ){
      char *zShm = posixShmFile(p);
      if( zShm ) unlink(zShm);
      lsmFree(p->pEnv, zShm);
    }
  }
  return LSM_OK;
}


static int lsmPosixOsClose(lsm_file *pFile){
   PosixFile *p = (PosixFile *)pFile;
   lsmPosixOsShmUnmap(pFile, 0);
   if( p->pMap ) munmap(p->pMap, p->nMap);
   close(p->fd);
   lsm_free(p->pEnv, p->apShm);
   lsm_free(p->pEnv, p);
   return LSM_OK;
}

static int lsmPosixOsSleep(lsm_env *pEnv, int us){
#if 0
  /* Apparently on Android usleep() returns void */
  if( usleep(us) ) return LSM_IOERR;
#endif
  usleep(us);
  return LSM_OK;
}

/****************************************************************************
** Memory allocation routines.
*/
#define BLOCK_HDR_SIZE ROUND8( sizeof(size_t) )

static void *lsmPosixOsMalloc(lsm_env *pEnv, size_t N){
  unsigned char * m;
  N += BLOCK_HDR_SIZE;
  m = (unsigned char *)malloc(N);
  *((size_t*)m) = N;
  return m + BLOCK_HDR_SIZE;
}

static void lsmPosixOsFree(lsm_env *pEnv, void *p){
  if(p){
    free( ((unsigned char *)p) - BLOCK_HDR_SIZE );
  }
}

static void *lsmPosixOsRealloc(lsm_env *pEnv, void *p, size_t N){
  unsigned char * m = (unsigned char *)p;
  if(1>N){
    lsmPosixOsFree( pEnv, p );
    return NULL;
  }else if(NULL==p){
    return lsmPosixOsMalloc(pEnv, N);
  }else{
    void * re = NULL;
    m -= BLOCK_HDR_SIZE;
#if 0 /* arguable: don't shrink */
    size_t * sz = (size_t*)m;
    if(*sz >= (size_t)N){
      return p;
    }
#endif
    re = realloc( m, N + BLOCK_HDR_SIZE );
    if(re){
      m = (unsigned char *)re;
      *((size_t*)m) = N;
      return m + BLOCK_HDR_SIZE;
    }else{
      return NULL;
    }
  }
}

static size_t lsmPosixOsMSize(lsm_env *pEnv, void *p){
  unsigned char * m = (unsigned char *)p;
  return *((size_t*)(m-BLOCK_HDR_SIZE));
}
#undef BLOCK_HDR_SIZE


#ifdef LSM_MUTEX_PTHREADS 
/*************************************************************************
** Mutex methods for pthreads based systems.  If LSM_MUTEX_PTHREADS is
** missing then a no-op implementation of mutexes found in lsm_mutex.c
** will be used instead.
*/
#include <pthread.h>

typedef struct PthreadMutex PthreadMutex;
struct PthreadMutex {
  lsm_env *pEnv;
  pthread_mutex_t mutex;
#ifdef LSM_DEBUG
  pthread_t owner;
#endif
};

#ifdef LSM_DEBUG
# define LSM_PTHREAD_STATIC_MUTEX { 0, PTHREAD_MUTEX_INITIALIZER, 0 }
#else
# define LSM_PTHREAD_STATIC_MUTEX { 0, PTHREAD_MUTEX_INITIALIZER }
#endif

static int lsmPosixOsMutexStatic(
  lsm_env *pEnv,
  int iMutex,
  lsm_mutex **ppStatic
){
  static PthreadMutex sMutex[2] = {
    LSM_PTHREAD_STATIC_MUTEX,
    LSM_PTHREAD_STATIC_MUTEX
  };

  assert( iMutex==LSM_MUTEX_GLOBAL || iMutex==LSM_MUTEX_HEAP );
  assert( LSM_MUTEX_GLOBAL==1 && LSM_MUTEX_HEAP==2 );

  *ppStatic = (lsm_mutex *)&sMutex[iMutex-1];
  return LSM_OK;
}

static int lsmPosixOsMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  PthreadMutex *pMutex;           /* Pointer to new mutex */
  pthread_mutexattr_t attr;       /* Attributes object */

  pMutex = (PthreadMutex *)lsmMallocZero(pEnv, sizeof(PthreadMutex));
  if( !pMutex ) return LSM_NOMEM_BKPT;

  pMutex->pEnv = pEnv;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&pMutex->mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  *ppNew = (lsm_mutex *)pMutex;
  return LSM_OK;
}

static void lsmPosixOsMutexDel(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_destroy(&pMutex->mutex);
  lsmFree(pMutex->pEnv, pMutex);
}

static void lsmPosixOsMutexEnter(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_lock(&pMutex->mutex);

#ifdef LSM_DEBUG
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = pthread_self();
  assert( pthread_equal(pMutex->owner, pthread_self()) );
#endif
}

static int lsmPosixOsMutexTry(lsm_mutex *p){
  int ret;
  PthreadMutex *pMutex = (PthreadMutex *)p;
  ret = pthread_mutex_trylock(&pMutex->mutex);
#ifdef LSM_DEBUG
  if( ret==0 ){
    assert( !pthread_equal(pMutex->owner, pthread_self()) );
    pMutex->owner = pthread_self();
    assert( pthread_equal(pMutex->owner, pthread_self()) );
  }
#endif
  return ret;
}

static void lsmPosixOsMutexLeave(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
#ifdef LSM_DEBUG
  assert( pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = 0;
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
#endif
  pthread_mutex_unlock(&pMutex->mutex);
}

#ifdef LSM_DEBUG
static int lsmPosixOsMutexHeld(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  return pMutex ? pthread_equal(pMutex->owner, pthread_self()) : 1;
}
static int lsmPosixOsMutexNotHeld(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  return pMutex ? !pthread_equal(pMutex->owner, pthread_self()) : 1;
}
#endif
/*
** End of pthreads mutex implementation.
*************************************************************************/
#else
/*************************************************************************
** Noop mutex implementation
*/
typedef struct NoopMutex NoopMutex;
struct NoopMutex {
  lsm_env *pEnv;                  /* Environment handle (for xFree()) */
  int bHeld;                      /* True if mutex is held */
  int bStatic;                    /* True for a static mutex */
};
static NoopMutex aStaticNoopMutex[2] = {
  {0, 0, 1},
  {0, 0, 1},
};

static int lsmPosixOsMutexStatic(
  lsm_env *pEnv,
  int iMutex,
  lsm_mutex **ppStatic
){
  assert( iMutex>=1 && iMutex<=(int)array_size(aStaticNoopMutex) );
  *ppStatic = (lsm_mutex *)&aStaticNoopMutex[iMutex-1];
  return LSM_OK;
}
static int lsmPosixOsMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  NoopMutex *p;
  p = (NoopMutex *)lsmMallocZero(pEnv, sizeof(NoopMutex));
  if( p ) p->pEnv = pEnv;
  *ppNew = (lsm_mutex *)p;
  return (p ? LSM_OK : LSM_NOMEM_BKPT);
}
static void lsmPosixOsMutexDel(lsm_mutex *pMutex)  { 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bStatic==0 && p->pEnv );
  lsmFree(p->pEnv, p);
}
static void lsmPosixOsMutexEnter(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
}
static int lsmPosixOsMutexTry(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
  return 0;
}
static void lsmPosixOsMutexLeave(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==1 );
  p->bHeld = 0;
}
#ifdef LSM_DEBUG
static int lsmPosixOsMutexHeld(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  return p ? p->bHeld : 1;
}
static int lsmPosixOsMutexNotHeld(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  return p ? !p->bHeld : 1;
}
#endif
/***************************************************************************/
#endif /* else LSM_MUTEX_NONE */

/* Without LSM_DEBUG, the MutexHeld tests are never called */
#ifndef LSM_DEBUG
# define lsmPosixOsMutexHeld    0
# define lsmPosixOsMutexNotHeld 0
#endif

lsm_env *lsm_default_env(void){
  static lsm_env posix_env = {
    sizeof(lsm_env),         /* nByte */
    1,                       /* iVersion */
    /***** file i/o ******************/
    0,                       /* pVfsCtx */
    lsmPosixOsFullpath,      /* xFullpath */
    lsmPosixOsOpen,          /* xOpen */
    lsmPosixOsRead,          /* xRead */
    lsmPosixOsWrite,         /* xWrite */
    lsmPosixOsTruncate,      /* xTruncate */
    lsmPosixOsSync,          /* xSync */
    lsmPosixOsSectorSize,    /* xSectorSize */
    lsmPosixOsRemap,         /* xRemap */
    lsmPosixOsFileid,        /* xFileid */
    lsmPosixOsClose,         /* xClose */
    lsmPosixOsUnlink,        /* xUnlink */
    lsmPosixOsLock,          /* xLock */
    lsmPosixOsTestLock,      /* xTestLock */
    lsmPosixOsShmMap,        /* xShmMap */
    lsmPosixOsShmBarrier,    /* xShmBarrier */
    lsmPosixOsShmUnmap,      /* xShmUnmap */
    /***** memory allocation *********/
    0,                       /* pMemCtx */
    lsmPosixOsMalloc,        /* xMalloc */
    lsmPosixOsRealloc,       /* xRealloc */
    lsmPosixOsFree,          /* xFree */
    lsmPosixOsMSize,         /* xSize */
    /***** mutexes *********************/
    0,                       /* pMutexCtx */
    lsmPosixOsMutexStatic,   /* xMutexStatic */
    lsmPosixOsMutexNew,      /* xMutexNew */
    lsmPosixOsMutexDel,      /* xMutexDel */
    lsmPosixOsMutexEnter,    /* xMutexEnter */
    lsmPosixOsMutexTry,      /* xMutexTry */
    lsmPosixOsMutexLeave,    /* xMutexLeave */
    lsmPosixOsMutexHeld,     /* xMutexHeld */
    lsmPosixOsMutexNotHeld,  /* xMutexNotHeld */
    /***** other *********************/
    lsmPosixOsSleep,         /* xSleep */
  };
  return &posix_env;
}

#endif

#line 1 "lsm_varint.c"

/*
** 2012-02-08
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** SQLite4-compatible varint implementation.
*/
/* #include "lsmInt.h" */

/*************************************************************************
** The following is a copy of the varint.c module from SQLite 4.
*/

/*
** Decode the varint in z[].  Write the integer value into *pResult and
** return the number of bytes in the varint.
*/
static int lsmSqlite4GetVarint64(const unsigned char *z, u64 *pResult){
  unsigned int x;
  if( z[0]<=240 ){
    *pResult = z[0];
    return 1;
  }
  if( z[0]<=248 ){
    *pResult = (z[0]-241)*256 + z[1] + 240;
    return 2;
  }
  if( z[0]==249 ){
    *pResult = 2288 + 256*z[1] + z[2];
    return 3;
  }
  if( z[0]==250 ){
    *pResult = (z[1]<<16) + (z[2]<<8) + z[3];
    return 4;
  }
  x = (z[1]<<24) + (z[2]<<16) + (z[3]<<8) + z[4];
  if( z[0]==251 ){
    *pResult = x;
    return 5;
  }
  if( z[0]==252 ){
    *pResult = (((u64)x)<<8) + z[5];
    return 6;
  }
  if( z[0]==253 ){
    *pResult = (((u64)x)<<16) + (z[5]<<8) + z[6];
    return 7;
  }
  if( z[0]==254 ){
    *pResult = (((u64)x)<<24) + (z[5]<<16) + (z[6]<<8) + z[7];
    return 8;
  }
  *pResult = (((u64)x)<<32) +
               (0xffffffff & ((z[5]<<24) + (z[6]<<16) + (z[7]<<8) + z[8]));
  return 9;
}

/*
** Write a 32-bit unsigned integer as 4 big-endian bytes.
*/
static void lsmVarintWrite32(unsigned char *z, unsigned int y){
  z[0] = (unsigned char)(y>>24);
  z[1] = (unsigned char)(y>>16);
  z[2] = (unsigned char)(y>>8);
  z[3] = (unsigned char)(y);
}

/*
** Write a varint into z[].  The buffer z[] must be at least 9 characters
** long to accommodate the largest possible varint.  Return the number of
** bytes of z[] used.
*/
static int lsmSqlite4PutVarint64(unsigned char *z, u64 x){
  unsigned int w, y;
  if( x<=240 ){
    z[0] = (unsigned char)x;
    return 1;
  }
  if( x<=2287 ){
    y = (unsigned int)(x - 240);
    z[0] = (unsigned char)(y/256 + 241);
    z[1] = (unsigned char)(y%256);
    return 2;
  }
  if( x<=67823 ){
    y = (unsigned int)(x - 2288);
    z[0] = 249;
    z[1] = (unsigned char)(y/256);
    z[2] = (unsigned char)(y%256);
    return 3;
  }
  y = (unsigned int)x;
  w = (unsigned int)(x>>32);
  if( w==0 ){
    if( y<=16777215 ){
      z[0] = 250;
      z[1] = (unsigned char)(y>>16);
      z[2] = (unsigned char)(y>>8);
      z[3] = (unsigned char)(y);
      return 4;
    }
    z[0] = 251;
    lsmVarintWrite32(z+1, y);
    return 5;
  }
  if( w<=255 ){
    z[0] = 252;
    z[1] = (unsigned char)w;
    lsmVarintWrite32(z+2, y);
    return 6;
  }
  if( w<=32767 ){
    z[0] = 253;
    z[1] = (unsigned char)(w>>8);
    z[2] = (unsigned char)w;
    lsmVarintWrite32(z+3, y);
    return 7;
  }
  if( w<=16777215 ){
    z[0] = 254;
    z[1] = (unsigned char)(w>>16);
    z[2] = (unsigned char)(w>>8);
    z[3] = (unsigned char)w;
    lsmVarintWrite32(z+4, y);
    return 8;
  }
  z[0] = 255;
  lsmVarintWrite32(z+1, w);
  lsmVarintWrite32(z+5, y);
  return 9;
}

/*
** End of SQLite 4 code.
*************************************************************************/

static int lsmVarintPut64(u8 *aData, i64 iVal){
  return lsmSqlite4PutVarint64(aData, (u64)iVal);
}

static int lsmVarintGet64(const u8 *aData, i64 *piVal){
  return lsmSqlite4GetVarint64(aData, (u64 *)piVal);
}

static int lsmVarintPut32(u8 *aData, int iVal){
  return lsmSqlite4PutVarint64(aData, (u64)iVal);
}

static int lsmVarintGet32(u8 *z, int *piVal){
  u64 i;
  int ret;

  if( z[0]<=240 ){
    *piVal = z[0];
    return 1;
  }
  if( z[0]<=248 ){
    *piVal = (z[0]-241)*256 + z[1] + 240;
    return 2;
  }
  if( z[0]==249 ){
    *piVal = 2288 + 256*z[1] + z[2];
    return 3;
  }
  if( z[0]==250 ){
    *piVal = (z[1]<<16) + (z[2]<<8) + z[3];
    return 4;
  }

  ret = lsmSqlite4GetVarint64(z, &i);
  *piVal = (int)i;
  return ret;
}

static int lsmVarintLen32(int n){
  u8 aData[9];
  return lsmVarintPut32(aData, n);
}

static int lsmVarintLen64(i64 n){
  u8 aData[9];
  return lsmVarintPut64(aData, n);
}

/*
** The argument is the first byte of a varint. This function returns the
** total number of bytes in the entire varint (including the first byte).
*/
static int lsmVarintSize(u8 c){
  if( c<241 ) return 1;
  if( c<249 ) return 2;
  return (int)(c - 246);
}

#line 1 "lsm_win32.c"
/*
** 2011-12-03
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Win32-specific run-time environment implementation for LSM.
*/

#ifdef _WIN32

#include <assert.h>
#include <string.h>

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>

#include "windows.h"

/* #include "lsmInt.h" */

/*
** An open file is an instance of the following object
*/
typedef struct Win32File Win32File;
struct Win32File {
  lsm_env *pEnv;                  /* The run-time environment */
  const char *zName;              /* Full path to file */

  HANDLE hFile;                   /* Open file handle */
  HANDLE hShmFile;                /* File handle for *-shm file */

  SYSTEM_INFO sysInfo;            /* Operating system information */
  HANDLE hMap;                    /* File handle for mapping */
  LPVOID pMap;                    /* Pointer to mapping of file fd */
  size_t nMap;                    /* Size of mapping at pMap in bytes */
  int nShm;                       /* Number of entries in ahShm[]/apShm[] */
  LPHANDLE ahShm;                 /* Array of handles for shared mappings */
  LPVOID *apShm;                  /* Array of 32K shared memory segments */
};

static char *win32ShmFile(Win32File *pWin32File){
  char *zShm;
  int nName = strlen(pWin32File->zName);
  zShm = (char *)lsmMallocZero(pWin32File->pEnv, nName+4+1);
  if( zShm ){
    memcpy(zShm, pWin32File->zName, nName);
    memcpy(&zShm[nName], "-shm", 5);
  }
  return zShm;
}

static int win32Sleep(int us){
  Sleep((us + 999) / 1000);
  return LSM_OK;
}

/*
** The number of times that an I/O operation will be retried following a
** locking error - probably caused by antivirus software.  Also the initial
** delay before the first retry.  The delay increases linearly with each
** retry.
*/
#ifndef LSM_WIN32_IOERR_RETRY
# define LSM_WIN32_IOERR_RETRY 10
#endif
#ifndef LSM_WIN32_IOERR_RETRY_DELAY
# define LSM_WIN32_IOERR_RETRY_DELAY 25000
#endif
static int win32IoerrRetry = LSM_WIN32_IOERR_RETRY;
static int win32IoerrRetryDelay = LSM_WIN32_IOERR_RETRY_DELAY;

/*
** The "win32IoerrCanRetry1" macro is used to determine if a particular
** I/O error code obtained via GetLastError() is eligible to be retried.
** It must accept the error code DWORD as its only argument and should
** return non-zero if the error code is transient in nature and the
** operation responsible for generating the original error might succeed
** upon being retried.  The argument to this macro should be a variable.
**
** Additionally, a macro named "win32IoerrCanRetry2" may be defined.  If
** it is defined, it will be consulted only when the macro
** "win32IoerrCanRetry1" returns zero.  The "win32IoerrCanRetry2" macro
** is completely optional and may be used to include additional error
** codes in the set that should result in the failing I/O operation being
** retried by the caller.  If defined, the "win32IoerrCanRetry2" macro
** must exhibit external semantics identical to those of the
** "win32IoerrCanRetry1" macro.
*/
#if !defined(win32IoerrCanRetry1)
#define win32IoerrCanRetry1(a) (((a)==ERROR_ACCESS_DENIED)        || \
                                ((a)==ERROR_SHARING_VIOLATION)    || \
                                ((a)==ERROR_LOCK_VIOLATION)       || \
                                ((a)==ERROR_DEV_NOT_EXIST)        || \
                                ((a)==ERROR_NETNAME_DELETED)      || \
                                ((a)==ERROR_SEM_TIMEOUT)          || \
                                ((a)==ERROR_NETWORK_UNREACHABLE))
#endif

/*
** If an I/O error occurs, invoke this routine to see if it should be
** retried.  Return TRUE to retry.  Return FALSE to give up with an
** error.
*/
static int win32RetryIoerr(
  lsm_env *pEnv,
  int *pnRetry
){
  DWORD lastErrno;
  if( *pnRetry>=win32IoerrRetry ){
    return 0;
  }
  lastErrno = GetLastError();
  if( win32IoerrCanRetry1(lastErrno) ){
    win32Sleep(win32IoerrRetryDelay*(1+*pnRetry));
    ++*pnRetry;
    return 1;
  }
#if defined(win32IoerrCanRetry2)
  else if( win32IoerrCanRetry2(lastErrno) ){
    win32Sleep(win32IoerrRetryDelay*(1+*pnRetry));
    ++*pnRetry;
    return 1;
  }
#endif
  return 0;
}

/*
** Convert a UTF-8 string to Microsoft Unicode.
**
** Space to hold the returned string is obtained from lsmMalloc().
*/
static LPWSTR win32Utf8ToUnicode(lsm_env *pEnv, const char *zText){
  int nChar;
  LPWSTR zWideText;

  nChar = MultiByteToWideChar(CP_UTF8, 0, zText, -1, NULL, 0);
  if( nChar==0 ){
    return 0;
  }
  zWideText = lsmMallocZero(pEnv, nChar * sizeof(WCHAR));
  if( zWideText==0 ){
    return 0;
  }
  nChar = MultiByteToWideChar(CP_UTF8, 0, zText, -1, zWideText, nChar);
  if( nChar==0 ){
    lsmFree(pEnv, zWideText);
    zWideText = 0;
  }
  return zWideText;
}

/*
** Convert a Microsoft Unicode string to UTF-8.
**
** Space to hold the returned string is obtained from lsmMalloc().
*/
static char *win32UnicodeToUtf8(lsm_env *pEnv, LPCWSTR zWideText){
  int nByte;
  char *zText;

  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideText, -1, 0, 0, 0, 0);
  if( nByte == 0 ){
    return 0;
  }
  zText = lsmMallocZero(pEnv, nByte);
  if( zText==0 ){
    return 0;
  }
  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideText, -1, zText, nByte, 0, 0);
  if( nByte == 0 ){
    lsmFree(pEnv, zText);
    zText = 0;
  }
  return zText;
}

#if !defined(win32IsNotFound)
#define win32IsNotFound(a) (((a)==ERROR_FILE_NOT_FOUND)  || \
                            ((a)==ERROR_PATH_NOT_FOUND))
#endif

static int win32Open(
  lsm_env *pEnv,
  const char *zFile,
  int flags,
  LPHANDLE phFile
){
  int rc;
  LPWSTR zConverted;

  zConverted = win32Utf8ToUnicode(pEnv, zFile);
  if( zConverted==0 ){
    rc = LSM_NOMEM_BKPT;
  }else{
    int bReadonly = (flags & LSM_OPEN_READONLY);
    DWORD dwDesiredAccess;
    DWORD dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD dwCreationDisposition;
    DWORD dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
    HANDLE hFile;
    int nRetry = 0;
    if( bReadonly ){
      dwDesiredAccess = GENERIC_READ;
      dwCreationDisposition = OPEN_EXISTING;
    }else{
      dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
      dwCreationDisposition = OPEN_ALWAYS;
    }
    while( (hFile = CreateFileW((LPCWSTR)zConverted,
                                dwDesiredAccess,
                                dwShareMode, NULL,
                                dwCreationDisposition,
                                dwFlagsAndAttributes,
                                NULL))==INVALID_HANDLE_VALUE &&
                                win32RetryIoerr(pEnv, &nRetry) ){
      /* Noop */
    }
    lsmFree(pEnv, zConverted);
    if( hFile!=INVALID_HANDLE_VALUE ){
      *phFile = hFile;
      rc = LSM_OK;
    }else{
      if( win32IsNotFound(GetLastError()) ){
        rc = lsmErrorBkpt(LSM_IOERR_NOENT);
      }else{
        rc = LSM_IOERR_BKPT;
      }
    }
  }
  return rc;
}

static int lsmWin32OsOpen(
  lsm_env *pEnv,
  const char *zFile,
  int flags,
  lsm_file **ppFile
){
  int rc = LSM_OK;
  Win32File *pWin32File;

  pWin32File = lsmMallocZero(pEnv, sizeof(Win32File));
  if( pWin32File==0 ){
    rc = LSM_NOMEM_BKPT;
  }else{
    HANDLE hFile = NULL;

    rc = win32Open(pEnv, zFile, flags, &hFile);
    if( rc==LSM_OK ){
      memset(&pWin32File->sysInfo, 0, sizeof(SYSTEM_INFO));
      GetSystemInfo(&pWin32File->sysInfo);
      pWin32File->pEnv = pEnv;
      pWin32File->zName = zFile;
      pWin32File->hFile = hFile;
    }else{
      lsmFree(pEnv, pWin32File);
      pWin32File = 0;
    }
  }
  *ppFile = (lsm_file *)pWin32File;
  return rc;
}

static int lsmWin32OsWrite(
  lsm_file *pFile, /* File to write to */
  lsm_i64 iOff,    /* Offset to write to */
  void *pData,     /* Write data from this buffer */
  int nData        /* Bytes of data to write */
){
  Win32File *pWin32File = (Win32File *)pFile;
  OVERLAPPED overlapped;  /* The offset for WriteFile. */
  u8 *aRem = (u8 *)pData; /* Data yet to be written */
  int nRem = nData;       /* Number of bytes yet to be written */
  int nRetry = 0;         /* Number of retrys */

  memset(&overlapped, 0, sizeof(OVERLAPPED));
  overlapped.Offset = (LONG)(iOff & 0XFFFFFFFF);
  overlapped.OffsetHigh = (LONG)((iOff>>32) & 0x7FFFFFFF);
  while( nRem>0 ){
    DWORD nWrite = 0; /* Bytes written using WriteFile */
    if( !WriteFile(pWin32File->hFile, aRem, nRem, &nWrite, &overlapped) ){
      if( win32RetryIoerr(pWin32File->pEnv, &nRetry) ) continue;
      break;
    }
    assert( nWrite==0 || nWrite<=(DWORD)nRem );
    if( nWrite==0 || nWrite>(DWORD)nRem ){
      break;
    }
    iOff += nWrite;
    overlapped.Offset = (LONG)(iOff & 0xFFFFFFFF);
    overlapped.OffsetHigh = (LONG)((iOff>>32) & 0x7FFFFFFF);
    aRem += nWrite;
    nRem -= nWrite;
  }
  if( nRem!=0 ) return LSM_IOERR_BKPT;
  return LSM_OK;
}

static int win32Truncate(
  HANDLE hFile,
  lsm_i64 nSize
){
  LARGE_INTEGER offset;
  offset.QuadPart = nSize;
  if( !SetFilePointerEx(hFile, offset, 0, FILE_BEGIN) ){
    return LSM_IOERR_BKPT;
  }
  if (!SetEndOfFile(hFile) ){
    return LSM_IOERR_BKPT;
  }
  return LSM_OK;
}

static int lsmWin32OsTruncate(
  lsm_file *pFile, /* File to write to */
  lsm_i64 nSize    /* Size to truncate file to */
){
  Win32File *pWin32File = (Win32File *)pFile;
  return win32Truncate(pWin32File->hFile, nSize);
}

static int lsmWin32OsRead(
  lsm_file *pFile, /* File to read from */
  lsm_i64 iOff,    /* Offset to read from */
  void *pData,     /* Read data into this buffer */
  int nData        /* Bytes of data to read */
){
  Win32File *pWin32File = (Win32File *)pFile;
  OVERLAPPED overlapped; /* The offset for ReadFile */
  DWORD nRead = 0;       /* Bytes read using ReadFile */
  int nRetry = 0;        /* Number of retrys */

  memset(&overlapped, 0, sizeof(OVERLAPPED));
  overlapped.Offset = (LONG)(iOff & 0XFFFFFFFF);
  overlapped.OffsetHigh = (LONG)((iOff>>32) & 0X7FFFFFFF);
  while( !ReadFile(pWin32File->hFile, pData, nData, &nRead, &overlapped) &&
         GetLastError()!=ERROR_HANDLE_EOF ){
    if( win32RetryIoerr(pWin32File->pEnv, &nRetry) ) continue;
    return LSM_IOERR_BKPT;
  }
  if( nRead<(DWORD)nData ){
    /* Unread parts of the buffer must be zero-filled */
    memset(&((char*)pData)[nRead], 0, nData - nRead);
  }
  return LSM_OK;
}

static int lsmWin32OsSync(lsm_file *pFile){
  int rc = LSM_OK;

#ifndef LSM_NO_SYNC
  Win32File *pWin32File = (Win32File *)pFile;

  if( pWin32File->pMap!=NULL ){
    if( !FlushViewOfFile(pWin32File->pMap, 0) ){
      rc = LSM_IOERR_BKPT;
    }
  }
  if( rc==LSM_OK && !FlushFileBuffers(pWin32File->hFile) ){
    rc = LSM_IOERR_BKPT;
  }
#else
  unused_parameter(pFile);
#endif

  return rc;
}

static int lsmWin32OsSectorSize(lsm_file *pFile){
  return 512;
}

static void win32Unmap(Win32File *pWin32File){
  if( pWin32File->pMap!=NULL ){
    UnmapViewOfFile(pWin32File->pMap);
    pWin32File->pMap = NULL;
    pWin32File->nMap = 0;
  }
  if( pWin32File->hMap!=NULL ){
    CloseHandle(pWin32File->hMap);
    pWin32File->hMap = NULL;
  }
}

static int lsmWin32OsRemap(
  lsm_file *pFile,
  lsm_i64 iMin,
  void **ppOut,
  lsm_i64 *pnOut
){
  Win32File *pWin32File = (Win32File *)pFile;

  /* If the file is between 0 and 2MB in size, extend it in chunks of 256K.
  ** Thereafter, in chunks of 1MB at a time.  */
  const int aIncrSz[] = {256*1024, 1024*1024};
  int nIncrSz = aIncrSz[iMin>(2*1024*1024)];

  *ppOut = NULL;
  *pnOut = 0;

  win32Unmap(pWin32File);
  if( iMin>=0 ){
    LARGE_INTEGER fileSize;
    DWORD dwSizeHigh;
    DWORD dwSizeLow;
    HANDLE hMap;
    LPVOID pMap;
    memset(&fileSize, 0, sizeof(LARGE_INTEGER));
    if( !GetFileSizeEx(pWin32File->hFile, &fileSize) ){
      return LSM_IOERR_BKPT;
    }
    assert( fileSize.QuadPart>=0 );
    if( fileSize.QuadPart<iMin ){
      int rc;
      fileSize.QuadPart = ((iMin + nIncrSz-1) / nIncrSz) * nIncrSz;
      rc = lsmWin32OsTruncate(pFile, fileSize.QuadPart);
      if( rc!=LSM_OK ){
        return rc;
      }
    }
    dwSizeLow = (DWORD)(fileSize.QuadPart & 0xFFFFFFFF);
    dwSizeHigh = (DWORD)((fileSize.QuadPart & 0x7FFFFFFFFFFFFFFF) >> 32);
    hMap = CreateFileMappingW(pWin32File->hFile, NULL, PAGE_READWRITE,
                              dwSizeHigh, dwSizeLow, NULL);
    if( hMap==NULL ){
      return LSM_IOERR_BKPT;
    }
    pWin32File->hMap = hMap;
    assert( fileSize.QuadPart<=0xFFFFFFFF );
    pMap = MapViewOfFile(hMap, FILE_MAP_WRITE | FILE_MAP_READ, 0, 0,
                         (SIZE_T)fileSize.QuadPart);
    if( pMap==NULL ){
      return LSM_IOERR_BKPT;
    }
    pWin32File->pMap = pMap;
    pWin32File->nMap = (SIZE_T)fileSize.QuadPart;
  }
  *ppOut = pWin32File->pMap;
  *pnOut = pWin32File->nMap;
  return LSM_OK;
}

static BOOL win32IsDriveLetterAndColon(
  const char *zPathname
){
  return ( isalpha(zPathname[0]) && zPathname[1]==':' );
}

static int lsmWin32OsFullpath(
  lsm_env *pEnv,
  const char *zName,
  char *zOut,
  int *pnOut
){
  DWORD nByte;
  void *zConverted;
  LPWSTR zTempWide;
  char *zTempUtf8;

  if( zName[0]=='/' && win32IsDriveLetterAndColon(zName+1) ){
    zName++;
  }
  zConverted = win32Utf8ToUnicode(pEnv, zName);
  if( zConverted==0 ){
    return LSM_NOMEM_BKPT;
  }
  nByte = GetFullPathNameW((LPCWSTR)zConverted, 0, 0, 0);
  if( nByte==0 ){
    lsmFree(pEnv, zConverted);
    return LSM_IOERR_BKPT;
  }
  nByte += 3;
  zTempWide = lsmMallocZero(pEnv, nByte * sizeof(zTempWide[0]));
  if( zTempWide==0 ){
    lsmFree(pEnv, zConverted);
    return LSM_NOMEM_BKPT;
  }
  nByte = GetFullPathNameW((LPCWSTR)zConverted, nByte, zTempWide, 0);
  if( nByte==0 ){
    lsmFree(pEnv, zConverted);
    lsmFree(pEnv, zTempWide);
    return LSM_IOERR_BKPT;
  }
  lsmFree(pEnv, zConverted);
  zTempUtf8 = win32UnicodeToUtf8(pEnv, zTempWide);
  lsmFree(pEnv, zTempWide);
  if( zTempUtf8 ){
    int nOut = *pnOut;
    int nLen = strlen(zTempUtf8) + 1;
    if( nLen<=nOut ){
      snprintf(zOut, nOut, "%s", zTempUtf8);
    }
    lsmFree(pEnv, zTempUtf8);
    *pnOut = nLen;
    return LSM_OK;
  }else{
    return LSM_NOMEM_BKPT;
  }
}

static int lsmWin32OsFileid(
  lsm_file *pFile,
  void *pBuf,
  int *pnBuf
){
  int nBuf;
  int nReq;
  u8 *pBuf2 = (u8 *)pBuf;
  Win32File *pWin32File = (Win32File *)pFile;
  BY_HANDLE_FILE_INFORMATION fileInfo;

  nBuf = *pnBuf;
  nReq = (sizeof(fileInfo.dwVolumeSerialNumber) +
          sizeof(fileInfo.nFileIndexHigh) +
          sizeof(fileInfo.nFileIndexLow));
  *pnBuf = nReq;
  if( nReq>nBuf ) return LSM_OK;
  memset(&fileInfo, 0, sizeof(BY_HANDLE_FILE_INFORMATION));
  if( !GetFileInformationByHandle(pWin32File->hFile, &fileInfo) ){
    return LSM_IOERR_BKPT;
  }
  nReq = sizeof(fileInfo.dwVolumeSerialNumber);
  memcpy(pBuf2, &fileInfo.dwVolumeSerialNumber, nReq);
  pBuf2 += nReq;
  nReq = sizeof(fileInfo.nFileIndexHigh);
  memcpy(pBuf, &fileInfo.nFileIndexHigh, nReq);
  pBuf2 += nReq;
  nReq = sizeof(fileInfo.nFileIndexLow);
  memcpy(pBuf2, &fileInfo.nFileIndexLow, nReq);
  return LSM_OK;
}

static int win32Delete(
  lsm_env *pEnv,
  const char *zFile
){
  int rc;
  LPWSTR zConverted;

  zConverted = win32Utf8ToUnicode(pEnv, zFile);
  if( zConverted==0 ){
    rc = LSM_NOMEM_BKPT;
  }else{
    int nRetry = 0;
    DWORD attr;

    do {
      attr = GetFileAttributesW(zConverted);
      if ( attr==INVALID_FILE_ATTRIBUTES ){
        rc = LSM_IOERR_BKPT;
        break;
      }
      if ( attr&FILE_ATTRIBUTE_DIRECTORY ){
        rc = LSM_IOERR_BKPT; /* Files only. */
        break;
      }
      if ( DeleteFileW(zConverted) ){
        rc = LSM_OK; /* Deleted OK. */
        break;
      }
      if ( !win32RetryIoerr(pEnv, &nRetry) ){
        rc = LSM_IOERR_BKPT; /* No more retries. */
        break;
      }
    }while( 1 );
  }
  lsmFree(pEnv, zConverted);
  return rc;
}

static int lsmWin32OsUnlink(lsm_env *pEnv, const char *zFile){
  return win32Delete(pEnv, zFile);
}

#if !defined(win32IsLockBusy)
#define win32IsLockBusy(a) (((a)==ERROR_LOCK_VIOLATION) || \
                            ((a)==ERROR_IO_PENDING))
#endif

static int win32LockFile(
  Win32File *pWin32File,
  int iLock,
  int nLock,
  int eType
){
  OVERLAPPED ovlp;

  assert( LSM_LOCK_UNLOCK==0 );
  assert( LSM_LOCK_SHARED==1 );
  assert( LSM_LOCK_EXCL==2 );
  assert( eType>=LSM_LOCK_UNLOCK && eType<=LSM_LOCK_EXCL );
  assert( nLock>=0 );
  assert( iLock>0 && iLock<=32 );

  memset(&ovlp, 0, sizeof(OVERLAPPED));
  ovlp.Offset = (4096-iLock-nLock+1);
  if( eType>LSM_LOCK_UNLOCK ){
    DWORD flags = LOCKFILE_FAIL_IMMEDIATELY;
    if( eType>=LSM_LOCK_EXCL ) flags |= LOCKFILE_EXCLUSIVE_LOCK;
    if( !LockFileEx(pWin32File->hFile, flags, 0, (DWORD)nLock, 0, &ovlp) ){
      if( win32IsLockBusy(GetLastError()) ){
        return LSM_BUSY;
      }else{
        return LSM_IOERR_BKPT;
      }
    }
  }else{
    if( !UnlockFileEx(pWin32File->hFile, 0, (DWORD)nLock, 0, &ovlp) ){
      return LSM_IOERR_BKPT;
    }
  }
  return LSM_OK;
}

static int lsmWin32OsLock(lsm_file *pFile, int iLock, int eType){
  Win32File *pWin32File = (Win32File *)pFile;
  return win32LockFile(pWin32File, iLock, 1, eType);
}

static int lsmWin32OsTestLock(lsm_file *pFile, int iLock, int nLock, int eType){
  int rc;
  Win32File *pWin32File = (Win32File *)pFile;
  rc = win32LockFile(pWin32File, iLock, nLock, eType);
  if( rc!=LSM_OK ) return rc;
  win32LockFile(pWin32File, iLock, nLock, LSM_LOCK_UNLOCK);
  return LSM_OK;
}

static int lsmWin32OsShmMap(lsm_file *pFile, int iChunk, int sz, void **ppShm){
  int rc;
  Win32File *pWin32File = (Win32File *)pFile;
  int iOffset = iChunk * sz;
  int iOffsetShift = iOffset % pWin32File->sysInfo.dwAllocationGranularity;
  int nNew = iChunk + 1;
  lsm_i64 nReq = nNew * sz;

  *ppShm = NULL;
  assert( sz>=0 );
  assert( sz==LSM_SHM_CHUNK_SIZE );
  if( iChunk>=pWin32File->nShm ){
    LPHANDLE ahNew;
    LPVOID *apNew;
    LARGE_INTEGER fileSize;

    /* If the shared-memory file has not been opened, open it now. */
    if( pWin32File->hShmFile==NULL ){
      char *zShm = win32ShmFile(pWin32File);
      if( !zShm ) return LSM_NOMEM_BKPT;
      rc = win32Open(pWin32File->pEnv, zShm, 0, &pWin32File->hShmFile);
      lsmFree(pWin32File->pEnv, zShm);
      if( rc!=LSM_OK ){
        return rc;
      }
    }

    /* If the shared-memory file is not large enough to contain the
    ** requested chunk, cause it to grow.  */
    memset(&fileSize, 0, sizeof(LARGE_INTEGER));
    if( !GetFileSizeEx(pWin32File->hShmFile, &fileSize) ){
      return LSM_IOERR_BKPT;
    }
    assert( fileSize.QuadPart>=0 );
    if( fileSize.QuadPart<nReq ){
      rc = win32Truncate(pWin32File->hShmFile, nReq);
      if( rc!=LSM_OK ){
        return rc;
      }
    }

    ahNew = (LPHANDLE)lsmMallocZero(pWin32File->pEnv, sizeof(HANDLE) * nNew);
    if( !ahNew ) return LSM_NOMEM_BKPT;
    apNew = (LPVOID *)lsmMallocZero(pWin32File->pEnv, sizeof(LPVOID) * nNew);
    if( !apNew ){
      lsmFree(pWin32File->pEnv, ahNew);
      return LSM_NOMEM_BKPT;
    }
    memcpy(ahNew, pWin32File->ahShm, sizeof(HANDLE) * pWin32File->nShm);
    memcpy(apNew, pWin32File->apShm, sizeof(LPVOID) * pWin32File->nShm);
    lsmFree(pWin32File->pEnv, pWin32File->ahShm);
    pWin32File->ahShm = ahNew;
    lsmFree(pWin32File->pEnv, pWin32File->apShm);
    pWin32File->apShm = apNew;
    pWin32File->nShm = nNew;
  }

  if( pWin32File->ahShm[iChunk]==NULL ){
    HANDLE hMap;
    assert( nReq<=0xFFFFFFFF );
    hMap = CreateFileMappingW(pWin32File->hShmFile, NULL, PAGE_READWRITE, 0,
                              (DWORD)nReq, NULL);
    if( hMap==NULL ){
      return LSM_IOERR_BKPT;
    }
    pWin32File->ahShm[iChunk] = hMap;
  }
  if( pWin32File->apShm[iChunk]==NULL ){
    LPVOID pMap;
    pMap = MapViewOfFile(pWin32File->ahShm[iChunk],
                         FILE_MAP_WRITE | FILE_MAP_READ, 0,
                         iOffset - iOffsetShift, sz + iOffsetShift);
    if( pMap==NULL ){
      return LSM_IOERR_BKPT;
    }
    pWin32File->apShm[iChunk] = pMap;
  }
  if( iOffsetShift!=0 ){
    char *p = (char *)pWin32File->apShm[iChunk];
    *ppShm = (void *)&p[iOffsetShift];
  }else{
    *ppShm = pWin32File->apShm[iChunk];
  }
  return LSM_OK;
}

static void lsmWin32OsShmBarrier(void){
  MemoryBarrier();
}

static int lsmWin32OsShmUnmap(lsm_file *pFile, int bDelete){
  Win32File *pWin32File = (Win32File *)pFile;

  if( pWin32File->hShmFile!=NULL ){
    int i;
    for(i=0; i<pWin32File->nShm; i++){
      if( pWin32File->apShm[i]!=NULL ){
        UnmapViewOfFile(pWin32File->apShm[i]);
        pWin32File->apShm[i] = NULL;
      }
      if( pWin32File->ahShm[i]!=NULL ){
        CloseHandle(pWin32File->ahShm[i]);
        pWin32File->ahShm[i] = NULL;
      }
    }
    CloseHandle(pWin32File->hShmFile);
    pWin32File->hShmFile = NULL;
    if( bDelete ){
      char *zShm = win32ShmFile(pWin32File);
      if( zShm ){ win32Delete(pWin32File->pEnv, zShm); }
      lsmFree(pWin32File->pEnv, zShm);
    }
  }
  return LSM_OK;
}

#define MX_CLOSE_ATTEMPT 3
static int lsmWin32OsClose(lsm_file *pFile){
  int rc;
  int nRetry = 0;
  Win32File *pWin32File = (Win32File *)pFile;
  lsmWin32OsShmUnmap(pFile, 0);
  win32Unmap(pWin32File);
  do{
    if( pWin32File->hFile==NULL ){
      rc = LSM_IOERR_BKPT;
      break;
    }
    rc = CloseHandle(pWin32File->hFile);
    if( rc ){
      pWin32File->hFile = NULL;
      rc = LSM_OK;
      break;
    }
    if( ++nRetry>=MX_CLOSE_ATTEMPT ){
      rc = LSM_IOERR_BKPT;
      break;
    }
  }while( 1 );
  lsmFree(pWin32File->pEnv, pWin32File->ahShm);
  lsmFree(pWin32File->pEnv, pWin32File->apShm);
  lsmFree(pWin32File->pEnv, pWin32File);
  return rc;
}

static int lsmWin32OsSleep(lsm_env *pEnv, int us){
  unused_parameter(pEnv);
  return win32Sleep(us);
}

/****************************************************************************
** Memory allocation routines.
*/

static void *lsmWin32OsMalloc(lsm_env *pEnv, size_t N){
  assert( HeapValidate(GetProcessHeap(), 0, NULL) );
  return HeapAlloc(GetProcessHeap(), 0, (SIZE_T)N);
}

static void lsmWin32OsFree(lsm_env *pEnv, void *p){
  assert( HeapValidate(GetProcessHeap(), 0, NULL) );
  if( p ){
    HeapFree(GetProcessHeap(), 0, p);
  }
}

static void *lsmWin32OsRealloc(lsm_env *pEnv, void *p, size_t N){
  unsigned char *m = (unsigned char *)p;
  assert( HeapValidate(GetProcessHeap(), 0, NULL) );
  if( 1>N ){
    lsmWin32OsFree(pEnv, p);
    return NULL;
  }else if( NULL==p ){
    return lsmWin32OsMalloc(pEnv, N);
  }else{
#if 0 /* arguable: don't shrink */
    SIZE_T sz = HeapSize(GetProcessHeap(), 0, m);
    if( sz>=(SIZE_T)N ){
      return p;
    }
#endif
    return HeapReAlloc(GetProcessHeap(), 0, m, N);
  }
}

static size_t lsmWin32OsMSize(lsm_env *pEnv, void *p){
  assert( HeapValidate(GetProcessHeap(), 0, NULL) );
  return (size_t)HeapSize(GetProcessHeap(), 0, p);
}


#ifdef LSM_MUTEX_WIN32
/*************************************************************************
** Mutex methods for Win32 based systems.  If LSM_MUTEX_WIN32 is
** missing then a no-op implementation of mutexes found below will be
** used instead.
*/
#include "windows.h"

typedef struct Win32Mutex Win32Mutex;
struct Win32Mutex {
  lsm_env *pEnv;
  CRITICAL_SECTION mutex;
#ifdef LSM_DEBUG
  DWORD owner;
#endif
};

#ifndef WIN32_MUTEX_INITIALIZER
# define WIN32_MUTEX_INITIALIZER { 0 }
#endif

#ifdef LSM_DEBUG
# define LSM_WIN32_STATIC_MUTEX { 0, WIN32_MUTEX_INITIALIZER, 0 }
#else
# define LSM_WIN32_STATIC_MUTEX { 0, WIN32_MUTEX_INITIALIZER }
#endif

static int lsmWin32OsMutexStatic(
  lsm_env *pEnv,
  int iMutex,
  lsm_mutex **ppStatic
){
  static volatile LONG initialized = 0;
  static Win32Mutex sMutex[2] = {
    LSM_WIN32_STATIC_MUTEX,
    LSM_WIN32_STATIC_MUTEX
  };

  assert( iMutex==LSM_MUTEX_GLOBAL || iMutex==LSM_MUTEX_HEAP );
  assert( LSM_MUTEX_GLOBAL==1 && LSM_MUTEX_HEAP==2 );

  if( InterlockedCompareExchange(&initialized, 1, 0)==0 ){
    int i;
    for(i=0; i<array_size(sMutex); i++){
      InitializeCriticalSection(&sMutex[i].mutex);
    }
  }
  *ppStatic = (lsm_mutex *)&sMutex[iMutex-1];
  return LSM_OK;
}

static int lsmWin32OsMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  Win32Mutex *pMutex;           /* Pointer to new mutex */

  pMutex = (Win32Mutex *)lsmMallocZero(pEnv, sizeof(Win32Mutex));
  if( !pMutex ) return LSM_NOMEM_BKPT;

  pMutex->pEnv = pEnv;
  InitializeCriticalSection(&pMutex->mutex);

  *ppNew = (lsm_mutex *)pMutex;
  return LSM_OK;
}

static void lsmWin32OsMutexDel(lsm_mutex *p){
  Win32Mutex *pMutex = (Win32Mutex *)p;
  DeleteCriticalSection(&pMutex->mutex);
  lsmFree(pMutex->pEnv, pMutex);
}

static void lsmWin32OsMutexEnter(lsm_mutex *p){
  Win32Mutex *pMutex = (Win32Mutex *)p;
  EnterCriticalSection(&pMutex->mutex);

#ifdef LSM_DEBUG
  assert( pMutex->owner!=GetCurrentThreadId() );
  pMutex->owner = GetCurrentThreadId();
  assert( pMutex->owner==GetCurrentThreadId() );
#endif
}

static int lsmWin32OsMutexTry(lsm_mutex *p){
  BOOL bRet;
  Win32Mutex *pMutex = (Win32Mutex *)p;
  bRet = TryEnterCriticalSection(&pMutex->mutex);
#ifdef LSM_DEBUG
  if( bRet ){
    assert( pMutex->owner!=GetCurrentThreadId() );
    pMutex->owner = GetCurrentThreadId();
    assert( pMutex->owner==GetCurrentThreadId() );
  }
#endif
  return !bRet;
}

static void lsmWin32OsMutexLeave(lsm_mutex *p){
  Win32Mutex *pMutex = (Win32Mutex *)p;
#ifdef LSM_DEBUG
  assert( pMutex->owner==GetCurrentThreadId() );
  pMutex->owner = 0;
  assert( pMutex->owner!=GetCurrentThreadId() );
#endif
  LeaveCriticalSection(&pMutex->mutex);
}

#ifdef LSM_DEBUG
static int lsmWin32OsMutexHeld(lsm_mutex *p){
  Win32Mutex *pMutex = (Win32Mutex *)p;
  return pMutex ? pMutex->owner==GetCurrentThreadId() : 1;
}
static int lsmWin32OsMutexNotHeld(lsm_mutex *p){
  Win32Mutex *pMutex = (Win32Mutex *)p;
  return pMutex ? pMutex->owner!=GetCurrentThreadId() : 1;
}
#endif
/*
** End of Win32 mutex implementation.
*************************************************************************/
#else
/*************************************************************************
** Noop mutex implementation
*/
typedef struct NoopMutex NoopMutex;
struct NoopMutex {
  lsm_env *pEnv;                  /* Environment handle (for xFree()) */
  int bHeld;                      /* True if mutex is held */
  int bStatic;                    /* True for a static mutex */
};
static NoopMutex aStaticNoopMutex[2] = {
  {0, 0, 1},
  {0, 0, 1},
};

static int lsmWin32OsMutexStatic(
  lsm_env *pEnv,
  int iMutex,
  lsm_mutex **ppStatic
){
  assert( iMutex>=1 && iMutex<=(int)array_size(aStaticNoopMutex) );
  *ppStatic = (lsm_mutex *)&aStaticNoopMutex[iMutex-1];
  return LSM_OK;
}
static int lsmWin32OsMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  NoopMutex *p;
  p = (NoopMutex *)lsmMallocZero(pEnv, sizeof(NoopMutex));
  if( p ) p->pEnv = pEnv;
  *ppNew = (lsm_mutex *)p;
  return (p ? LSM_OK : LSM_NOMEM_BKPT);
}
static void lsmWin32OsMutexDel(lsm_mutex *pMutex)  {
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bStatic==0 && p->pEnv );
  lsmFree(p->pEnv, p);
}
static void lsmWin32OsMutexEnter(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
}
static int lsmWin32OsMutexTry(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
  return 0;
}
static void lsmWin32OsMutexLeave(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==1 );
  p->bHeld = 0;
}
#ifdef LSM_DEBUG
static int lsmWin32OsMutexHeld(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  return p ? p->bHeld : 1;
}
static int lsmWin32OsMutexNotHeld(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  return p ? !p->bHeld : 1;
}
#endif
/***************************************************************************/
#endif /* else LSM_MUTEX_NONE */

/* Without LSM_DEBUG, the MutexHeld tests are never called */
#ifndef LSM_DEBUG
# define lsmWin32OsMutexHeld    0
# define lsmWin32OsMutexNotHeld 0
#endif

lsm_env *lsm_default_env(void){
  static lsm_env win32_env = {
    sizeof(lsm_env),         /* nByte */
    1,                       /* iVersion */
    /***** file i/o ******************/
    0,                       /* pVfsCtx */
    lsmWin32OsFullpath,      /* xFullpath */
    lsmWin32OsOpen,          /* xOpen */
    lsmWin32OsRead,          /* xRead */
    lsmWin32OsWrite,         /* xWrite */
    lsmWin32OsTruncate,      /* xTruncate */
    lsmWin32OsSync,          /* xSync */
    lsmWin32OsSectorSize,    /* xSectorSize */
    lsmWin32OsRemap,         /* xRemap */
    lsmWin32OsFileid,        /* xFileid */
    lsmWin32OsClose,         /* xClose */
    lsmWin32OsUnlink,        /* xUnlink */
    lsmWin32OsLock,          /* xLock */
    lsmWin32OsTestLock,      /* xTestLock */
    lsmWin32OsShmMap,        /* xShmMap */
    lsmWin32OsShmBarrier,    /* xShmBarrier */
    lsmWin32OsShmUnmap,      /* xShmUnmap */
    /***** memory allocation *********/
    0,                       /* pMemCtx */
    lsmWin32OsMalloc,        /* xMalloc */
    lsmWin32OsRealloc,       /* xRealloc */
    lsmWin32OsFree,          /* xFree */
    lsmWin32OsMSize,         /* xSize */
    /***** mutexes *********************/
    0,                       /* pMutexCtx */
    lsmWin32OsMutexStatic,   /* xMutexStatic */
    lsmWin32OsMutexNew,      /* xMutexNew */
    lsmWin32OsMutexDel,      /* xMutexDel */
    lsmWin32OsMutexEnter,    /* xMutexEnter */
    lsmWin32OsMutexTry,      /* xMutexTry */
    lsmWin32OsMutexLeave,    /* xMutexLeave */
    lsmWin32OsMutexHeld,     /* xMutexHeld */
    lsmWin32OsMutexNotHeld,  /* xMutexNotHeld */
    /***** other *********************/
    lsmWin32OsSleep,         /* xSleep */
  };
  return &win32_env;
}

#endif


    
#endif /* !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_LSM1) */
