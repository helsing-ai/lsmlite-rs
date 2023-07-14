# lsmlite-rs

Helsing's Rust bindings for `sqlite3`'s `lsm1` extension.

`lsmlite-rs` exposes `sqlite3`'s [lsm1](https://github.com/sqlite/sqlite/tree/master/ext/lsm1) extension in stand-alone fashion (without the whole `sqlite3` stack). This extension is an excellent implementation of Log-structured Merge Trees and is in principle similar in spirit to [RocksDB](https://rocksdb.org/) and [WiredTiger](https://source.wiredtiger.com/) (the storage engine of [MongoDB](https://www.mongodb.com/docs/manual/core/wiredtiger/)). Unlike RocksDB, for example, [lsm1](https://github.com/sqlite/sqlite/tree/master/ext/lsm1) structures data on stable storage as a collection of read-only B-trees (called "segments" in `lsm1`'s terminology) that increase in size as the database grows. Thus, `lsm1` follows the fundamental design principles of [bLSM](https://dl.acm.org/doi/10.1145/2213836.2213862) rather than those of a traditional LSM-Tree - in which data is stored in immutable sorted arrays. This comes with the advantage of offering excellent I/O for reads out-of-the-box; while also being efficient at writing data.

Other appealing characteristics of `lsm1` are:

1. [Permissive license](https://github.com/sqlite/sqlite/blob/master/LICENSE.md).
2. Industrial-grade (robust) implementation.
3. Single-file implementation: `src/lsm1/lsm.c`.
4. Single-file database with single-writer/multiple-reader [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)-based transactional concurrency model.
5. Data durability in the face of application or system failure.
6. Compression and/or encryption can be added.
7. Read-only support. Writes to a database opened in read-only mode are rejected.
8. Optimizations for write-once read-many (`WORM`) workloads. A database becomes essentially a single densely-packed `B`-tree. Improving the space required by the database on disk, as well as providing optimal I/O for reads.

# Main design decisions realized in the current version of these bindings

1. The main memory level of the LSM is currently limited in size to 32 MiB in total.
2. To avoid main memory overheads, no part of the database file is currently memory-mapped.
3. Access to a database file from a different process is currently deactivated. This speeds up operations on the database from different threads - as no shared-memory and POSIX locks are used.
4. Robustness in the presence of a system crash is currently configured to provide a good trade-off between performance and safety. A system crash may not corrupt the database, but recently committed transactions may be lost following recovery. This is `lsm1`'s default durability setting.
5. Background threads (in the relevant operational modes) are actively scheduled after writes to the database are performed. To keep memory usage as well as data safety under control, writes to the database may incur in higher latencies while the corresponding background thread works towards flushing data from main memory to disk (thus reducing main memory consumption) and/or check-pointing volatile data (the data that could be lost during a failure).

# Future work

Relevant work in our roadmap currently includes:

1. More configurability of the engine. Most parameters are currently set within the bindings and are not exposed to (experienced) users for example. Some of those will be exposed as features of the crate.
2. High-performance mode. Currently, resources are used in a very conservative manner e.g., main memory usage and  scheduling of background threads. Read/write performance can be significantly improved by allowing more main memory to be used, as well as scheduling background threads much more aggressively. Also, it is actually possible to have two background threads (additional to the main writing thread) working together, one would take on database file operations like flushing from main memory and merging segments, while the other checkpoints the database file.
3. WebAssembly/JS support (`lsmlite-js`).
4. Python bindings (`lsmlite-py`) using [PyO3](https://github.com/PyO3/pyo3). We are aware of existing python bindings for `lsm1` like [python-lsm-db](https://github.com/coleifer/python-lsm-db), so this has low priority at the moment.
5. Encryption at rest.

# `lsm1` versions

- `lsmlite-rs` version `0.1.0` is based on `lsm1` contained in [sqlite3-3.41.2](https://github.com/sqlite/sqlite/releases/tag/version-3.41.2) released on 22nd of March 2023. Amalgamated `lsm1` file can be found under `src/lsm1/lsm1-ae2e7fc.c` along a short explanation about how this file was produced and how can it be locally updated from the official SQLite3's source code.

# Getting started

The following is a short example on how to declare and open a database, then insert data and traverse the whole database extracting all keys and values currently contained therein. Additional examples on particular topics (e.g., transactions, or compression) can be found under the `examples` directory.

```rust
use lsmlite_rs::*;

// Make sure that `/tmp/my_db.lsm` does not exist yet.
let db_conf = DbConf::new("/tmp/", "my_db".to_string());

// Let's declare an empty handle.
let mut db: LsmDb = Default::default();
// Let's initialize the handle with our configuration.
let rc = db.initialize(db_conf);
// Let's connect to the database. It is at this point that the file is produced.
let rc = db.connect();

// Insert data into the database, so that something gets traversed.
// Let's persist numbers 1 to 100 with 1 KB zeroed payload.
let value = vec![0; 1024];
let max_n: usize = 100;
for n in 1..=max_n {
    let key_serial = n.to_be_bytes();
    let rc = db.persist(&key_serial, &value).unwrap();
}

// Let's open the cursor once we have written data (snapshot isolation).
let mut cursor = db.cursor_open().unwrap();
// Let's move the cursor to the very first record on the database.
let rc = cursor.first();
assert!(rc.is_ok());

// Now let's traverse the database extracting the data we just added.
let mut num_records = 0;
while cursor.valid().is_ok() {
    num_records += 1;
    // Extract the key.
    let current_key = Cursor::get_key(&cursor).unwrap();
    // Parse it to an integer.
    assert!(current_key.len() == 8);
    let key = usize::from_be_bytes(current_key.try_into().unwrap());
    // Extract the value.
    let current_value = Cursor::get_value(&cursor).unwrap();
    // Everything should match.
    assert!(key == num_records);
    assert!(current_value.len() == 1024);
    // Move onto the next record.
    cursor.next().unwrap();
}
// We did find what we wanted.
assert_eq!(num_records, max_n);

// EOF
assert!(cursor.valid().is_err());
```

# How to contribute

We would be happy to hear your thoughts, feedback and pull requests are welcome.