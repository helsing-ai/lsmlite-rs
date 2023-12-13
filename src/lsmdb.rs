// Copyright 2023 Helsing GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr::null_mut;
use std::thread::park_timeout;
use std::time::{Duration, Instant};

use crate::compression::lz4::LsmLz4;
use crate::compression::zlib::LsmZLib;
use crate::compression::zstd::LsmZStd;
use crate::compression::Compression;
use crate::threads::NUM_MERGE_SEGMENTS;
use crate::{
    lsm_cursor, lsm_db, lsm_env, Cursor, DbConf, Disk, LsmBgWorkerMessage, LsmBgWorkers,
    LsmCompressionLib, LsmCursor, LsmCursorSeekOp, LsmDb, LsmErrorCode, LsmHandleMode, LsmInfo,
    LsmMode, LsmParam, LsmSafety,
};

// This is the amount of time a writer sleeps while a background worker does some work.
// It is only relevant when background threads are spawn.
const WRITER_PARK_TIME_MS: u64 = 1; // milliseconds.

// This is the hard main memory limit we keep for the main memory component of the LSM
// (a main-memory b-tree).
pub(crate) const SIZE_MAIN_MEMORY_TREE_KB: i32 = 16 << 10; // X KiBs * 1024 = X MiB

// These are lower and upper bounds on the amount of unchecked data that we can bear
// to lose. Observe that there is also the main memory component that needs to be
// flushed. Thus, at any point in time, there is at most
// (2 * SIZE_MAIN_MEMORY_TREE_KB) + MAX_CHECKPOINT_SIZE_KB worth of data at risk.
pub(crate) const MIN_CHECKPOINT_SIZE_KB: i32 = 2 << 10; // X KiBs * 1024 = X MiB
pub(crate) const MAX_CHECKPOINT_SIZE_KB: i32 = 2 * MIN_CHECKPOINT_SIZE_KB;

// Block size of the database (amount of data to be written as a unit).
pub(crate) const BLOCK_SIZE_KB: i32 = 8 << 10;
// Page size of the database (unit of bytes into which blocks are divided).
pub(crate) const PAGE_SIZE_B: i32 = 4 << 10;

// These functions translate to internal LSM functions. Thus the signatures have
// to match. Observe that we treat LSM's types as opaque, and thus they are passed
// around as memory references that are fully visible inside LSM, but not so
// outside of it.
extern "C" {
    // These functions are the ones we need in other files (like threads.rs).
    pub(crate) fn lsm_info(db: *mut lsm_db, e_conf: i32, ...) -> i32;
    pub(crate) fn lsm_config(db: *mut lsm_db, e_param: i32, ...) -> i32;
    pub(crate) fn lsm_work(db: *mut lsm_db, n_segments: i32, n_kb: i32, p_nwrite: *mut i32) -> i32;
    pub(crate) fn lsm_checkpoint(db: *mut lsm_db, p_n_kb: *mut i32) -> i32;
    pub(crate) fn lsm_new(env: *mut lsm_env, db: *mut *mut lsm_db) -> i32;
    pub(crate) fn lsm_open(db: *mut lsm_db, file_name: *const c_char) -> i32;
    pub(crate) fn lsm_close(db: *mut lsm_db) -> i32;

    // These functions are private to this file.
    fn lsm_insert(
        db: *mut lsm_db,
        p_key: *const u8,
        n_key: i32,
        p_val: *const u8,
        n_val: i32,
    ) -> i32;
    fn lsm_delete(db: *mut lsm_db, p_key: *const u8, n_key: i32) -> i32;
    fn lsm_delete_range(
        db: *mut lsm_db,
        p_key1: *const u8,
        n_key1: i32,
        p_key2: *const u8,
        n_key2: i32,
    ) -> i32;
    fn lsm_begin(db: *mut lsm_db, level: i32) -> i32;
    fn lsm_commit(db: *mut lsm_db, level: i32) -> i32;
    fn lsm_rollback(db: *mut lsm_db, level: i32) -> i32;
    fn lsm_csr_open(db: *mut lsm_db, cursor: *const *mut lsm_cursor) -> i32;
    fn lsm_csr_close(cursor: *mut lsm_cursor) -> i32;
    fn lsm_csr_first(cursor: *mut lsm_cursor) -> i32;
    fn lsm_csr_seek(cursor: *mut lsm_cursor, p_key: *const u8, n_key: i32, e_seek: i32) -> i32;
    fn lsm_csr_last(cursor: *mut lsm_cursor) -> i32;
    fn lsm_csr_next(cursor: *mut lsm_cursor) -> i32;
    fn lsm_csr_prev(cursor: *mut lsm_cursor) -> i32;
    fn lsm_csr_valid(cursor: *mut lsm_cursor) -> i32;
    fn lsm_csr_key(cursor: *mut lsm_cursor, pp_key: *const *mut u8, pn_key: *mut i32) -> i32;
    fn lsm_csr_value(cursor: *mut lsm_cursor, pp_val: *const *mut u8, pn_val: *mut i32) -> i32;
    fn lsm_csr_cmp(cursor: *mut lsm_cursor, p_key: *const u8, n_key: i32, pi_res: *mut i32) -> i32;
    #[allow(dead_code)]
    fn lsm_free(env: *mut lsm_env, ptr: *mut c_char);
}

/// Custom implementation of [`Disk`] for [`LsmDb`].
impl Disk for LsmDb {
    type C<'a> = LsmCursor<'a>;
    /// This function sets up general variables about the database. Initializing
    /// a handle more than once is considered [`LsmErrorCode::LsmMisuse`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_b".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// assert!(!db.is_initialized());
    ///
    /// let rc = db.initialize(db_conf.clone());
    /// assert_eq!(rc, Ok(()));
    /// assert!(db.is_initialized());
    ///
    /// let rc = db.initialize(db_conf);
    /// assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
    /// ```
    fn initialize(&mut self, conf: DbConf) -> Result<(), LsmErrorCode> {
        if self.initialized {
            // If the database handle has already been initialized, we signal
            // trying to initialize it again as an error.
            return Err(LsmErrorCode::LsmMisuse);
        }
        self.db_conf = conf;

        self.db_env = null_mut();
        self.db_handle = null_mut();

        // This is the fully-qualified name of the database.
        let db_fq_name = format!(
            "{}/{}.lsm",
            self.db_conf.db_path.display(),
            self.db_conf.db_base_name
        );
        // This is the c-string version of the name of the database.
        self.db_fq_name = CString::new(db_fq_name).map_err(|e| {
            tracing::error!(?e, "Name of the data base is not a valid c-string.");
            LsmErrorCode::LsmError
        })?;

        self.initialized = true;

        Ok(())
    }

    /// This method produces a main-memory handle to connect to the database. At this point
    /// the database file is created at the given path, and upon success, the database can be
    /// operated using any other available method for it.
    ///
    /// Connecting to a database using the same handle more than once, or connecting using
    /// an uninitialized handle, is considered [`LsmErrorCode::LsmMisuse`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_c".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    ///
    /// let rc = db.connect();
    /// assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
    ///
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    /// assert_eq!(rc, Ok(()));
    /// assert!(db.is_connected());
    ///
    /// let rc = db.connect();
    /// assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
    /// ```
    fn connect(&mut self) -> Result<(), LsmErrorCode> {
        if !self.initialized || self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        // Database has been initialized, thus we can proceed.
        let mut rc: i32;
        let mut db_handle = null_mut();
        unsafe {
            // Get a new handle to connect to the database on disk.
            rc = lsm_new(null_mut(), &mut db_handle);
        }

        if rc != 0 {
            // The only error that can occur is memory allocation. Thus,
            // if we fail while allocating, we do not have to deallocate anything.
            return Err(LsmErrorCode::try_from(rc)?);
        }

        self.db_handle = db_handle;

        // In here we configure parameters of the database. These parameters
        // are a good approximation in general. On bigger machines we would
        // consume more main-memory. We differentiate between opening in read-only mode
        // or not, as in read-only we need no extra threads or write-oriented parameters.
        unsafe {
            // These parameters are independent of the mode we open the file in.

            // Disable multi-process support to improve performance
            // (no OS advisory locks are used to synchronize access
            // to the database file).
            let multi_process: i32 = 0;
            rc = lsm_config(
                self.db_handle,
                LsmParam::MultipleProcesses as i32,
                &multi_process,
            );

            if rc != 0 {
                self.disconnect()?;
                return Err(LsmErrorCode::try_from(rc)?);
            }

            // These are our default parameters of any handle (whether it writes
            // to the database or not).

            // Maximum size of a main-memory tree before it can be marked as old.
            let autoflush: i32 = SIZE_MAIN_MEMORY_TREE_KB;
            rc = lsm_config(self.db_handle, LsmParam::AutoFlush as i32, &autoflush);

            if rc != 0 {
                self.disconnect()?;
                return Err(LsmErrorCode::try_from(rc)?);
            }

            rc = lsm_config(self.db_handle, LsmParam::PageSize as i32, &PAGE_SIZE_B);

            if rc != 0 {
                self.disconnect()?;
                return Err(LsmErrorCode::try_from(rc)?);
            }

            rc = lsm_config(self.db_handle, LsmParam::BlockSize as i32, &BLOCK_SIZE_KB);

            if rc != 0 {
                self.disconnect()?;
                return Err(LsmErrorCode::try_from(rc)?);
            }

            // How much of the file is kept in memory.
            let mmap_size: i32 = 0;
            rc = lsm_config(self.db_handle, LsmParam::Mmap as i32, &mmap_size);

            if rc != 0 {
                self.disconnect()?;
                return Err(LsmErrorCode::try_from(rc)?);
            }

            let safety: i32 = LsmSafety::Normal as i32;
            rc = lsm_config(self.db_handle, LsmParam::Safety as i32, &safety);

            if rc != 0 {
                self.disconnect()?;
                return Err(LsmErrorCode::try_from(rc)?);
            }

            if self.db_conf.handle_mode == LsmHandleMode::ReadOnly {
                // Here are parameters set that are only relevant in read-only mode.
                // Observe that this overwrites the mode the handle operates in,
                // as only reads are performed.
                let read_only_int = 1;
                rc = lsm_config(self.db_handle, LsmParam::ReadOnly as i32, &read_only_int);

                if rc != 0 {
                    self.disconnect()?;
                    return Err(LsmErrorCode::try_from(rc)?);
                }

                // When opening in read-only mode, we ignore the mode provided by the user
                // as no background threads (operating on the file) are needed.
                self.db_conf.mode = LsmMode::LsmNoBackgroundThreads;
            }

            // If we are instructed to configure compression, we do it now. Setting the
            // compression can be done only when the database was created, once set,
            // trying to do it again or not setting it to the same compression scheme
            // will be considered an error (LsmErrorCode::LsmMismatch).
            match self.db_conf.compression {
                LsmCompressionLib::NoCompression => {}
                LsmCompressionLib::LZ4 => {
                    let lz4 = LsmLz4::new();
                    self.db_compress = lz4.get_compression_methods().ok();
                }
                LsmCompressionLib::ZLib => {
                    let zlib = LsmZLib::new();
                    self.db_compress = zlib.get_compression_methods().ok();
                }
                LsmCompressionLib::ZStd => {
                    let zstd = LsmZStd::new();
                    self.db_compress = zstd.get_compression_methods().ok();
                }
            };

            // Only if the compression library is defined we pass it onto
            // the engine. Otherwise no compression whatsoever.
            if let Some(lsm_compress) = self.db_compress.as_ref() {
                rc = lsm_config(
                    self.db_handle,
                    LsmParam::SetCompression as i32,
                    lsm_compress,
                );

                if rc != 0 {
                    self.disconnect()?;
                    return Err(LsmErrorCode::try_from(rc)?);
                }
            }

            rc = lsm_open(self.db_handle, self.db_fq_name.as_ptr());

            if rc != 0 {
                self.disconnect()?;
                return Err(LsmErrorCode::try_from(rc)?);
            }

            self.connected = true;

            // Whether we spawn background threads is at this point properly set.
            // Currently we spawn only one background thread at most, and thus its
            // id is set to 0. This has to be executed after we have connected
            // to the database.
            self.configure_bg_threads(self.db_conf.mode, 0)?;
        }

        // We output the current parameters of the writer.
        unsafe {
            let auto_flush: i32 = -1;
            let _ = lsm_config(self.db_handle, LsmParam::AutoFlush as i32, &auto_flush);

            let page_size_b: i32 = -1;
            let _ = lsm_config(self.db_handle, LsmParam::PageSize as i32, &page_size_b);

            let block_size_kb: i32 = -1;
            let _ = lsm_config(self.db_handle, LsmParam::BlockSize as i32, &block_size_kb);

            let auto_checkpoint_kb: i32 = -1;
            let _ = lsm_config(
                self.db_handle,
                LsmParam::AutoCheckPoint as i32,
                &auto_checkpoint_kb,
            );

            let auto_work: i32 = -1;
            let _ = lsm_config(self.db_handle, LsmParam::AutoWork as i32, &auto_work);

            let multi_process: i32 = -1;
            let _ = lsm_config(
                self.db_handle,
                LsmParam::MultipleProcesses as i32,
                &multi_process,
            );

            let read_only: i32 = -1;
            let _ = lsm_config(self.db_handle, LsmParam::ReadOnly as i32, &read_only);

            let mmap_size: i32 = -1;
            let _ = lsm_config(self.db_handle, LsmParam::Mmap as i32, &mmap_size);

            let safety: i32 = -1;
            let _ = lsm_config(self.db_handle, LsmParam::Safety as i32, &safety);

            tracing::info!(
                auto_flush = format!("{auto_flush} KBs"),
                page_size = format!("{page_size_b} Bs"),
                block_size = format!("{block_size_kb} KBs"),
                auto_checkpoint = format!("{auto_checkpoint_kb} KBs"),
                auto_work = if auto_work != 0 { "yes" } else { "no" },
                multi_process = if multi_process != 0 { "yes" } else { "no" },
                read_only = if read_only != 0 { "yes" } else { "no" },
                background_threads = if self.db_conf.mode != LsmMode::LsmNoBackgroundThreads {
                    "yes"
                } else {
                    "no"
                },
                mmap_overhead = format!("{mmap_size} KBs"),
                compression = ?self.db_conf.compression,
                safety = if safety == 0 { "None" } else if safety == 1 { "Normal" } else { "Full" },
                "lsmlite-rs parameters.",
            );
        }

        // If we get through, then everything is fine.
        Ok(())
    }

    /// This method frees up all the resources used by the main-memory handle. A call
    /// to `connect` has to have the corresponding call to `disconnect`, otherwise:
    /// 1. The resources that belong to the memory handle will be leaked.
    /// 2. The database (file) won't be closed and the next time we open it the recovery
    /// process will kick-in (which can take considerable time. Depending on the
    /// size of the log).
    ///
    /// For completeness, [`LsmDb`] also implements [`Drop`] so that a handle
    /// gets automatically released once it goes out of scope.
    ///
    /// Disconnecting using an uninitialized handle, or a handle that is not yet connected
    /// is not considered an error.
    ///
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_d".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.disconnect();
    /// assert_eq!(rc, Ok(()));
    ///
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    /// let rc = db.disconnect();
    /// assert_eq!(rc, Ok(()));
    /// assert!(!db.is_connected());
    /// ```
    fn disconnect(&mut self) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Ok(());
        }

        // First, we explicitly shutdown all background threads (it might take some time
        // depending on what they are doing). In this manner, they will all flush their
        // data to disk.
        if self.db_conf.mode != LsmMode::LsmNoBackgroundThreads {
            self.db_bg_threads.shutdown();
        }

        // We now proceed to close the database and destroy all allocated resources of the handle.
        let rc: i32;
        unsafe {
            rc = lsm_close(self.db_handle);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        // We reset the pointer once we know we were able to cleanly close the database.
        self.db_handle = null_mut();
        self.connected = false;

        // If we get through, then everything is fine.
        Ok(())
    }

    /// This function writes the given entry on the database file in a transactional
    /// manner. That is, it either writes it completely, or not, but it leaves the
    /// database in no inconsistent state.
    ///
    /// Trying to persist data using an uninitialized handle, or one that is not yet
    /// connected to a database, is considered [`LsmErrorCode::LsmMisuse`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    /// use rand::{thread_rng, Rng};
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_e".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    ///
    /// let mut prng = thread_rng();
    /// // 16-byte random key (not very useful in practice).
    /// let key: Vec<u8> = (0..16).map(|_| prng.gen_range(0..=255)).collect();
    /// // 1 KB zeroed payload.
    /// let value = vec![0; 1024];
    ///
    /// let rc = db.persist(&key, &value);
    /// assert_eq!(rc, Ok(()));
    ///
    /// // This is also possible (would overwrite the entry)
    /// let rc = Disk::persist(&mut db, &key, &value);
    /// assert_eq!(rc, Ok(()));
    ///
    /// let rc = db.disconnect();
    /// ```
    fn persist(&mut self, key: &[u8], value: &[u8]) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let start = Instant::now();
        let serial_key = key;
        let serial_key_len = serial_key.len() as i32;
        let serial_blob = value;
        let serial_blob_len = serial_blob.len() as i32;
        let rc: i32;

        unsafe {
            // If we have background threads, we have to synchronize to avoid running
            // out of memory. That is, we cannot write to the database until we make sure that
            // we haven't exceeded the resources we are told (main memory for example).
            self.deal_with_bg_threads()?;

            rc = lsm_insert(
                self.db_handle,
                serial_key.as_ptr(),
                serial_key_len,
                serial_blob.as_ptr(),
                serial_blob_len,
            );
            if rc != 0 {
                return Err(LsmErrorCode::try_from(rc)?);
            }
        }

        let current_request_duration = Instant::now()
            .checked_duration_since(start)
            .unwrap_or_default();
        match &self.db_conf.metrics {
            None => {}
            Some(metrics) => metrics
                .write_times_s
                .observe(current_request_duration.as_secs_f64()),
        }
        Ok(())
    }

    /// This function is just sugar. The database file can be considered a primary
    /// index in which only one entry under a given key can exist. If another entry
    /// with an existing key is persisted, it overwrites the existing one.
    fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), LsmErrorCode> {
        Disk::persist(self, key, value)
    }

    /// This deletes the entry under the given key (in a transactional manner).
    ///
    /// Trying to delete data using an uninitialized handle, or one that is not yet
    /// connected to a database, is considered [`LsmErrorCode::LsmMisuse`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    /// use rand::{thread_rng, Rng};
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_f".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    ///
    /// let mut prng = thread_rng();
    /// // 16-byte random key (not very useful in practice).
    /// let key: Vec<u8> = (0..16).map(|_| prng.gen_range(0..=255)).collect();
    /// // 1 KB zeroed payload.
    /// let value = vec![0; 1024];
    ///
    /// let rc = db.persist(&key, &value);
    /// assert_eq!(rc, Ok(()));
    ///
    /// // Entry under `key` will disappear.
    /// let rc = db.delete(&key);
    /// assert_eq!(rc, Ok(()));
    ///
    /// let rc = db.disconnect();
    /// ```
    fn delete(&mut self, key: &[u8]) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        let key_len = key.len();
        let key_ptr = key.as_ptr();
        unsafe { rc = lsm_delete(self.db_handle, key_ptr, key_len as i32) }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// This function deletes the open interval of keys (being, end) (in a transactional
    /// manner as well).
    fn delete_range(&mut self, begin: &[u8], end: &[u8]) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        let starting_key_len = begin.len();
        let starting_key_ptr = begin.as_ptr();
        let ending_key_len = end.len();
        let ending_key_ptr = end.as_ptr();
        unsafe {
            rc = lsm_delete_range(
                self.db_handle,
                starting_key_ptr,
                starting_key_len as i32,
                ending_key_ptr,
                ending_key_len as i32,
            )
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// This function optimizes a database to make it occupy as little space as possible.
    /// Essentially, this function compacts the whole database into a single tightly-packed
    /// B-tree: Thus, read I/O is optimized.
    /// This function is thought to be used in an offline fashion - once
    /// all writers have finished with the database.
    fn optimize(&mut self) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        unsafe {
            // Let's work all the way through.
            rc = lsm_work(self.db_handle, 1, -1, null_mut());

            // Anything different than ok (0), or busy (5) is wrong!
            if rc != 0 && rc != 5 {
                lsm_close(self.db_handle);
                let ec = LsmErrorCode::try_from(rc)?;
                tracing::error!(
                    datafile = self.get_full_db_path()?,
                    rc  = ?ec,
                    "Error occurred while working on the datafile. Exiting background thread.",
                );
            }
        }

        Ok(())
    }

    /// This function opens a transaction explicitly. All operations contained between
    /// opening a transaction and committing it using [`Disk::commit_transaction`] will be
    /// performed atomically. Similarly, if the transaction is explicitly rolled back using
    /// [`Disk::rollback_transaction`], all enclosed operations will not be persistent.
    /// Observe that every database operation is contained in an implicit transaction. This
    /// function is thought to encapsulate multiple operations into a single transaction.
    fn begin_transaction(&mut self) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        // TODO: For the time being we accept only a single
        // transaction, no nested ones.
        let rc: i32;
        unsafe {
            rc = lsm_begin(self.db_handle, 1);
        }

        // A transaction is easy to deal with, it's either successfully
        // opened or not.
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// This function commits an opened transaction. Without committing a transaction,
    /// all enclosed operations will remain hidden from the consistent state of the database.
    fn commit_transaction(&mut self) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        // TODO: For the time being we do not support nested
        // transactions, but we try to commit all open ones anyway.
        let rc: i32;
        unsafe {
            rc = lsm_commit(self.db_handle, 0);
        }

        // The commit either succeeds or not.
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// This function rollbacks an opened transaction explicitly. All enclosed
    /// operations will remain hidden from the consistent state of the database.
    fn rollback_transaction(&mut self) -> Result<(), LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        // TODO: For the time being we do not support nested
        // transactions, thus we simply rollback and close the top-level
        // transaction.
        let rc: i32;
        unsafe {
            rc = lsm_rollback(self.db_handle, 0);
        }

        // The rollback should succeed, otherwise there are pretty bad
        // issues down the pipeline.
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// This function returns a cursor to the underlying database.
    /// This cursor can be operated by the methods provided by the [`Cursor`] trait.
    /// When opening a cursor, a snapshot of the database will be created for it. No
    /// new data arriving after the cursor has been created will be visible to the
    /// cursor. A cursor is used to performed read-only operations over the database.
    /// That is, no data of the database can be modified through a cursor.
    ///
    /// Trying to open a cursor using a uninitialized handle, or one that is not yet
    /// connected to a database, is considered [`LsmErrorCode::LsmMisuse`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_g".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    ///
    /// // Opening a cursor for `db`. This cursor is currently
    /// // not positioned anywhere, and thus no data can be extracted
    /// // from it.
    /// let cursor = db.cursor_open();
    /// assert!(cursor.is_ok());
    /// ```
    fn cursor_open(&self) -> Result<LsmCursor, LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let cursor: *mut lsm_cursor = null_mut();
        let rc: i32;
        unsafe {
            rc = lsm_csr_open(self.db_handle, &cursor);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }
        Ok(LsmCursor {
            db_cursor: cursor,
            _marker: Default::default(),
        })
    }
}

/// Custom implementation of [`Cursor`] for [`LsmCursor`].
impl Cursor for LsmCursor<'_> {
    /// This function closes an existing cursor over the underlying
    /// database. A call to [`Disk::cursor_open`] must be paired up (in the end)
    /// with a call to [`Cursor::close`]. Otherwise the database won't be cleanly
    /// closed (due to the snapshot that belongs to the cursor), and
    /// a recovery process will be spawn the next time the database
    /// file is opened.
    ///
    /// For completeness, [`LsmCursor`] also implements [`Drop`] so that a cursor
    /// gets automatically released once it goes out of scope (thus releasing
    /// resources).
    ///
    /// Closing an uninitialized [`LsmCursor`] is considered [`LsmErrorCode::LsmMisuse`].
    fn close(&mut self) -> Result<(), LsmErrorCode> {
        if self.db_cursor.is_null() {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        // Free resources.
        unsafe {
            // This call is infallible, it always returns Ok.
            rc = lsm_csr_close(self.db_cursor);
            // From here on, this cursor is not useful any longer.
            self.db_cursor = null_mut();
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// Tests whether the cursor is currently pointing to a valid database record.
    /// When operating a cursor, this function has to be called before extracting
    /// records from it (key and/or value) to make sure that the values can be
    /// trusted. That is, a cursor might internally retain the last output value
    /// for a while after it has become invalid (say moved past the end of the
    /// database), or empty values can be extracted from it before positioning
    /// the cursor on a valid record.
    ///
    /// Testing for validity of an uninitialized uninitialized [`LsmCursor`] is
    /// considered [`LsmErrorCode::LsmMisuse`].
    fn valid(&self) -> Result<(), LsmErrorCode> {
        if self.db_cursor.is_null() {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let mut rc: i32;
        unsafe {
            rc = lsm_csr_valid(self.db_cursor);
            // Internally, lsm_csr_valid returns true (== 1) when valid,
            // but LsmOk == 0, thus we exchange the value to represent
            // true as LsmOk, and false as LsmError.
            rc = 1 - rc;
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// Moves the cursor to the very first record in the database.
    /// Positioning an uninitialized [`LsmCursor`] is considered [`LsmErrorCode::LsmMisuse`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_h".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    ///
    /// // Insert data into the database, so that something gets traversed.
    /// let key: usize = 1;
    /// let key_serial = key.to_be_bytes();
    /// // 1 KB zeroed payload.
    /// let value = vec![0; 1024];
    /// let rc = db.persist(&key_serial, &value)?;
    ///
    /// let key: usize = 2;
    /// let key_serial = key.to_be_bytes();
    /// let rc = db.persist(&key_serial, &value)?;
    ///
    /// let mut cursor = db.cursor_open()?;
    ///
    /// let rc = cursor.first();
    /// assert!(rc.is_ok());
    ///
    /// let mut num_records = 0;
    /// while cursor.valid().is_ok() {
    ///     num_records += 1;
    ///     let current_key = Cursor::get_key(&cursor)?;
    ///     let current_value = Cursor::get_value(&cursor)?;
    ///     cursor.next()?;
    /// }
    /// assert_eq!(num_records, 2);
    ///
    /// // EOF
    /// assert!(cursor.valid().is_err());
    ///
    /// # Result::<(), LsmErrorCode>::Ok(())
    /// ```
    fn first(&mut self) -> Result<(), LsmErrorCode> {
        if self.db_cursor.is_null() {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        unsafe {
            rc = lsm_csr_first(self.db_cursor);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// Moves the cursor to the very last record in the database.
    /// Positioning an uninitialized [`LsmCursor`] is considered [`LsmErrorCode::LsmMisuse`].
    /// # Example
    ///
    /// ```rust
    /// use lsmlite_rs::*;
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_i".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    ///
    /// // Insert data into the database, so that something gets traversed.
    /// let key: usize = 1;
    /// let key_serial = key.to_be_bytes();
    /// // 1 KB zeroed payload.
    /// let value = vec![0; 1024];
    /// let rc = db.persist(&key_serial, &value)?;
    ///
    /// let key: usize = 2;
    /// let key_serial = key.to_be_bytes();
    /// let rc = db.persist(&key_serial, &value)?;
    ///
    /// let mut cursor = db.cursor_open()?;
    ///
    /// let rc = cursor.last();
    /// assert!(rc.is_ok());
    ///
    /// let mut num_records = 0;
    /// while cursor.valid().is_ok() {
    ///     num_records += 1;
    ///     let current_key = Cursor::get_key(&cursor)?;
    ///     let current_value = Cursor::get_value(&cursor)?;
    ///     cursor.prev()?;
    /// }
    /// assert_eq!(num_records, 2);
    ///
    /// // EOF
    /// assert!(cursor.valid().is_err());
    ///
    /// # Result::<(), LsmErrorCode>::Ok(())
    /// ```
    fn last(&mut self) -> Result<(), LsmErrorCode> {
        if self.db_cursor.is_null() {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        unsafe {
            rc = lsm_csr_last(self.db_cursor);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// This positions the cursor on an entry of the database that depends on
    /// the seek mode provided:
    /// 1. If [`LsmCursorSeekOp::LsmCursorSeekLe`] is given, then the cursor will be positioned
    /// at the entry that is less or equal than the provided key depending on whether
    /// the key is found in the database or not.
    /// 2. If [`LsmCursorSeekOp::LsmCursorSeekEq`] is given, then the cursor will be positioned
    /// at the entry that corresponding to the given key, or at the end of the database
    /// depending on whether the entry is found or not. If the entry is found, a call
    /// to `valid` on the cursor will return success, and otherwise an error.
    /// 3. If [`LsmCursorSeekOp::LsmCursorSeekGe`] is given, then the cursor will be positioned
    /// at the entry that is greater or equal than the provided key depending on
    /// whether the key is found in the database or not.
    fn seek(&mut self, key: &[u8], mode: LsmCursorSeekOp) -> Result<(), LsmErrorCode> {
        if self.db_cursor.is_null() {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        let key_len = key.len();
        let key_ptr = key.as_ptr();
        unsafe {
            rc = lsm_csr_seek(self.db_cursor, key_ptr, key_len as i32, mode as i32);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// Once a cursor is position at a valid entry, this function moves it to the next
    /// entry. This function can be called only when moving forward on the database. That is,
    /// when starting from [`Cursor::first`] or when seeking with [`LsmCursorSeekOp::LsmCursorSeekGe`].
    /// Otherwise an error will be issued.
    fn next(&mut self) -> Result<(), LsmErrorCode> {
        if self.db_cursor.is_null() {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        unsafe {
            rc = lsm_csr_next(self.db_cursor);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// Similar to [`Cursor::next`], but moving to the previous entry. This function can
    /// be called only when moving backwards on the database. That is, when starting from
    /// [`Cursor::last`] or when seeking with [`LsmCursorSeekOp::LsmCursorSeekLe`]. Otherwise an
    /// error will be issued.
    fn prev(&mut self) -> Result<(), LsmErrorCode> {
        if self.db_cursor.is_null() {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let rc: i32;
        unsafe {
            rc = lsm_csr_prev(self.db_cursor);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(())
    }

    /// If the cursor is [`Cursor::valid`], then this function retrieves the key of
    /// the entry the cursor is currently pointing to. The memory the key uses
    /// belongs to the parent call. If the cursor is not valid, an error is returned.
    fn get_key(&self) -> Result<Vec<u8>, LsmErrorCode> {
        self.valid()?;

        let rc: i32;
        let key_ptr: *mut u8 = null_mut();
        let mut key_len: i32 = 0;
        let mut key: Vec<u8> = vec![];
        unsafe {
            rc = lsm_csr_key(self.db_cursor, &key_ptr, &mut key_len);
            if rc != 0 {
                return Err(LsmErrorCode::try_from(rc)?);
            }
            // We reserve enough space so that we can copy what the cursor returned.
            key.reserve(key_len as usize);
            // We copy the returned value onto new memory that the upper call
            // will own.
            key_ptr.copy_to_nonoverlapping(key.as_mut_ptr(), key_len as usize);
            key.set_len(key_len as usize);
        }
        // This memory belongs now to the upper layer.
        Ok(key)
    }

    /// If the cursor is [`Cursor::valid`], then this function retrieves the value
    /// of the entry the cursor is currently pointing to. The memory the key uses
    /// belongs to the parent call. If the cursor is not valid, an error is returned.
    fn get_value(&self) -> Result<Vec<u8>, LsmErrorCode> {
        self.valid()?;

        let rc: i32;
        let value_ptr: *mut u8 = null_mut();
        let mut value_len: i32 = 0;
        let mut value: Vec<u8> = vec![];
        unsafe {
            rc = lsm_csr_value(self.db_cursor, &value_ptr, &mut value_len);
            if rc != 0 {
                return Err(LsmErrorCode::try_from(rc)?);
            }
            // We reserve enough space so that we can copy what the cursor returned.
            value.reserve(value_len as usize);
            // We copy the returned value onto new memory that the upper call
            // will own.
            value_ptr.copy_to_nonoverlapping(value.as_mut_ptr(), value_len as usize);
            value.set_len(value_len as usize);
        }
        // This memory belongs now to the upper layer.
        Ok(value)
    }

    /// If the cursor is [`Cursor::valid`], then this function compares the key of the
    /// entry the cursor is currently pointing to, with the given key. On success, the
    /// result of the comparison is returned. The comparison happens as per
    /// `memcmp`, that is, if the cursor's key is [`Ordering::Less`], [`Ordering::Equal`],
    /// or [`Ordering::Greater`] than the provided key, then the corresponding [`Ordering`]
    /// will be returned. On prefix comparison, that is, the given key is a strict prefix of
    /// the cursor key, [`Ordering::Greater`] will be returned.
    ///
    /// This function is useful when probing the database for a range.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::cmp::Ordering;
    /// use lsmlite_rs::*;
    ///
    /// let db_conf = DbConf::new(
    ///                           "/tmp/",
    ///                           "my_db_j".to_string(),
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.initialize(db_conf);
    /// let rc = db.connect();
    ///
    /// // Insert data into the database, so that something gets traversed.
    /// let key: usize = 1;
    /// let key_serial = key.to_be_bytes();
    /// // 1 KB zeroed payload.
    /// let value = vec![0; 1024];
    /// let rc = db.persist(&key_serial, &value)?;
    ///
    /// let key: usize = 2;
    /// let key_serial = key.to_be_bytes();
    /// let rc = db.persist(&key_serial, &value)?;
    ///
    /// let mut cursor = db.cursor_open()?;
    ///
    /// let rc = cursor.first();
    /// assert!(rc.is_ok());
    ///
    /// // Assume the very first record is smaller than this.
    /// let key_ub_value: usize = 2;
    /// let key_ub_serial = key_ub_value.to_be_bytes();
    /// // `Ordering::Less` tells that the key of the cursor is smaller
    /// // than `key_ub_value`.
    /// let mut key_cmp = cursor.compare(&key_ub_serial)?;
    ///
    /// let mut num_records = 0;
    /// while cursor.valid().is_ok() && key_cmp < Ordering::Equal {
    ///    num_records += 1;
    ///    cursor.next()?;
    ///    key_cmp = cursor.compare(&key_ub_serial)?;
    /// }
    ///
    /// assert_eq!(num_records, 1);
    ///
    /// // We either exhausted the database or found a key >= than `key_ub_value`.
    /// # Result::<(), LsmErrorCode>::Ok(())
    /// ```
    fn compare(&self, key: &[u8]) -> Result<Ordering, LsmErrorCode> {
        self.valid()?;

        let rc: i32;
        let mut result: i32 = 0;
        let zero: i32 = 0;
        let key_len = key.len();
        let key_ptr = key.as_ptr();
        unsafe {
            rc = lsm_csr_cmp(self.db_cursor, key_ptr, key_len as i32, &mut result);
        }
        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        Ok(result.cmp(&zero))
    }
}

/// Additional to implementing [`Disk`], the following helper functions are also available.
impl LsmDb {
    fn configure_bg_threads(&mut self, mode: LsmMode, id: usize) -> Result<(), LsmErrorCode> {
        let rc: i32;
        match mode {
            LsmMode::LsmNoBackgroundThreads => {
                // In single-threaded mode we do nothing but to set parameters of the connection.
                // These parameters are a good approximation in general. On bigger machines we would
                // want to consume more main-memory for example.
                unsafe {
                    // Modifying auto checkpointing, as a single thread will handle all operations.
                    let checkpoint_size: i32 = MAX_CHECKPOINT_SIZE_KB;
                    rc = lsm_config(
                        self.db_handle,
                        LsmParam::AutoCheckPoint as i32,
                        &checkpoint_size,
                    );

                    if rc != 0 {
                        return Err(LsmErrorCode::try_from(rc)?);
                    }
                }
            }
            // If a single extra thread should handle work and checkpoint, then
            // we signal this.
            LsmMode::LsmBackgroundMerger => {
                // We now initialize the thread that will take care of working and checkpointing.
                self.db_bg_threads = LsmBgWorkers::new(&self.db_conf, &self.db_fq_name, id);
                // If the background thread was issued, then we output this information.
                if self.db_bg_threads.bg_threads[0].thread.is_some() {
                    // Disable auto work, which will be delegated to a thread. The main
                    // writer won't take care of this.
                    let auto_work: i32 = 0;
                    unsafe {
                        rc = lsm_config(self.db_handle, LsmParam::AutoWork as i32, &auto_work);
                    }

                    if rc != 0 {
                        // Let's destroy the background threads.
                        self.db_bg_threads.shutdown();
                        return Err(LsmErrorCode::try_from(rc)?);
                    }

                    // All good, go ahead and inform.
                    tracing::info!(
                        datafile = self.get_full_db_path()?,
                        "Combined merger and check-pointer thread scheduled.",
                    );
                }

                // If the background thread was not issued (due to internal errors)
                // we change no property of the main connection to avoid problems.
            }
            LsmMode::LsmBackgroundCheckpointer => {
                // We first initialize the thread that will take care of checkpointing.
                self.db_bg_threads = LsmBgWorkers::new(&self.db_conf, &self.db_fq_name, id);
                // If the background thread was issued, then we output this information.
                if self.db_bg_threads.bg_threads[0].thread.is_some() {
                    // Disable auto checkpointing, which will be delegated to a thread. The main
                    // writer won't take care of this.
                    let auto_checkpoint: i32 = 0;
                    unsafe {
                        rc = lsm_config(
                            self.db_handle,
                            LsmParam::AutoCheckPoint as i32,
                            &auto_checkpoint,
                        );
                    }

                    if rc != 0 {
                        // Let's destroy the background threads.
                        self.db_bg_threads.shutdown();
                        return Err(LsmErrorCode::try_from(rc)?);
                    }

                    // All good, go ahead and inform.
                    tracing::info!(
                        datafile = self.get_full_db_path()?,
                        "Check-pointer thread scheduled.",
                    );
                }

                // If the background thread was not issued (due to internal errors)
                // we change no property of the main connection to avoid problems.
            }
        }
        Ok(())
    }

    fn deal_with_bg_threads(&mut self) -> Result<(), LsmErrorCode> {
        match self.db_conf.mode {
            LsmMode::LsmNoBackgroundThreads => {}
            LsmMode::LsmBackgroundMerger => {
                // We register the time it takes for the merger to work.
                let start = Instant::now();
                self.wait_on_merger()?;
                let current_request_duration = Instant::now()
                    .checked_duration_since(start)
                    .unwrap_or_default();
                match &self.db_conf.metrics {
                    None => {}
                    Some(metrics) => metrics
                        .work_times_s
                        .observe(current_request_duration.as_secs_f64()),
                }
            }
            LsmMode::LsmBackgroundCheckpointer => {
                // We register the time it takes for the checkpointer to work.
                let start = Instant::now();
                self.wait_on_checkpointer()?;
                let current_request_duration = Instant::now()
                    .checked_duration_since(start)
                    .unwrap_or_default();
                match &self.db_conf.metrics {
                    None => {}
                    Some(metrics) => metrics
                        .checkpoint_times_s
                        .observe(current_request_duration.as_secs_f64()),
                }
            }
        }
        Ok(())
    }

    fn wait_on_merger(&mut self) -> Result<(), LsmErrorCode> {
        let mut rc: i32;
        let mut old_tree_size: i32 = -1;
        let mut new_tree_size: i32 = -1;

        // Since the database is single-writer, we can safely query
        // the current sizes of the main memory structures (trees)
        // to decide how to proceed. Observe that this is done
        // only in the case that multiple threads are used.
        // Otherwise, LSM does this internally.
        unsafe {
            rc = lsm_info(
                self.db_handle,
                LsmInfo::LsmTreeSize as i32,
                &mut old_tree_size,
                &mut new_tree_size,
            );
        }

        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        let mut written_kb: i32 = 0;
        let mut overall_written_kb = 0;
        let work_kb: i32 = 128;
        // We perform work until we have enough space in main memory to keep writing.
        // This is to avoid running over the amount of main memory allowed to use.
        while old_tree_size > 0 {
            unsafe {
                rc = lsm_work(self.db_handle, NUM_MERGE_SEGMENTS, work_kb, &mut written_kb);
                overall_written_kb += written_kb;

                // Anything different than ok (0), or busy (5) is wrong!
                if rc != 0 && rc != 5 {
                    lsm_close(self.db_handle);
                    let ec = LsmErrorCode::try_from(rc);
                    tracing::error!(
                            datafile = ?self.get_full_db_path(),
                            rc = ?ec,
                        "Error occurred while working on the datafile. No work performed \
                        on the database.",
                    );
                    return Err(LsmErrorCode::try_from(rc)?);
                }

                // After having performed some work, we query the sizes of the
                // main-memory components to decide whether another iteration will be done or not.
                rc = lsm_info(
                    self.db_handle,
                    LsmInfo::LsmTreeSize as i32,
                    &mut old_tree_size,
                    &mut new_tree_size,
                );

                // Something went wrong!
                if rc != 0 {
                    let ec = LsmErrorCode::try_from(rc);
                    tracing::error!(
                        datafile = ?self.get_full_db_path(),
                        rc = ?ec,
                        "Error occurred while obtaining segment information for background thread. \
                        Exiting background thread.",
                    );
                    return Err(LsmErrorCode::try_from(rc)?);
                }

                park_timeout(Duration::from_millis(WRITER_PARK_TIME_MS));
            }
        }

        // We update the metric on the amount of data written.
        match &self.db_conf.metrics {
            None => {}
            Some(metrics) => metrics.work_kbs.observe(overall_written_kb as f64),
        }

        // Should hold.
        debug_assert!(old_tree_size == 0);

        // Background thread will now take care of file operations in the background,
        // but main thread will be allowed to write to memory as well.
        self.db_bg_threads.execute(LsmBgWorkerMessage::Merge)?;

        Ok(())
    }

    fn wait_on_checkpointer(&mut self) -> Result<(), LsmErrorCode> {
        let mut rc: i32;
        let mut amount_volatile_data: i32 = -1;
        let writer_park_time_ms = Duration::from_millis(WRITER_PARK_TIME_MS);

        // Since the database is single-writer, we can safely query
        // the current sizes of the main memory structures (trees)
        // to decide how to proceed. Observe that this is done
        // only in the case that multiple threads are used.
        // Otherwise, LSM does this internally.
        unsafe {
            rc = lsm_info(
                self.db_handle,
                LsmInfo::LsmCheckpointSize as i32,
                &mut amount_volatile_data,
            );
        }

        if rc != 0 {
            return Err(LsmErrorCode::try_from(rc)?);
        }

        // If a checkpoint is due, then we wake up the background thread.
        if amount_volatile_data >= MAX_CHECKPOINT_SIZE_KB {
            // This asks the background thread to checkpoint the data file (needed at this point).
            self.db_bg_threads.execute(LsmBgWorkerMessage::Checkpoint)?;

            // Once the message has been sent, we wait for the background thread to
            // finish before returning control to the upper layer.
            // To avoid busy waits we yield for a little bit in every iteration.
            while amount_volatile_data >= MAX_CHECKPOINT_SIZE_KB {
                // TODO: We currently assume that the background thread is running
                // doing stuff. If the background thread dies, then we have to see how we
                // proceed (re-spawning the thread most probably to not interfere with the
                // existing writer connection).
                park_timeout(writer_park_time_ms);

                unsafe {
                    rc = lsm_info(
                        self.db_handle,
                        LsmInfo::LsmCheckpointSize as i32,
                        &mut amount_volatile_data,
                    );
                }

                if rc != 0 {
                    return Err(LsmErrorCode::try_from(rc)?);
                }
            }
        }

        // If this condition holds, we can keep "safely" writing to the database.
        debug_assert!(amount_volatile_data < MAX_CHECKPOINT_SIZE_KB);

        Ok(())
    }

    /// This function tests whether a database handle has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// This function tests whether a database handle is connected.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// This function outputs the full-qualified path of the database.
    /// It errors if the database has not been initialized.
    pub fn get_full_db_path(&self) -> Result<String, LsmErrorCode> {
        if !self.initialized {
            return Err(LsmErrorCode::LsmMisuse);
        }
        Ok(String::from_utf8_lossy(self.db_fq_name.as_bytes()).to_string())
    }

    /// This function outputs the compression id of the database. The only possible
    /// error is [`LsmErrorCode::LsmMismatch`] which means that records of the database
    /// have been compressed with a unknown library. At this point there is not much
    /// to do, and this error should be considered unrecoverable. That is, the database
    /// can be considered corrupted, and its data is most probably lost.
    pub fn get_compression_id(&self) -> Result<LsmCompressionLib, LsmErrorCode> {
        if !self.initialized || !self.connected {
            return Err(LsmErrorCode::LsmMisuse);
        }

        let compression_id: i32 = -1;
        unsafe {
            let _ = lsm_info(
                self.db_handle,
                LsmInfo::LsmCompressionId as i32,
                &compression_id,
            );
        }
        LsmCompressionLib::try_from(compression_id)
    }
}

/// A default database. This database is not useful without
/// getting first initialized using [`Disk::initialize`]. The purpose
/// of this method is to simply zero all attributes of [`LsmDb`].
impl Default for LsmDb {
    fn default() -> Self {
        Self {
            db_env: null_mut(),
            db_handle: null_mut(),
            db_compress: None,
            db_fq_name: Default::default(),
            db_conf: Default::default(),
            db_bg_threads: Default::default(),
            initialized: false,
            connected: false,
        }
    }
}

/// A default cursor. This cursor is not useful by itself as it is not
/// bound to any database. This construction is provided to be used in cases
/// in which a cursor needs to be declared ahead of time, only to be later
/// assigned a cursor bound to a database.
impl Default for LsmCursor<'_> {
    fn default() -> Self {
        Self {
            db_cursor: null_mut(),
            _marker: Default::default(),
        }
    }
}

/// Drop for [`LsmDb`] so that it gets properly terminated when it goes out of scope for example.
impl Drop for LsmDb {
    fn drop(&mut self) {
        // Now we close the data file so that it does not require recovery next time we open it.
        // We might fail thou. If we fail, for whatever reason, the handle will be leaked, but since
        // this is `Drop`, we have no way to signal this to the upper layer.
        let rc = self.disconnect();
        if rc == Err(LsmErrorCode::LsmMisuse) {
            tracing::warn!(
                ?rc,
                "Database could not be closed. Most probably there are still cursors accessing it. \
                A recovery procedure will be required next time the database is accessed. \
                DB handle has not been destroyed and is still fully functional.",
            );
        } else if rc != Ok(()) {
            tracing::error!(
                ?rc,
                "Database could not be closed. Unexpected error happened. \
                Resources occupied by the handle will be most probably leaked.",
            );
        }
    }
}

/// Drop for `LsmCursor` so that it gets properly terminated when it goes out of scope for example.
impl Drop for LsmCursor<'_> {
    fn drop(&mut self) {
        // We simply close the cursor (thus releasing resources occupied by it like
        // snapshot(s) and memory.
        let _ = self.close();
    }
}

/// A database handle is marked as [`Send`] as it can be safely sent to another
/// thread (for further usage), for example in async code.
unsafe impl Send for LsmDb {}
/// For convenience a database handle is marked as [`Sync`]. This is not because the same
/// handle can be safely shared among threads, but because in this manner a handle can be
/// wrapped in a [`std::sync::RwLock`] and be shared among threads safely as it captures
/// the single-writer, multiple-reader nature of a [`LsmDb`]. In this manner, multiple
/// cursors may be opened through the same handle, while writes through the handle
/// are exclusive (serialized).
unsafe impl Sync for LsmDb {}
