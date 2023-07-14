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
#![doc = include_str!("../README.md")]
/*!
# Documentation

This documentation contains examples that show how to create a database, as well as how read from it and write to it. Primary operations (reads and writes) over a database file are done via a lightweight set of methods described by traits  [`Disk`] and [`Cursor`]. These traits are implemented by [`LsmDb`] and [`LsmCursor`]. For database configuration and creation, the relevant information can be found under [`DbConf`] and [`LsmDb`].
*/

// Private mods.
mod compression;
mod lsmdb;
mod threads;

use crate::compression::lsm_compress;
use prometheus::Histogram;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ffi::CString;
use std::marker::{PhantomData, PhantomPinned};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

/// This struct contains the configuration of a database.
#[derive(Clone, Debug, Default)]
pub struct DbConf {
    pub(crate) db_path: PathBuf,
    pub(crate) db_base_name: String,
    pub(crate) handle_mode: LsmHandleMode,
    pub(crate) mode: LsmMode,
    pub(crate) metrics: Option<LsmMetrics>,
    pub(crate) compression: LsmCompressionLib,
}

impl DbConf {
    /// The minimum amount of information a database requires is
    /// the path the file will be put in as well as the name of it.
    /// This version of `new` will create a database using
    /// no background threads, without Prometheus metrics, and using
    /// no compression.
    pub fn new(db_path: impl Into<PathBuf>, db_name: String) -> Self {
        Self {
            db_path: db_path.into(),
            db_base_name: db_name,
            ..Default::default()
        }
    }

    /// A full database configuration requires:
    /// 1. The path the database file will be put in.
    /// 2. The name of the database file.
    /// 3. The mode it will work on (single- or multi-threaded).
    /// 4. Whether the database is to be used in read-only mode, or writes
    /// are also allowed.
    /// 5. The upper-layer set of (Prometheus) metrics to be updated by
    /// the database.
    /// 6. Whether compression is to be used or not, and what kind.
    ///
    /// # Example
    ///
    /// ```rust
    /// use prometheus::{Histogram, HistogramOpts};
    /// use lsmlite_rs::*;
    ///
    /// let default_checkpointer_kbs_buckets: Vec::<f64> =
    /// vec![2048., 4096., 5120., 6144., 7168., 8192., 12288., 16384., 20480.];
    /// let default_worker_kbs_buckets: Vec::<f64> =
    /// vec![1024., 2048., 4096., 8192., 16384., 24576., 32768., 49152., 65536.];
    /// let default_write_times_sec_buckets: Vec::<f64> =
    /// vec![0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.5, 1.0, 5.0, 10.];
    ///
    /// let opts_1 = HistogramOpts::new(
    ///                                 "non_typed_write_times_s",
    ///                                 "non_typed_write_times_s help"
    ///                                )
    ///                                .buckets(default_write_times_sec_buckets.clone());
    /// let opts_2 = HistogramOpts::new(
    ///                                 "work_kbs",
    ///                                 "work_kbs help"
    ///                                )
    ///                                 .buckets(default_worker_kbs_buckets.clone());
    /// let opts_3 = HistogramOpts::new(
    ///                                 "work_times_s",
    ///                                 "work_times_s help"
    ///                                 )
    ///                                 .buckets(default_write_times_sec_buckets.clone());
    /// let opts_4 = HistogramOpts::new(
    ///                                 "checkpoint_kbs",
    ///                                 "checkpoint_kbs help"
    ///                                 )
    ///                                 .buckets(default_checkpointer_kbs_buckets.clone());
    /// let opts_5 = HistogramOpts::new(
    ///                                 "checkpoint_times_s",
    ///                                 "checkpoint_times_s help"
    ///                                )
    ///                                .buckets(default_write_times_sec_buckets);
    ///
    /// let metrics = LsmMetrics {
    ///     write_times_s: Histogram::with_opts(opts_1).unwrap(),
    ///     work_kbs: Histogram::with_opts(opts_2).unwrap(),
    ///     work_times_s: Histogram::with_opts(opts_3).unwrap(),
    ///     checkpoint_kbs: Histogram::with_opts(opts_4).unwrap(),
    ///     checkpoint_times_s: Histogram::with_opts(opts_5).unwrap(),
    /// };
    ///
    /// let db_conf = DbConf::new_with_parameters(
    ///                                           "/tmp/",
    ///                                           "my_db_z".to_string(),
    ///                                           LsmMode::LsmNoBackgroundThreads,
    ///                                           LsmHandleMode::ReadWrite,
    ///                                           Some(metrics),
    ///                                           LsmCompressionLib::ZLib,
    /// );
    ///
    /// let mut db: LsmDb = Default::default();
    /// let rc = db.initialize(db_conf)?;
    /// # Result::<(), LsmErrorCode>::Ok(())
    /// ```
    pub fn new_with_parameters(
        db_path: impl Into<PathBuf>,
        db_name: String,
        mode: LsmMode,
        handle_mode: LsmHandleMode,
        metrics: Option<LsmMetrics>,
        compression: LsmCompressionLib,
    ) -> Self {
        Self {
            db_path: db_path.into(),
            db_base_name: db_name,
            handle_mode,
            mode,
            metrics,
            compression,
        }
    }
}

/// These are stubs that mirror LSM's types. They are define like this to
/// provide type safety only on the Rust side. Their body at runtime is
/// the one from LSM.
#[repr(C)]
#[derive(Copy, Clone)]
struct lsm_db {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct lsm_env {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct lsm_cursor {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

/// These are the different signals we can send to a background thread.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub(crate) enum LsmBgWorkerMessage {
    Checkpoint = 0,
    Merge,
    Stop,
}

#[derive(Debug)]
pub(crate) struct LsmBgWorker {
    pub(crate) thread: Option<thread::JoinHandle<()>>,
}

// This is the thread pool that will contain our worker threads.
#[derive(Debug, Default)]
pub(crate) struct LsmBgWorkers {
    pub(crate) bg_threads: Vec<LsmBgWorker>,
    pub(crate) sender: Option<mpsc::SyncSender<LsmBgWorkerMessage>>,
    pub(crate) id: usize,
}

/// This represents the main-memory handle to a database file, most operations
/// on the database are performed through the corresponding handle, like writing
/// to it, or opening cursors, or handling explicit transactions.
pub struct LsmDb {
    pub(crate) db_env: *mut lsm_env,
    pub(crate) db_handle: *mut lsm_db,
    pub(crate) db_fq_name: CString,
    pub(crate) db_conf: DbConf,
    pub(crate) db_bg_threads: LsmBgWorkers,
    pub(crate) db_compress: Option<lsm_compress>,
    pub(crate) initialized: bool,
    pub(crate) connected: bool,
}

/// This is the main cursor structure.
pub struct LsmCursor<'a> {
    pub(crate) db_cursor: *mut lsm_cursor,
    _marker: PhantomData<&'a ()>,
}

/// These are the metrics exposed by the engine. This metrics are
/// Prometheus histograms, see <https://docs.rs/prometheus/latest/prometheus/struct.Histogram.html>.
#[derive(Clone, Debug)]
pub struct LsmMetrics {
    /// Histogram of the time it takes to write to the database file.
    /// Due to internal database file operations, and depending on the size
    /// of the database, some write operations might have latencies in the seconds.
    /// Since this histogram is produced external to the engine, it is suggested
    /// to capture a well-space set of intervals with precision from milliseconds
    /// to up to, say, 10 seconds. The storage engine updates this histogram with
    /// precision in seconds.
    pub write_times_s: Histogram,
    /// Histogram of the amount of data (in KBs) written during merge operations
    /// and flushing of in-memory data into the database file. This histogram is
    /// only updated in [`LsmMode::LsmBackgroundMerger`] mode.
    /// As before, a well-space set of intervals with precision between say
    /// 512 KBs and 32 MBs is recommended.
    pub work_kbs: Histogram,
    /// Histogram of the time it takes to perform database file operations like merging
    /// segments and flushing in-memory data. As before, this histogram is updated with
    /// precision in seconds. This histogram is only updated in
    /// [`LsmMode::LsmBackgroundMerger`] mode.
    pub work_times_s: Histogram,
    /// Histogram of the amount of data (in KBs) written during a checkpoint operation.
    /// This histogram is only updated in [`LsmMode::LsmBackgroundCheckpointer`] mode. As
    /// before, a well-space set of intervals with precision between 1 and, say, 32 MB is
    /// recommended.
    pub checkpoint_kbs: Histogram,
    /// Histogram of the time it takes to perform checkpointing operations on the database
    /// file. This histogram is only updated in [`LsmMode::LsmBackgroundCheckpointer`]
    /// mode and with precision in seconds.
    pub checkpoint_times_s: Histogram,
}

/// Whether a database handle operates in read-only mode or not. By default,
/// reads and writes are allowed in a database.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum LsmHandleMode {
    /// Attempts to write to the database will be rejected in this mode.
    ReadOnly = 0,
    /// This is the default mode in which reads and writes to the database
    /// are allowed.
    #[default]
    ReadWrite,
}

/// These are the different seek operations of a cursor.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LsmCursorSeekOp {
    /// When seeking with a [`LsmCursor`] using this mode, the cursor
    /// is positioned on the record indexed by the given key if found, or the
    /// record right before it in the total order of keys (as per `memcmp`).
    /// If the database contains no such record, the cursor is left at EOF.
    LsmCursorSeekLe = -1,
    /// When seeking with a [`LsmCursor`] using the mode, the cursor
    /// is positioned on the record indexed by the given key if found, or
    /// at EOF if such record does not exist in the database.
    LsmCursorSeekEq,
    /// When seeking with a [`LsmCursor`] using this mode, the cursor
    /// is positioned on the record indexed by the given key if found, or the
    /// record right after it in the total order of keys (as per `memcmp`).
    /// If the database contains no such record, the cursor is left at EOF.
    LsmCursorSeekGe,
}

/// These are the different kind of errors that we can encounter.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LsmErrorCode {
    LsmError = 1,
    LsmBusy = 5,
    LsmNoMem = 7,
    LsmReadOnly = 8,
    LsmIOErr = 10,
    LsmCorrupt = 11,
    LsmFull = 13,
    LsmCantOpen = 14,
    LsmProtocol = 15,
    LsmMisuse = 21,
    LsmMismatch = 50,
    LsmConversionErr = 55,
    LsmMetricCreation = 56,
    LsmMetricRegistration = 57,
    LsmMetricsEmpty = 58,
    LsmBgThreadUnavailable = 59,
    LsmUnknownCode = 60,
}

/// These are the different levels of safety that the engine can offer.
/// For most cases, `LsmSafetyNormal` should be enough. The more
/// restrictive, the slower the engine performs.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum LsmSafety {
    /// Do not sync to disk at all. This is the fastest mode.
    /// If a power failure occurs while writing to the database,
    /// following recovery the database may be corrupt. All or some
    /// data may be recoverable.
    Off = 0,
    /// Sync only as much as is necessary to prevent database corruption.
    /// This is the default setting. Although slower than [`LsmSafety::Off`],
    /// this mode is still much faster than [`LsmSafety::Full`].
    #[default]
    Normal = 1,
    /// Sync every transaction to disk as part of committing it. This is the
    /// slowest mode. If a power failure occurs while writing to the database,
    /// all successfully committed transactions should be present. The database
    /// file should not be corrupt.
    Full = 2,
}

/// These are the different kinds of execution modes that the engine can
/// work on. The default mode is `LsmNoBackgroundThreads` - in which no
/// background thread is scheduled to perform any task. All file operations
/// are performed by the thread that is currently writing by the database.
///
/// The other available modes are `LsmBackgroundMerger` in which a background
/// thread is scheduled to merge database segments towards producing larger
/// segments. This background thread is also in charge of checkpointing the
/// database file - thus making data truly persistent on stable storage - and
/// flushing in-memory data into the database file.
///
/// In mode `LsmBackgroundMerger`, the additional background thread only
/// checkpoints the database file. In this mode, merge operations are performed
/// by the thread that also writes data to the database.
///
/// In any case, file operations (like merging and checkpointing) cannot be
/// scheduled, these operations are triggered depending on the current state
/// of the database.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LsmMode {
    /// Default mode. No additional background thread is scheduled.
    #[default]
    LsmNoBackgroundThreads = 0,
    /// An additional background thread is scheduled to merge segments
    /// and to checkpoint the database file (update file headers and sync
    /// the contents of the database file to disk). This thread is also
    /// in charge of flushing in-memory data to the database file.
    LsmBackgroundMerger,
    /// An additional background thread is scheduled to checkpoint the
    /// database file. Merge operations are not performed by this thread,
    /// but by the thread that also writes data to the database.
    LsmBackgroundCheckpointer,
}

/// These are the current supported compression libraries. A comparison of the
/// performance of different compression libraries (compression ratio and throughput
/// can be found here: <https://github.com/lz4/lz4>. Support for more compression
/// libraries like, `Snappy`, may be added in the future.
#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum LsmCompressionLib {
    /// By default, no compression is performed.
    #[default]
    NoCompression = 1,
    /// Uses `LZ4` to compress data pages.
    LZ4 = 10001,
    /// Uses `Zlib` to compress data pages.
    ZLib,
    /// Uses `ZStd` to compress data pages.
    ZStd,
}

/// These are parameters that impact the behaviour of the engine.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum LsmParam {
    AutoFlush = 1,
    PageSize = 2,
    Safety = 3,
    BlockSize = 4,
    AutoWork = 5,
    Mmap = 7,
    UseLog = 8,
    AutoMerge = 9,
    MaxFreeList = 10,
    MultipleProcesses = 11,
    AutoCheckPoint = 12,
    SetCompression = 13,
    GetCompression = 14,
    SetCompressionFactory = 15,
    ReadOnly = 16,
}

// This enum is most probably only relevant in this file. Thus we won't expose it to
// the public for the time being.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum LsmInfo {
    Lsm4KbPagesWritten = 1,
    Lsm4KbPagesRead = 2,
    LsmDbStructure = 3,
    LsmLogStructure = 4,
    LsmPageDumpAscii = 6,
    LsmPageDumpHex = 7,
    LsmCheckpointSize = 10,
    LsmTreeSize = 11,
    LsmCompressionId = 13,
}

// This is the simplest implementation of the std::error:Error trait
// on LsmErrorCode to ease error handling (e.g. using anyhow).
impl std::error::Error for LsmErrorCode {}

// Display trait is needed when implementing Error
impl std::fmt::Display for LsmErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Recycle Debug implementation
        write!(f, "{self:?}")
    }
}

/// The following `try_from`s are to be able to parse an integer to the
/// corresponding enum (one of the above).
impl TryFrom<i32> for LsmErrorCode {
    type Error = LsmErrorCode;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(LsmErrorCode::LsmError),
            5 => Ok(LsmErrorCode::LsmBusy),
            7 => Ok(LsmErrorCode::LsmNoMem),
            8 => Ok(LsmErrorCode::LsmReadOnly),
            10 => Ok(LsmErrorCode::LsmIOErr),
            11 => Ok(LsmErrorCode::LsmCorrupt),
            13 => Ok(LsmErrorCode::LsmFull),
            14 => Ok(LsmErrorCode::LsmCantOpen),
            15 => Ok(LsmErrorCode::LsmProtocol),
            21 => Ok(LsmErrorCode::LsmMisuse),
            50 => Ok(LsmErrorCode::LsmMismatch),
            55 => Ok(LsmErrorCode::LsmConversionErr),
            56 => Ok(LsmErrorCode::LsmMetricCreation),
            57 => Ok(LsmErrorCode::LsmMetricRegistration),
            58 => Ok(LsmErrorCode::LsmMetricsEmpty),
            59 => Ok(LsmErrorCode::LsmBgThreadUnavailable),
            _ => Err(LsmErrorCode::LsmUnknownCode),
        }
    }
}

impl TryFrom<i32> for LsmCursorSeekOp {
    type Error = LsmErrorCode;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            -1 => Ok(LsmCursorSeekOp::LsmCursorSeekLe),
            0 => Ok(LsmCursorSeekOp::LsmCursorSeekEq),
            1 => Ok(LsmCursorSeekOp::LsmCursorSeekGe),
            _ => Err(LsmErrorCode::LsmUnknownCode),
        }
    }
}

impl TryFrom<i32> for LsmSafety {
    type Error = LsmErrorCode;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(LsmSafety::Off),
            1 => Ok(LsmSafety::Normal),
            2 => Ok(LsmSafety::Full),
            _ => Err(LsmErrorCode::LsmUnknownCode),
        }
    }
}

impl TryFrom<i32> for LsmParam {
    type Error = LsmErrorCode;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(LsmParam::AutoFlush),
            2 => Ok(LsmParam::PageSize),
            3 => Ok(LsmParam::Safety),
            4 => Ok(LsmParam::BlockSize),
            5 => Ok(LsmParam::AutoWork),
            7 => Ok(LsmParam::Mmap),
            8 => Ok(LsmParam::UseLog),
            9 => Ok(LsmParam::AutoMerge),
            10 => Ok(LsmParam::MaxFreeList),
            11 => Ok(LsmParam::MultipleProcesses),
            12 => Ok(LsmParam::AutoCheckPoint),
            13 => Ok(LsmParam::SetCompression),
            14 => Ok(LsmParam::GetCompression),
            15 => Ok(LsmParam::SetCompressionFactory),
            16 => Ok(LsmParam::ReadOnly),
            _ => Err(LsmErrorCode::LsmUnknownCode),
        }
    }
}

impl TryFrom<i32> for LsmMode {
    type Error = LsmErrorCode;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(LsmMode::LsmNoBackgroundThreads),
            1 => Ok(LsmMode::LsmBackgroundMerger),
            2 => Ok(LsmMode::LsmBackgroundCheckpointer),
            _ => Err(LsmErrorCode::LsmUnknownCode),
        }
    }
}

impl TryFrom<i32> for LsmCompressionLib {
    type Error = LsmErrorCode;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(LsmCompressionLib::NoCompression),
            10001 => Ok(LsmCompressionLib::LZ4),
            10002 => Ok(LsmCompressionLib::ZLib),
            10003 => Ok(LsmCompressionLib::ZStd),
            _ => Err(LsmErrorCode::LsmMismatch),
        }
    }
}

impl TryFrom<i32> for LsmInfo {
    type Error = LsmErrorCode;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(LsmInfo::Lsm4KbPagesWritten),
            2 => Ok(LsmInfo::Lsm4KbPagesRead),
            3 => Ok(LsmInfo::LsmDbStructure),
            4 => Ok(LsmInfo::LsmLogStructure),
            6 => Ok(LsmInfo::LsmPageDumpAscii),
            7 => Ok(LsmInfo::LsmPageDumpHex),
            10 => Ok(LsmInfo::LsmCheckpointSize),
            11 => Ok(LsmInfo::LsmTreeSize),
            13 => Ok(LsmInfo::LsmCompressionId),
            _ => Err(LsmErrorCode::LsmUnknownCode),
        }
    }
}

/// Primary set of methods of a database.
pub trait Disk
where
    for<'a> Self: 'a,
{
    type C<'a>: Cursor;
    /// Initializes a database with a given configuration (of type [`DbConf`]).
    fn initialize(&mut self, conf: DbConf) -> Result<(), LsmErrorCode>;
    /// Connects to a database on disk.
    fn connect(&mut self) -> Result<(), LsmErrorCode>;
    /// Disconnects from a database on disk.
    fn disconnect(&mut self) -> Result<(), LsmErrorCode>;
    /// Persists a given `value` under the given `key`. Observe that both fields
    /// are given as a set of bytes.
    fn persist(&mut self, key: &[u8], value: &[u8]) -> Result<(), LsmErrorCode>;
    /// Updates the value stored under `key` to the given `value`.
    fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), LsmErrorCode>;
    /// Deletes the value stored under `key`.
    fn delete(&mut self, key: &[u8]) -> Result<(), LsmErrorCode>;
    /// Deletes all stored values in the open interval (begin, end).
    fn delete_range(&mut self, begin: &[u8], end: &[u8]) -> Result<(), LsmErrorCode>;
    /// Optimizes the database in certain way. This is implementation-defined. For
    /// example, currently `lsmlite-rs` optimizes the database for write-once, read-many
    /// workloads (for space and read efficiency). Other underlying implementations
    /// might have different optimization opportunities.
    fn optimize(&mut self) -> Result<(), LsmErrorCode>;
    /// Opens a transaction explicitly.
    fn begin_transaction(&mut self) -> Result<(), LsmErrorCode>;
    /// Commits the operations contained in the current transaction.
    fn commit_transaction(&mut self) -> Result<(), LsmErrorCode>;
    /// Rollbacks the operations contained in the current transaction.
    fn rollback_transaction(&mut self) -> Result<(), LsmErrorCode>;
    /// Opens a database cursor.
    fn cursor_open(&self) -> Result<Self::C<'_>, LsmErrorCode>;
}

/// Primary set of methods of database cursors.
pub trait Cursor {
    /// Closes a database cursor.
    fn close(&mut self) -> Result<(), LsmErrorCode>;
    /// Moves the cursor to the very first record in the database.
    fn first(&mut self) -> Result<(), LsmErrorCode>;
    /// Moves the cursor to the very last record in the database.
    fn last(&mut self) -> Result<(), LsmErrorCode>;
    /// Moves the cursor in the database to the record pointed by `key`.
    /// How this operation behaves depends on the given seek mode ([`LsmCursorSeekOp`]).
    fn seek(&mut self, key: &[u8], mode: LsmCursorSeekOp) -> Result<(), LsmErrorCode>;
    /// Moves the cursor to the next record in the database (as seen from the
    /// current value the cursor is pointing to).
    fn next(&mut self) -> Result<(), LsmErrorCode>;
    /// Moves the cursor to the previous record in the database (as seen from the
    /// current value the cursor is pointing to).
    fn prev(&mut self) -> Result<(), LsmErrorCode>;
    /// Tests whether the cursor is currently pointing to a valid database record.
    fn valid(&self) -> Result<(), LsmErrorCode>;
    /// Obtains a copy of the key of the record the cursor is currently pointing
    /// to (if valid).
    fn get_key(&self) -> Result<Vec<u8>, LsmErrorCode>;
    /// Obtains a copy of the value of the record the cursor is currently pointing
    /// to (if valid).
    fn get_value(&self) -> Result<Vec<u8>, LsmErrorCode>;
    /// Compares the key the cursor is currently pointing to (if valid) with the
    /// given `key` (as per `memcmp`). The result of the comparison (`< 0, == 0, > 0`)
    /// is returned.
    fn compare(&self, key: &[u8]) -> Result<Ordering, LsmErrorCode>;
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::ops::Not;
    use std::path::Path;
    use std::thread;

    use crate::{
        Cursor, DbConf, Disk, LsmCompressionLib, LsmCursorSeekOp, LsmDb, LsmErrorCode,
        LsmHandleMode, LsmInfo, LsmMetrics, LsmMode, LsmParam, LsmSafety,
    };

    use chrono::Utc;
    use prometheus::{Histogram, HistogramOpts, DEFAULT_BUCKETS};
    use prost::Message;
    use rand::Rng;
    use rand::SeedableRng;
    use rand_mt::{Mt19937GenRand64, Mt64};

    // To test the strongly typed api we need a message type.
    #[derive(Message)]
    pub struct TypedBlob {
        #[prost(bytes = "vec", tag = "1")]
        pub(crate) payload: Vec<u8>,
    }

    fn construct_random_blob(bytes: usize, prng: &mut Mt19937GenRand64) -> Vec<u8> {
        // Produce a random vector of the given number of bytes.
        let random_vector: Vec<u8> = (0..bytes).map(|_| prng.gen_range(0..=255)).collect();

        random_vector
    }

    fn construct_compressible_blob(bytes: usize) -> Vec<u8> {
        vec![0; bytes]
    }

    fn test_initialize(
        id: usize,
        name: String,
        mode: LsmMode,
        compression: LsmCompressionLib,
    ) -> LsmDb {
        let now = Utc::now();
        let db_path = "/tmp".to_string();
        let db_base_name = format!("{}-{}-{}", name, id, now.timestamp_nanos());

        let buckets = DEFAULT_BUCKETS.to_vec();
        let opts_1 = HistogramOpts::new("non_typed_write_times_s", "non_typed_write_times_s help")
            .buckets(buckets.clone());
        let opts_2 = HistogramOpts::new("work_kbs", "work_kbs help").buckets(buckets.clone());
        let opts_3 =
            HistogramOpts::new("work_times_s", "work_times_s help").buckets(buckets.clone());
        let opts_4 =
            HistogramOpts::new("checkpoint_kbs", "checkpoint_kbs help").buckets(buckets.clone());
        let opts_5 =
            HistogramOpts::new("checkpoint_times_s", "checkpoint_times_s help").buckets(buckets);

        let metrics = LsmMetrics {
            write_times_s: Histogram::with_opts(opts_1).unwrap(),
            work_kbs: Histogram::with_opts(opts_2).unwrap(),
            work_times_s: Histogram::with_opts(opts_3).unwrap(),
            checkpoint_kbs: Histogram::with_opts(opts_4).unwrap(),
            checkpoint_times_s: Histogram::with_opts(opts_5).unwrap(),
        };

        let db_conf = DbConf::new_with_parameters(
            db_path,
            db_base_name,
            mode,
            LsmHandleMode::ReadWrite,
            Some(metrics),
            compression,
        );

        let mut db: LsmDb = Default::default();

        assert!(!db.is_initialized());

        let rc = db.initialize(db_conf.clone());
        assert_eq!(rc, Ok(()));

        assert!(db.is_initialized());

        let rc = db.initialize(db_conf);
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        db
    }

    fn test_connect(db: &mut LsmDb) {
        // Produces a new handle.
        let rc = db.connect();
        assert_eq!(rc, Ok(()));
        assert!(db.is_connected());

        let rc = db.connect();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        assert!(db.is_connected());
    }

    fn test_disconnect(db: &mut LsmDb) {
        // Destroys the handle.
        let rc = db.disconnect();
        assert_eq!(rc, Ok(()));
        assert!(!db.is_connected());
    }

    fn test_persist_blobs(
        db: &mut LsmDb,
        num_blobs: usize,
        size_blob: usize,
        prng: Option<Mt19937GenRand64>,
        id: usize,
    ) {
        let master_blob = if let Some(mut prng) = prng {
            construct_random_blob(size_blob, &mut prng)
        } else {
            construct_compressible_blob(size_blob)
        };
        // Observe that we assume that a handle for the database has been
        // produced.
        for b in 1..=num_blobs {
            // The keys are tagged. Blobs with smaller key will appear first in the database.
            // In this manner, having different clients (with different ids) we can detect
            // which client's blobs are in the database.
            let current_blob_key = [id.to_be_bytes().as_ref(), b.to_be_bytes().as_ref()].concat();
            let mut current_blob = master_blob.clone();
            // Blobs are marked so that we can verify them later.
            current_blob[0] = (b & 0xFF) as u8;
            // Update is actually upsert (update if exists, insert otherwise)
            let rc = Disk::update(db, &current_blob_key, &current_blob);
            assert_eq!(rc, Ok(()));
        }
    }

    fn test_persist_grpc_blobs(
        db: &mut LsmDb,
        num_blobs: usize,
        size_blob: usize,
        mut prng: Mt19937GenRand64,
    ) {
        // One needs to produce a handle before operating on the database.
        let mut rc;
        for _ in 1..=num_blobs {
            let master_blob = construct_random_blob(size_blob, &mut prng);
            let current_blob = TypedBlob::encode_to_vec(&TypedBlob {
                payload: master_blob.clone(),
            });
            let signature = md5::compute(&current_blob).0;
            // Observe that what we insert is the grpc serialization of current_master_blob
            // and it is put under its md5 signature. In this way, when deserializing,
            // we can again compute the md5 signature of what we get and compare with the key.
            // In this way we test integrity of the blobs (I know md5 is broken on all sides, but
            // for this test, this is enough).
            // Update is actually upsert (update if exists, insert otherwise)
            rc = Disk::update(db, signature.as_ref(), &current_blob);
            assert_eq!(rc, Ok(()));
        }
    }

    fn test_forward_cursor(db: &mut LsmDb, num_blobs: usize, size_blob: usize, id: usize) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_forward = cursor_res.unwrap();
        let mut rc = cursor_forward.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_forward.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // This key is not found.
        let no_key: usize = 0x7FFFFFFFFFFFFFFF;
        let no_key_serial = [id.to_be_bytes().as_ref(), no_key.to_be_bytes().as_ref()].concat();

        // Cursor is not valid, thus an error is thrown.
        let cmp = Cursor::compare(&cursor_forward, &no_key_serial);
        assert_eq!(cmp, Err(LsmErrorCode::LsmError));

        rc = cursor_forward.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // Extracting from a non-valid cursor is not allowed.
        let current_key_res = Cursor::get_key(&cursor_forward);
        assert_eq!(current_key_res, Err(LsmErrorCode::LsmError));
        let current_blob_res = Cursor::get_value(&cursor_forward);
        assert_eq!(current_blob_res, Err(LsmErrorCode::LsmError));

        // We will now traverse the database forward.
        rc = cursor_forward.first();
        assert_eq!(rc, Ok(()));

        // Let's check that the database contains what we just persisted.
        for b in 1..=num_blobs {
            rc = cursor_forward.valid();
            assert_eq!(rc, Ok(()));
            let current_key_res = Cursor::get_key(&cursor_forward);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_forward);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            // These were the values originally inserted.
            let original_blob_key = [id.to_be_bytes().as_ref(), b.to_be_bytes().as_ref()].concat();
            assert_eq!(current_key, original_blob_key);
            assert_eq!(current_blob[0], (b & 0xFF) as u8);
            assert_eq!(current_blob.len(), size_blob);

            rc = cursor_forward.next();
            assert_eq!(rc, Ok(()));

            // Since we moved the cursor to the first element
            // we can never move backwards.
            rc = cursor_forward.prev();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        // The cursor is not valid any more because it's located at EOF.
        rc = cursor_forward.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // Moving a not-valid cursor is not considered an error.
        rc = cursor_forward.next();
        assert_eq!(rc, Ok(()));

        // Cursor remains invalid.
        rc = cursor_forward.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // Extracting from a non-valid cursor is not allowed.
        let current_key_res = Cursor::get_key(&cursor_forward);
        assert_eq!(current_key_res, Err(LsmErrorCode::LsmError));
        let current_blob_res = Cursor::get_value(&cursor_forward);
        assert_eq!(current_blob_res, Err(LsmErrorCode::LsmError));

        // Cursor is not valid, thus an error is thrown.
        let cmp = Cursor::compare(&cursor_forward, &no_key_serial);
        assert_eq!(cmp, Err(LsmErrorCode::LsmError));

        // Cursor can be moved again though.
        rc = cursor_forward.first();
        assert_eq!(rc, Ok(()));

        if num_blobs > 0 {
            // Cursor becomes valid again.
            rc = cursor_forward.valid();
            assert_eq!(rc, Ok(()));
        } else {
            rc = cursor_forward.valid();
            assert_eq!(rc, Err(LsmErrorCode::LsmError));
        }

        // Freeing up resources.
        rc = cursor_forward.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_forward_cursor_grpc(db: &mut LsmDb, num_blobs: usize) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_forward = cursor_res.unwrap();
        let mut rc = cursor_forward.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_forward.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // We will now traverse the database forward.
        rc = cursor_forward.first();
        assert_eq!(rc, Ok(()));

        // Let's check that the database contains what we just persisted.
        for _ in 1..=num_blobs {
            let current_key_res = Cursor::get_key(&cursor_forward);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_forward);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            let signature = md5::compute(current_blob).0;
            // In the signature matches, we assume everything is ok.
            assert_eq!(current_key, signature);

            rc = cursor_forward.next();
            assert_eq!(rc, Ok(()));

            // Since we moved the cursor to the first element
            // we can never move backwards.
            rc = cursor_forward.prev();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        // The cursor is not valid any more because it's located at EOF.
        rc = cursor_forward.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // Freeing up resources.
        rc = cursor_forward.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_backward_cursor(db: &mut LsmDb, num_blobs: usize, size_blob: usize, id: usize) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_backward = cursor_res.unwrap();
        let mut rc = cursor_backward.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_backward.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        rc = cursor_backward.last();
        assert_eq!(rc, Ok(()));

        // Let's check that the database contains what we just persisted.
        // but now traversing backwards.
        for b in (1..=num_blobs).rev() {
            let current_key_res = Cursor::get_key(&cursor_backward);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_backward);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            // These were the values originally inserted.
            let original_blob_key = [id.to_be_bytes().as_ref(), b.to_be_bytes().as_ref()].concat();
            assert_eq!(current_key, original_blob_key);
            assert_eq!(current_blob[0], (b & 0xFF) as u8);
            assert_eq!(current_blob.len(), size_blob);

            rc = cursor_backward.prev();
            assert_eq!(rc, Ok(()));

            // Since we moved the cursor to the last element
            // we can never move forward.
            rc = cursor_backward.next();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        // The cursor is not valid any more because it's located at EOF.
        rc = cursor_backward.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // Moving a not-valid cursor is not considered an error.
        rc = cursor_backward.prev();
        assert_eq!(rc, Ok(()));

        // Cursor remains invalid.
        rc = cursor_backward.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // Cursor can be moved again though.
        rc = cursor_backward.last();
        assert_eq!(rc, Ok(()));

        if num_blobs > 0 {
            // Cursor becomes valid again.
            rc = cursor_backward.valid();
            assert_eq!(rc, Ok(()));
        } else {
            rc = cursor_backward.valid();
            assert_eq!(rc, Err(LsmErrorCode::LsmError));
        }

        // Freeing up resources.
        rc = cursor_backward.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_backward_cursor_grpc(db: &mut LsmDb, num_blobs: usize) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_backward = cursor_res.unwrap();
        let mut rc = cursor_backward.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_backward.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        rc = cursor_backward.last();
        assert_eq!(rc, Ok(()));

        // Let's check that the database contains what we just persisted.
        // but now traversing backwards.
        for _ in (1..=num_blobs).rev() {
            let current_key_res = Cursor::get_key(&cursor_backward);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_backward);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            let signature = md5::compute(current_blob).0;
            // In the signature matches, we assume everything is ok.
            assert_eq!(current_key, signature);

            rc = cursor_backward.prev();
            assert_eq!(rc, Ok(()));

            // Since we moved the cursor to the last element
            // we can never move forward.
            rc = cursor_backward.next();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        // The cursor is not valid any more because it's located at EOF.
        rc = cursor_backward.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // Freeing up resources.
        rc = cursor_backward.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_seek_cursor_forward_limited(
        db: &mut LsmDb,
        start_key: usize,
        expected_num_blobs: usize,
        size_blob: usize,
        id: usize,
    ) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_seek_ge = cursor_res.unwrap();
        let mut rc = cursor_seek_ge.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_seek_ge.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // This key is not found.
        let no_key: usize = 0x7FFFFFFFFFFFFFFF;
        rc = Cursor::seek(
            &mut cursor_seek_ge,
            &[id.to_be_bytes().as_ref(), no_key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekGe,
        );
        assert_eq!(rc, Ok(()));
        rc = cursor_seek_ge.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // We only traverse the number of blobs that we are told.
        let key = start_key;
        let upper_key = key + start_key;
        let mut num_traversed_keys = 0;
        let mut current_underlying_key = key;
        rc = Cursor::seek(
            &mut cursor_seek_ge,
            &[id.to_be_bytes().as_ref(), key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekGe,
        );
        assert_eq!(rc, Ok(()));

        let mut cmp = Ordering::Less;
        // A cursor is valid as long as it's not positioned at the end of the file.
        // In which case the cursor has been exhausted.
        while cmp < Ordering::Equal {
            let current_key_res = Cursor::get_key(&cursor_seek_ge);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_seek_ge);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            // These were the values originally inserted.
            let original_blob_key = [
                id.to_be_bytes().as_ref(),
                current_underlying_key.to_be_bytes().as_ref(),
            ]
            .concat();
            assert_eq!(current_key, original_blob_key);
            assert_eq!(current_blob[0], (current_underlying_key & 0xFF) as u8);
            assert_eq!(current_blob.len(), size_blob);

            // We compare the upper bound we got from the user to see if the blob is
            // relevant for the result set. The moment the underlying key becomes larger,
            // we get out of the loop.
            let cmp_res = Cursor::compare(
                &cursor_seek_ge,
                &[id.to_be_bytes().as_ref(), upper_key.to_be_bytes().as_ref()].concat(),
            );
            assert!(cmp_res.is_ok());
            cmp = cmp_res.unwrap();

            num_traversed_keys += 1;
            current_underlying_key += 1;

            rc = cursor_seek_ge.next();
            assert_eq!(rc, Ok(()));

            // This one does not work.
            rc = cursor_seek_ge.prev();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        assert_eq!(num_traversed_keys, expected_num_blobs);

        rc = cursor_seek_ge.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_seek_cursor_forward_eof(
        db: &mut LsmDb,
        start_key: usize,
        expected_num_blobs: usize,
        size_blob: usize,
        id: usize,
    ) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_seek_ge = cursor_res.unwrap();
        let mut rc = cursor_seek_ge.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_seek_ge.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // This key is not found.
        let no_key: usize = 0x7FFFFFFFFFFFFFFF;
        rc = Cursor::seek(
            &mut cursor_seek_ge,
            &[id.to_be_bytes().as_ref(), no_key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekGe,
        );
        assert_eq!(rc, Ok(()));
        rc = cursor_seek_ge.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // We only traverse the number of blobs that we are told.
        let mut num_traversed_keys = 0;
        let mut current_underlying_key = start_key;
        rc = Cursor::seek(
            &mut cursor_seek_ge,
            &[id.to_be_bytes().as_ref(), start_key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekGe,
        );
        assert_eq!(rc, Ok(()));

        // A cursor is valid as long as it's not positioned at the end of the file.
        // In which case the cursor has been exhausted.
        while cursor_seek_ge.valid() == Ok(()) {
            let current_key_res = Cursor::get_key(&cursor_seek_ge);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_seek_ge);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            // These were the values originally inserted.
            let original_blob_key = [
                id.to_be_bytes().as_ref(),
                current_underlying_key.to_be_bytes().as_ref(),
            ]
            .concat();
            assert_eq!(current_key, original_blob_key);
            assert_eq!(current_blob[0], (current_underlying_key & 0xFF) as u8);
            assert_eq!(current_blob.len(), size_blob);

            num_traversed_keys += 1;
            current_underlying_key += 1;

            rc = cursor_seek_ge.next();
            assert_eq!(rc, Ok(()));

            // This one does not work.
            rc = cursor_seek_ge.prev();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        assert_eq!(num_traversed_keys, expected_num_blobs);

        rc = cursor_seek_ge.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_seek_cursor_backward_limited(
        db: &mut LsmDb,
        num_blobs: usize,
        size_blob: usize,
        id: usize,
    ) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_seek_le = cursor_res.unwrap();
        let mut rc = cursor_seek_le.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_seek_le.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // This key is not found.
        let no_key: usize = 0x0;
        rc = Cursor::seek(
            &mut cursor_seek_le,
            &[id.to_be_bytes().as_ref(), no_key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekLe,
        );
        assert_eq!(rc, Ok(()));
        rc = cursor_seek_le.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // We only traverse the number of blobs that we are told.
        let key = num_blobs << 1;
        let lower_key = key - num_blobs;
        let mut num_traversed_keys = 0;
        let mut current_underlying_key = key;
        rc = Cursor::seek(
            &mut cursor_seek_le,
            &[id.to_be_bytes().as_ref(), key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekLe,
        );
        assert_eq!(rc, Ok(()));

        let mut cmp = Ordering::Greater;
        // A cursor is valid as long as it's not positioned at the end of the file.
        // In which case the cursor has been exhausted.
        while cmp > Ordering::Equal {
            let current_key_res = Cursor::get_key(&cursor_seek_le);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_seek_le);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            // These were the values originally inserted.
            let original_blob_key = [
                id.to_be_bytes().as_ref(),
                current_underlying_key.to_be_bytes().as_ref(),
            ]
            .concat();
            assert_eq!(current_key, original_blob_key);
            assert_eq!(current_blob[0], (current_underlying_key & 0xFF) as u8);
            assert_eq!(current_blob.len(), size_blob);

            // We compare the upper bound we got from the user to see if the blob is
            // relevant for the result set. The moment the underlying key becomes larger,
            // we get out of the loop.
            let cmp_res = Cursor::compare(
                &cursor_seek_le,
                &[id.to_be_bytes().as_ref(), lower_key.to_be_bytes().as_ref()].concat(),
            );
            assert!(cmp_res.is_ok());
            cmp = cmp_res.unwrap();

            num_traversed_keys += 1;
            current_underlying_key -= 1;

            rc = cursor_seek_le.prev();
            assert_eq!(rc, Ok(()));

            // This one does not work.
            rc = cursor_seek_le.next();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        assert_eq!(num_traversed_keys, num_blobs + 1);

        rc = cursor_seek_le.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_seek_cursor_backward_eof(
        db: &mut LsmDb,
        start_key: usize,
        expected_num_keys: usize,
        size_blob: usize,
        id: usize,
    ) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_seek_le = cursor_res.unwrap();
        let mut rc = cursor_seek_le.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_seek_le.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // This key is not found.
        let no_key: usize = 0x0;
        rc = Cursor::seek(
            &mut cursor_seek_le,
            &[id.to_be_bytes().as_ref(), no_key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekLe,
        );
        assert_eq!(rc, Ok(()));
        rc = cursor_seek_le.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // We only traverse the number of blobs that we are told.
        let mut num_traversed_keys = 0;
        let mut current_underlying_key = start_key;
        rc = Cursor::seek(
            &mut cursor_seek_le,
            &[id.to_be_bytes().as_ref(), start_key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekLe,
        );
        assert_eq!(rc, Ok(()));

        // A cursor is valid as long as it's not positioned at the end of the file.
        // In which case the cursor has been exhausted.
        while cursor_seek_le.valid() == Ok(()) {
            let current_key_res = Cursor::get_key(&cursor_seek_le);
            assert!(current_key_res.is_ok());
            let current_blob_res = Cursor::get_value(&cursor_seek_le);
            assert!(current_blob_res.is_ok());
            let current_key = current_key_res.unwrap();
            let current_blob = current_blob_res.unwrap();
            // These were the values originally inserted.
            let original_blob_key = [
                id.to_be_bytes().as_ref(),
                current_underlying_key.to_be_bytes().as_ref(),
            ]
            .concat();
            assert_eq!(current_key, original_blob_key);
            assert_eq!(current_blob[0], (current_underlying_key & 0xFF) as u8);
            assert_eq!(current_blob.len(), size_blob);

            num_traversed_keys += 1;
            current_underlying_key -= 1;

            rc = cursor_seek_le.prev();
            assert_eq!(rc, Ok(()));

            // This one does not work.
            rc = cursor_seek_le.next();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        assert_eq!(num_traversed_keys, expected_num_keys);

        rc = cursor_seek_le.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_seek_cursor_exact(db: &mut LsmDb, start_key: usize, size_blob: usize, id: usize) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_seek_eq = cursor_res.unwrap();
        let mut rc = cursor_seek_eq.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_seek_eq.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // This key is not found.
        let no_key: usize = 0x7FFFFFFFFFFFFFFF;
        rc = Cursor::seek(
            &mut cursor_seek_eq,
            &[id.to_be_bytes().as_ref(), no_key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekEq,
        );
        assert_eq!(rc, Ok(()));
        rc = cursor_seek_eq.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        // We only traverse the number of blobs that we are told.
        let key = start_key;
        let current_underlying_key = key;
        rc = Cursor::seek(
            &mut cursor_seek_eq,
            &[id.to_be_bytes().as_ref(), key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekEq,
        );
        assert_eq!(rc, Ok(()));

        let current_key_res = Cursor::get_key(&cursor_seek_eq);
        assert!(current_key_res.is_ok());
        let current_blob_res = Cursor::get_value(&cursor_seek_eq);
        assert!(current_blob_res.is_ok());
        let current_key = current_key_res.unwrap();
        let current_blob = current_blob_res.unwrap();
        // These were the values originally inserted.
        let original_blob_key = [
            id.to_be_bytes().as_ref(),
            current_underlying_key.to_be_bytes().as_ref(),
        ]
        .concat();
        assert_eq!(current_key, original_blob_key);
        assert_eq!(current_blob[0], (current_underlying_key & 0xFF) as u8);
        assert_eq!(current_blob.len(), size_blob);

        // We compare the upper bound we got from the user to see if the blob is
        // relevant for the result set. The moment the underlying key becomes larger,
        // we get out of the loop.
        let cmp_res = Cursor::compare(
            &cursor_seek_eq,
            &[id.to_be_bytes().as_ref(), key.to_be_bytes().as_ref()].concat(),
        );
        assert_eq!(cmp_res, Ok(Ordering::Equal));

        rc = cursor_seek_eq.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // This one does not work.
        rc = cursor_seek_eq.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        rc = cursor_seek_eq.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_num_blobs_are_in_file(db: &mut LsmDb, expected_num_blobs: usize) {
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor_seek_forward = cursor_res.unwrap();
        let mut rc = cursor_seek_forward.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor_seek_forward.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // Move the cursor to the very first key of the file.
        rc = cursor_seek_forward.first();
        assert_eq!(rc, Ok(()));

        let mut num_traversed_keys = 0;
        // A cursor is valid as long as it's not positioned at the end of the file.
        // In which case the cursor has been exhausted.
        while cursor_seek_forward.valid() == Ok(()) {
            num_traversed_keys += 1;

            rc = cursor_seek_forward.next();
            assert_eq!(rc, Ok(()));

            // This one does not work.
            rc = cursor_seek_forward.prev();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        assert_eq!(num_traversed_keys, expected_num_blobs);
    }

    fn test_single_deletion(db: &mut LsmDb, key: usize, total_num_blobs: usize, id: usize) {
        let rc = Disk::delete(
            db,
            &[id.to_be_bytes().as_ref(), key.to_be_bytes().as_ref()].concat(),
        );
        assert_eq!(rc, Ok(()));

        // Now we verify that we have one element less,
        // and that we cannot find the one we just deleted.
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor = cursor_res.unwrap();
        let mut rc = cursor.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // We will now traverse the database forward.
        rc = cursor.first();
        assert_eq!(rc, Ok(()));

        let mut num_traversed_keys = 0;
        while cursor.valid() == Ok(()) {
            num_traversed_keys += 1;

            rc = cursor.next();
            assert_eq!(rc, Ok(()));

            // This one does not work.
            rc = cursor.prev();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        assert_eq!(num_traversed_keys, total_num_blobs - 1);

        // Now let us check that the key cannot be found.
        rc = Cursor::seek(
            &mut cursor,
            &[id.to_be_bytes().as_ref(), key.to_be_bytes().as_ref()].concat(),
            LsmCursorSeekOp::LsmCursorSeekEq,
        );
        assert_eq!(rc, Ok(()));
        rc = cursor.valid();
        assert_eq!(rc, Err(LsmErrorCode::LsmError));

        rc = cursor.close();
        assert_eq!(rc, Ok(()));
    }

    fn test_range_deletion(
        db: &mut LsmDb,
        starting_key: usize,
        ending_key: usize,
        total_num_blobs: usize,
        id: usize,
    ) {
        // Ending and starting keys are not deleted.
        let num_deleted_keys = ending_key - starting_key - 1;

        // If the interval extremes are swapped, nothing happens.
        // If something gets deleted here, then the following tests
        // will fail because cardinalities won't match any more.
        let rc = Disk::delete_range(
            db,
            &[id.to_be_bytes().as_ref(), ending_key.to_be_bytes().as_ref()].concat(),
            &[
                id.to_be_bytes().as_ref(),
                starting_key.to_be_bytes().as_ref(),
            ]
            .concat(),
        );
        assert_eq!(rc, Ok(()));

        // Range-delete deletes an open interval.
        let rc = Disk::delete_range(
            db,
            &[
                id.to_be_bytes().as_ref(),
                starting_key.to_be_bytes().as_ref(),
            ]
            .concat(),
            &[id.to_be_bytes().as_ref(), ending_key.to_be_bytes().as_ref()].concat(),
        );
        assert_eq!(rc, Ok(()));

        // Now we verify that we have much less entries,
        // and that we cannot find the ones we just deleted.
        let cursor_res = Disk::cursor_open(db);
        assert!(cursor_res.is_ok());

        let mut cursor = cursor_res.unwrap();
        let mut rc = cursor.next();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        rc = cursor.prev();
        assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

        // We will now traverse the database forward.
        rc = cursor.first();
        assert_eq!(rc, Ok(()));

        let mut num_traversed_keys = 0;
        while cursor.valid() == Ok(()) {
            num_traversed_keys += 1;

            rc = cursor.next();
            assert_eq!(rc, Ok(()));

            // This one does not work.
            rc = cursor.prev();
            assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
        }
        // The - 1 it's because in a previous step outside, we deleted a single
        // element, and thus it cannot be found any longer.
        assert_eq!(num_traversed_keys, total_num_blobs - num_deleted_keys - 1);

        //  Remember that starting and ending keys are not deleted.
        for key in (starting_key + 1)..ending_key {
            // Now let us check that the key cannot be found.
            rc = Cursor::seek(
                &mut cursor,
                &[id.to_be_bytes().as_ref(), key.to_be_bytes().as_ref()].concat(),
                LsmCursorSeekOp::LsmCursorSeekEq,
            );
            assert_eq!(rc, Ok(()));
            rc = cursor.valid();
            assert_eq!(rc, Err(LsmErrorCode::LsmError));
        }

        rc = cursor.close();
        assert_eq!(rc, Ok(()));
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        let mut master_prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        for db_idx in 1..=num_dbs {
            // Every thread will have a different seed, but each time it will be same,
            // as the master prng is initialized with the same seed every time.
            let thread_prng: Mt64 = SeedableRng::seed_from_u64(master_prng.next_u64());
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-blobs-optimized".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::NoCompression,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(
                        &mut db,
                        num_blobs,
                        size_blob,
                        Some(thread_prng.clone()),
                        db_idx,
                    );
                }

                // Let's optimize the DB. No wasted data pages.
                assert!(db.optimize().is_ok());

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now using cursors via seeking a particular blob.
                test_seek_cursor_forward_limited(
                    &mut db,
                    num_blobs >> 2,
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);

                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_non_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        let mut master_prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        for db_idx in 1..=num_dbs {
            // Every thread will have a different seed, but each time it will be same,
            // as the master prng is initialized with the same seed every time.
            let thread_prng: Mt64 = SeedableRng::seed_from_u64(master_prng.next_u64());
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-blobs-non-optimized".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::NoCompression,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(
                        &mut db,
                        num_blobs,
                        size_blob,
                        Some(thread_prng.clone()),
                        db_idx,
                    );
                }

                // No optimization now. There will be wasted data pages.

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);

                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_empty_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let mut thread_handles = vec![];
        for db_idx in 1..=num_dbs {
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-get-blobs-optimized-empty".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::NoCompression,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 0;
                let size_blob = 0;

                // Let's optimize the DB. No wasted data pages.
                assert!(db.optimize().is_ok());

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(&mut db, 3 * (num_blobs >> 2), 0, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_compressed_lz4_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        // Every database will be handled by a thread.
        for db_idx in 1..=num_dbs {
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-compressed-lz4-blobs".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::LZ4,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(&mut db, num_blobs, size_blob, None, db_idx);
                }

                // Let's optimize the DB. No wasted data pages.
                assert!(db.optimize().is_ok());

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now using cursors via seeking a particular blob.
                test_seek_cursor_forward_limited(
                    &mut db,
                    num_blobs >> 2,
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);
                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                assert_eq!(db.get_compression_id(), Ok(LsmCompressionLib::LZ4));

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_compressed_lz4_non_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        // Every database will be handled by a thread.
        for db_idx in 1..=num_dbs {
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-compressed-lz4-blobs".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::LZ4,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(&mut db, num_blobs, size_blob, None, db_idx);
                }

                // No optimization now. There will be wasted data pages.

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now using cursors via seeking a particular blob.
                test_seek_cursor_forward_limited(
                    &mut db,
                    num_blobs >> 2,
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);
                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                assert_eq!(db.get_compression_id(), Ok(LsmCompressionLib::LZ4));

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_compressed_zlib_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        // Every database will be handled by a thread.
        for db_idx in 1..=num_dbs {
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-compressed-zlib-blobs".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::ZLib,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(&mut db, num_blobs, size_blob, None, db_idx);
                }

                // Let's optimize the DB. No wasted data pages.
                assert!(db.optimize().is_ok());

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now using cursors via seeking a particular blob.
                test_seek_cursor_forward_limited(
                    &mut db,
                    num_blobs >> 2,
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);
                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                assert_eq!(db.get_compression_id(), Ok(LsmCompressionLib::ZLib));

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_compressed_zlib_non_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        // Every database will be handled by a thread.
        for db_idx in 1..=num_dbs {
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-compressed-zlib-blobs".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::ZLib,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(&mut db, num_blobs, size_blob, None, db_idx);
                }

                // No optimization now. There will be wasted data pages.

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now using cursors via seeking a particular blob.
                test_seek_cursor_forward_limited(
                    &mut db,
                    num_blobs >> 2,
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);
                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                assert_eq!(db.get_compression_id(), Ok(LsmCompressionLib::ZLib));

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_compressed_zstd_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        // Every database will be handled by a thread.
        for db_idx in 1..=num_dbs {
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-compressed-zstd-blobs-optimized".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::ZStd,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(&mut db, num_blobs, size_blob, None, db_idx);
                }

                // Let's optimize the DB. No wasted data pages.
                assert!(db.optimize().is_ok());

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now using cursors via seeking a particular blob.
                test_seek_cursor_forward_limited(
                    &mut db,
                    num_blobs >> 2,
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);
                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                assert_eq!(db.get_compression_id(), Ok(LsmCompressionLib::ZStd));

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// We test the whole api under different circumstances.
    fn lsm_whole_rust_bindings_multiple_compressed_zstd_non_optimized_dbs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let num_segments = 2;
        let mut thread_handles = vec![];
        // Every database will be handled by a thread.
        for db_idx in 1..=num_dbs {
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-compressed-zstd-blobs-non-optimized".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::ZStd,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 10000_usize;
                let size_blob = 1 << 10; // 1 KB

                // At most this many segment are created (depending on checkpointing for example).
                for _ in 0..num_segments {
                    // Let's persist some blobs.
                    test_persist_blobs(&mut db, num_blobs, size_blob, None, db_idx);
                }

                // No optimization now. There will be wasted data pages.

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now traverse the database backward.
                test_backward_cursor(&mut db, num_blobs, size_blob, db_idx);

                // Now using cursors via seeking a particular blob.
                test_seek_cursor_forward_limited(
                    &mut db,
                    num_blobs >> 2,
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now until the very end of the database (EOF)
                test_seek_cursor_forward_eof(
                    &mut db,
                    3 * (num_blobs >> 2),
                    (num_blobs >> 2) + 1,
                    size_blob,
                    db_idx,
                );

                // Now using seeking going backwards.
                test_seek_cursor_backward_limited(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now until the beginning of the database (inverse EOF)
                test_seek_cursor_backward_eof(
                    &mut db,
                    num_blobs >> 2,
                    num_blobs >> 2,
                    size_blob,
                    db_idx,
                );

                // Now seek with exact match.
                test_seek_cursor_exact(&mut db, num_blobs >> 2, size_blob, db_idx);

                // Now let's test deletions.
                test_single_deletion(&mut db, num_blobs >> 2, num_blobs, db_idx);
                test_range_deletion(&mut db, num_blobs >> 1, num_blobs, num_blobs, db_idx);

                assert_eq!(db.get_compression_id(), Ok(LsmCompressionLib::ZStd));

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// Let's test that a non-initialized database behaves well.
    fn lsm_rust_bindings_with_not_yet_initialized_db() {
        let num_dbs = 2;
        let mut thread_handles = vec![];
        let mut master_prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        for _ in 1..=num_dbs {
            // Every thread will have a different seed, but each time it will be same,
            // as the master prng is initialized with the same seed every time.
            let mut thread_prng: Mt64 = SeedableRng::seed_from_u64(master_prng.next_u64());
            let handle = thread::spawn(move || {
                let mut db: LsmDb = Default::default();

                // We now produce certain amount of blobs and persist them.
                let size_blob = 1 << 10; // 1 KB
                let blob = construct_random_blob(size_blob, &mut thread_prng);

                let mut rc = db.connect();
                assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));
                rc = Disk::persist(&mut db, &Vec::from(1usize.to_be_bytes()), &blob);
                assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

                let key = Vec::from(1usize.to_be_bytes());
                rc = Disk::delete(&mut db, &key);
                assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

                // let cursor_res = Disk::cursor_open(&mut db);
                // assert!(cursor_res.is_err());

                let starting_key = Vec::from(1usize.to_be_bytes());
                let ending_key = Vec::from(0usize.not().to_be_bytes());
                rc = Disk::delete_range(&mut db, &starting_key, &ending_key);
                assert_eq!(rc, Err(LsmErrorCode::LsmMisuse));

                rc = db.disconnect();
                assert_eq!(rc, Ok(()));
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    /// To see whether we can open and close the same file over and over again.
    fn lsm_open_close_multiple_times() {
        let num_connection_cycles = 100;
        let mut db = test_initialize(
            0,
            "test-open-close".to_string(),
            LsmMode::LsmNoBackgroundThreads,
            LsmCompressionLib::NoCompression,
        );
        test_connect(&mut db);

        for _ in 0..num_connection_cycles {
            test_disconnect(&mut db);
            test_connect(&mut db);
        }

        test_disconnect(&mut db);
    }

    #[test]
    fn lsm_rust_bindings_multiple_dbs_grpc_blobs() {
        let num_dbs = 2;
        let num_connection_cycles = 10;
        let mut thread_handles = vec![];
        let mut master_prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        for db_idx in 1..=num_dbs {
            // Every thread will have a different seed, but each time it will be same,
            // as the master prng is initialized with the same seed every time.
            let thread_prng: Mt64 = SeedableRng::seed_from_u64(master_prng.next_u64());
            let handle = thread::spawn(move || {
                // Let's try to initialize a database per thread.
                let mut db = test_initialize(
                    db_idx,
                    "test-insert-get-grpc-blobs".to_string(),
                    LsmMode::LsmNoBackgroundThreads,
                    LsmCompressionLib::NoCompression,
                );

                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // We now produce certain amount of blobs and persist them.
                let num_blobs = 100_usize;
                let size_blob = 1 << 10; // 1 KB

                // Let's persist some blobs.
                test_persist_grpc_blobs(&mut db, num_blobs, size_blob, thread_prng);

                // Let's optimize the DB.
                assert!(db.optimize().is_ok());

                // Once some blobs have been persisted, we connect
                // and disconnect multiple times and then we test that
                // what we read is what we expect.
                for _ in 0..num_connection_cycles {
                    test_disconnect(&mut db);
                    test_connect(&mut db);
                }

                // Let's test forward iterators on the whole database.
                test_forward_cursor_grpc(&mut db, num_blobs);

                // Now traverse the database backward.
                test_backward_cursor_grpc(&mut db, num_blobs);

                // Good bye
                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    fn lsm_cursor_automatic_drop() {
        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        // Let's try to initialize a database per thread.
        let mut db = test_initialize(
            1,
            "test-cursor-automatic-drop".to_string(),
            LsmMode::LsmNoBackgroundThreads,
            LsmCompressionLib::NoCompression,
        );

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 100_usize;
        let size_blob = 1 << 10; // 1 KB

        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        {
            // Let's open a cursor (which will produce a snapshot) within a scope.
            let cursor_res = db.cursor_open();
            assert!(cursor_res.is_ok());
        }

        // We should now be able to close the database cleanly as the cursor has been
        // dropped automatically.
        test_disconnect(&mut db);
    }

    #[test]
    fn lsm_handle_automatic_drop() {
        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        let db_fqn: String;

        // The database is opened within a scope. Thus the `db` handle will
        // be valid inside it. When going out of scope, the handle is freed, and
        // thus the database will be cleanly close.
        {
            // Let's try to initialize a database per thread.
            let mut db = test_initialize(
                1,
                "test-handle-automatic-drop".to_string(),
                LsmMode::LsmNoBackgroundThreads,
                LsmCompressionLib::NoCompression,
            );

            // We copy the whole path of the database. Observe that we cannot use db_fq_name
            // as that's a c-string (and thus nul-terminated).
            db_fqn = format!(
                "{}/{}.lsm",
                db.db_conf.db_path.display(),
                db.db_conf.db_base_name
            );

            // Let's connect to it via a main memory handle.
            test_connect(&mut db);

            // We now produce certain amount of blobs and persist them.
            let num_blobs = 100_usize;
            let size_blob = 1 << 10; // 1 KB

            // Let's persist some blobs.
            test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

            // We don't disconnect explicitly from the database. This will be done
            // by going out of scope.
        }
        // The only way we can test whether the database was closed cleanly is that
        // no database files other than the main database file exist in the filesystem.
        let db_file = Path::new(&db_fqn);
        let db_wal_fqn = format!("{}-{}", db_fqn.clone(), "log");
        let db_wal = Path::new(&db_wal_fqn);

        // The following file should exist.
        assert!(db_file.exists());
        // The following files shouldn't exist.
        assert!(!db_wal.exists());
    }

    #[test]
    fn lsm_multiple_writers_interlaced() {
        let num_writers = 5;
        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        let now = Utc::now();
        let db_path: String = "/tmp".to_string();
        let db_base_name: String = format!(
            "{}-{}",
            "test-multiple-writers-interlaced",
            now.timestamp_nanos()
        );
        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        // Let's produce the handles for every writer.
        let mut db_handles = vec![];
        for _ in 1..=num_writers {
            // Let's try to initialize a database per thread.
            let db_conf = DbConf {
                db_path: db_path.clone().into(),
                db_base_name: db_base_name.clone(),
                mode: LsmMode::LsmNoBackgroundThreads,
                handle_mode: LsmHandleMode::ReadWrite,
                metrics: None,
                compression: LsmCompressionLib::NoCompression,
            };

            let mut db: LsmDb = Default::default();

            let rc = db.initialize(db_conf);
            assert_eq!(rc, Ok(()));

            let rc = db.connect();
            assert_eq!(rc, Ok(()));

            db_handles.push(db);
        }

        // Now let's use the handles to write to the same file one writer at a time.
        // This works because each time the writer lock is obtained by the corresponding
        // writer.
        for (db_handle_id, db_handle) in db_handles.iter_mut().enumerate().take(num_writers) {
            // Let's persist some more blobs (which can only be written on a clean database).
            test_persist_blobs(
                db_handle,
                num_blobs,
                size_blob,
                Some(prng.clone()),
                db_handle_id,
            );
        }
    }

    #[test]
    fn lsm_bg_checkpointer_and_merger_in_one() {
        // Let's try to initialize a database. The connection will issue one background
        // thread that will merge and checkpoint the database file at certain intervals.
        let mut db = test_initialize(
            1,
            "test-bg-checkpointer-and-merger-in-one".to_string(),
            LsmMode::LsmBackgroundMerger,
            LsmCompressionLib::NoCompression,
        );

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 100_usize;
        let size_blob = 1 << 20; // 1 MB
        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        // Checkpointing increases the size of the data file. Thus, in the end,
        // the file size should be something between 88 and 92 MiBs (for this particular test).
        let db_path = db.db_conf.db_path.clone();
        let db_base_name = db.db_conf.db_base_name.clone();
        let db_fqn = format!("{}/{db_base_name}.lsm", db_path.display());
        let db_file = Path::new(&db_fqn);
        let db_wal_fqn = format!("{}-{}", db_fqn.clone(), "log");
        let db_wal = Path::new(&db_wal_fqn);
        assert!(db_file.exists());
        assert!(db_wal.exists());

        let datafile_size_res = db_file.metadata();
        assert!(datafile_size_res.is_ok());

        let datafile_size = datafile_size_res.unwrap().len();
        let expected_datafile_min_size: u64 = 67_000_000;
        let expected_datafile_max_size: u64 = 125_000_000;
        // The size of the data file might vary from platform to platform, but for this
        // particular test it should be about 68 MiBs
        assert!(datafile_size > expected_datafile_min_size);
        assert!(datafile_size < expected_datafile_max_size);

        // We should now be able to close the database cleanly as the cursor has been
        // dropped automatically.
        test_disconnect(&mut db)
    }

    #[test]
    fn lsm_bg_checkpointer_only() {
        // Let's try to initialize a database. The connection will issue one background
        // thread that will checkpoint the database file at certain intervals.
        let mut db = test_initialize(
            1,
            "test-bg-checkpointer-only".to_string(),
            LsmMode::LsmBackgroundCheckpointer,
            LsmCompressionLib::NoCompression,
        );

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 100_usize;
        let size_blob = 1 << 20; // 1 MB
        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        // Checkpointing increases the size of the data file. Thus, in the end,
        // the file size should be around 160 MiBs (for this particular test).
        let db_path = db.db_conf.db_path.clone();
        let db_base_name = db.db_conf.db_base_name.clone();
        let db_fqn = format!("{}/{db_base_name}.lsm", db_path.display());
        let db_file = Path::new(&db_fqn);
        let db_wal_fqn = format!("{}-{}", db_fqn.clone(), "log");
        let db_wal = Path::new(&db_wal_fqn);
        assert!(db_file.exists());
        assert!(db_wal.exists());

        let datafile_size_res = db_file.metadata();
        assert!(datafile_size_res.is_ok());

        let datafile_size = datafile_size_res.unwrap().len();
        let expected_datafile_min_size: u64 = 160_000_000;
        let expected_datafile_max_size: u64 = 170_000_000;
        // The size of the data file might vary from platform to platform, but for this
        // particular test it should be about 68 MiBs
        assert!(datafile_size > expected_datafile_min_size);
        assert!(datafile_size < expected_datafile_max_size);

        // We should now be able to close the database cleanly as the cursor has been
        // dropped automatically.
        test_disconnect(&mut db);
    }

    #[test]
    fn lsm_initialization_fails_with_non_c_string() {
        let bad_filename = "test-no-null\0in-the-middle".to_string();
        let now = Utc::now();
        let db_conf = DbConf {
            db_path: "/tmp".into(),
            db_base_name: format!("{}-{}", bad_filename, now.timestamp_nanos(),),
            mode: LsmMode::LsmNoBackgroundThreads,
            handle_mode: LsmHandleMode::ReadWrite,
            metrics: None,
            compression: LsmCompressionLib::NoCompression,
        };

        let mut db: LsmDb = Default::default();

        let rc = db.initialize(db_conf);
        assert_eq!(rc, Err(LsmErrorCode::LsmError));
    }

    #[test]
    fn transactions_rollback() {
        // Let's initialize a handle.
        let mut db = test_initialize(
            1,
            "test-transactions-rollback".to_string(),
            LsmMode::LsmNoBackgroundThreads,
            LsmCompressionLib::NoCompression,
        );

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB
        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // Let's open a transaction
        let mut rc = db.begin_transaction();
        assert_eq!(rc, Ok(()));

        // Let's persist some blobs within the opened transactions.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        // Let's rollback the transaction (making all persisted blobs disappear).
        rc = db.rollback_transaction();
        assert_eq!(rc, Ok(()));

        // We now test that no blob is found in the database although some were "persisted".
        test_num_blobs_are_in_file(&mut db, 0);
    }

    #[test]
    fn transactions_commit() {
        // Let's initialize a handle.
        let mut db = test_initialize(
            1,
            "test-transactions-rollback".to_string(),
            LsmMode::LsmNoBackgroundThreads,
            LsmCompressionLib::NoCompression,
        );

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB
        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // Let's open a transaction
        let mut rc = db.begin_transaction();
        assert_eq!(rc, Ok(()));

        // Let's persist some blobs within the opened transactions.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        // Let's commit the transaction; making all blobs truly persisted blobs.
        rc = db.commit_transaction();
        assert_eq!(rc, Ok(()));

        // We now test that no blob is found in the database although some were "persisted".
        test_num_blobs_are_in_file(&mut db, num_blobs);
    }

    #[test]
    fn can_work_with_empty_metrics_no_background_threads() {
        let mut db = test_initialize(
            1,
            "test-can-work-with-empty-metrics-no-background-threads".to_string(),
            LsmMode::LsmNoBackgroundThreads,
            LsmCompressionLib::NoCompression,
        );
        // This is the interesting bit.
        db.db_conf.metrics = None;

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        // Let's test forward iterators on the whole database.
        test_forward_cursor(&mut db, num_blobs, size_blob, 0);

        test_disconnect(&mut db);
    }

    #[test]
    fn can_work_with_empty_metrics_with_background_merger() {
        let mut db = test_initialize(
            1,
            "test-can-work-with-empty-metrics-with-background-merger".to_string(),
            LsmMode::LsmBackgroundMerger,
            LsmCompressionLib::NoCompression,
        );
        // This is the interesting bit.
        db.db_conf.metrics = None;

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        // We disconnect to be able to finish all background work, before
        // continuing.
        test_disconnect(&mut db);
        test_connect(&mut db);

        // Let's test forward iterators on the whole database.
        test_forward_cursor(&mut db, num_blobs, size_blob, 0);

        test_disconnect(&mut db);
    }

    #[test]
    fn can_work_with_compressed_db_with_background_merger() {
        let mut dbs = vec![];
        let db_lz4 = test_initialize(
            1,
            "test-can-work-with-compressed-db-lz4-with-background-merger".to_string(),
            LsmMode::LsmBackgroundMerger,
            LsmCompressionLib::LZ4,
        );
        let db_zlib = test_initialize(
            1,
            "test-can-work-with-compressed-db-zlib-with-background-merger".to_string(),
            LsmMode::LsmBackgroundMerger,
            LsmCompressionLib::ZLib,
        );
        let db_zstd = test_initialize(
            1,
            "test-can-work-with-compressed-db-zstd-with-background-merger".to_string(),
            LsmMode::LsmBackgroundMerger,
            LsmCompressionLib::ZStd,
        );
        dbs.push(db_lz4);
        dbs.push(db_zstd);
        dbs.push(db_zlib);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let mut thread_handles = vec![];
        for mut db in dbs {
            let handle = thread::spawn(move || {
                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // Let's persist some blobs.
                test_persist_blobs(&mut db, num_blobs, size_blob, None, 0);

                // We disconnect to be able to finish all background work, before
                // continuing.
                test_disconnect(&mut db);
                test_connect(&mut db);

                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, 0);

                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    fn can_work_with_empty_metrics_with_background_checkpointer() {
        let mut db = test_initialize(
            1,
            "test-can-work-with-empty-metrics-with-background-checkpointer".to_string(),
            LsmMode::LsmBackgroundCheckpointer,
            LsmCompressionLib::NoCompression,
        );
        // This is the interesting bit.
        db.db_conf.metrics = None;

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng), 0);

        // Let's test forward iterators on the whole database.
        test_forward_cursor(&mut db, num_blobs, size_blob, 0);

        test_disconnect(&mut db);
    }

    #[test]
    fn can_work_with_compressed_db_with_background_checkpointer() {
        let mut dbs = vec![];
        let db_lz4 = test_initialize(
            1,
            "test-can-work-with-compressed-db-lz4-with-background-checkpointer".to_string(),
            LsmMode::LsmBackgroundCheckpointer,
            LsmCompressionLib::LZ4,
        );
        let db_zlib = test_initialize(
            1,
            "test-can-work-with-compressed-db-zlib-with-background-checkpointer".to_string(),
            LsmMode::LsmBackgroundCheckpointer,
            LsmCompressionLib::ZLib,
        );
        let db_zstd = test_initialize(
            1,
            "test-can-work-with-compressed-db-zstd-with-background-checkpointer".to_string(),
            LsmMode::LsmBackgroundCheckpointer,
            LsmCompressionLib::ZStd,
        );
        dbs.push(db_lz4);
        dbs.push(db_zstd);
        dbs.push(db_zlib);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let mut thread_handles = vec![];
        for mut db in dbs {
            let handle = thread::spawn(move || {
                // Let's connect to it via a main memory handle.
                test_connect(&mut db);

                // Let's persist some blobs.
                test_persist_blobs(&mut db, num_blobs, size_blob, None, 0);
                // Let's test forward iterators on the whole database.
                test_forward_cursor(&mut db, num_blobs, size_blob, 0);

                test_disconnect(&mut db);
            });
            thread_handles.push(handle);
        }
        for t in thread_handles {
            t.join().unwrap();
        }
    }

    #[test]
    fn lsm_no_concurrent_writers_no_background_threads() {
        let num_writers = 10;
        let mut master_prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        let now = Utc::now();
        let db_path: String = "/tmp".to_string();
        let db_base_name: String =
            format!("{}-{}", "test-no-concurrent_writers", now.timestamp_nanos());
        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let mut thread_handles = vec![];
        for writer_id in 1..=num_writers {
            let thread_db_path = db_path.clone();
            let thread_db_base_name = db_base_name.clone();
            let thread_prng: Mt64 = SeedableRng::seed_from_u64(master_prng.next_u64());
            let handle = thread::spawn(move || {
                // Let's try to initialize a database handle per thread.
                let db_conf = DbConf {
                    db_path: thread_db_path.into(),
                    db_base_name: thread_db_base_name,
                    mode: LsmMode::LsmNoBackgroundThreads,
                    handle_mode: LsmHandleMode::ReadWrite,
                    metrics: None,
                    compression: LsmCompressionLib::NoCompression,
                };

                let mut db: LsmDb = Default::default();

                let mut rc = db.initialize(db_conf);
                assert_eq!(rc, Ok(()));

                rc = db.connect();
                assert_eq!(rc, Ok(()));

                // Let's open a write transaction to write a batch.
                rc = db.begin_transaction();
                // Who knows which thread will succeed, but it can be only one.
                if rc.is_ok() {
                    test_persist_blobs(&mut db, num_blobs, size_blob, Some(thread_prng), writer_id);
                    rc = db.commit_transaction();

                    if rc.is_ok() {
                        writer_id
                    } else {
                        assert_eq!(rc, Err(LsmErrorCode::LsmBusy));
                        0
                    }
                } else {
                    assert_eq!(rc, Err(LsmErrorCode::LsmBusy));
                    // Unsuccessful thread returns id 0.
                    0
                }
            });

            thread_handles.push(handle);
        }

        let mut successful_thread: usize = 0;
        for t in thread_handles {
            let result = t.join().unwrap();
            successful_thread = if result != 0 {
                result
            } else {
                successful_thread
            };
        }

        // Final connection to the database.
        let db_conf = DbConf {
            db_path: db_path.into(),
            db_base_name,
            mode: LsmMode::LsmNoBackgroundThreads,
            handle_mode: LsmHandleMode::ReadWrite,
            metrics: None,
            compression: LsmCompressionLib::NoCompression,
        };

        let mut db: LsmDb = Default::default();

        let rc = db.initialize(db_conf);
        assert_eq!(rc, Ok(()));

        let rc = db.connect();
        assert_eq!(rc, Ok(()));

        // After all threads have finished, we traverse the whole database. There can be only
        // blobs from a single writers.
        test_forward_cursor(&mut db, num_blobs, size_blob, successful_thread);
    }

    #[test]
    fn open_file_in_read_only_mode() {
        // We first produce a file we can work on
        let now = Utc::now();
        let db_path = "/tmp".to_string();
        let db_base_name = format!("{}-{}-{}", "test-read-only-mode", 0, now.timestamp_nanos());

        let mut db_conf = DbConf::new(db_path, db_base_name);

        let mut db: LsmDb = Default::default();

        let mut rc = db.initialize(db_conf.clone());
        assert_eq!(rc, Ok(()));

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let mut prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, Some(prng.clone()), 0);

        test_disconnect(&mut db);

        // To open the exact same file, we reuse the the same configuration but
        // marked as read-only.
        db_conf.handle_mode = LsmHandleMode::ReadOnly;

        // Initialize the read-only configuration.
        let mut db_ro: LsmDb = Default::default();
        rc = db_ro.initialize(db_conf);
        assert_eq!(rc, Ok(()));

        // Connect to the database.
        rc = db_ro.connect();
        assert_eq!(rc, Ok(()));

        // Let us try to write one more blob...and fail
        let extra_blob = construct_random_blob(size_blob, &mut prng);
        rc = Disk::persist(
            &mut db_ro,
            &Vec::from((num_blobs + 1).to_be_bytes()),
            &extra_blob,
        );
        assert_eq!(rc.unwrap_err(), LsmErrorCode::LsmReadOnly);

        // Without our fix to lsm1.c this test would fail/crash/forever loop, who knows.
        test_forward_cursor(&mut db_ro, num_blobs, size_blob, 0);
    }

    #[test]
    fn open_compressed_file_in_read_only_mode() {
        // We first produce a file we can work on
        let now = Utc::now();
        let db_path = "/tmp".to_string();
        let db_base_name = format!(
            "{}-{}-{}",
            "test-read-only-mode-compressed-db",
            0,
            now.timestamp_nanos()
        );

        let mut db_conf = DbConf::new_with_parameters(
            db_path,
            db_base_name,
            LsmMode::LsmBackgroundMerger,
            LsmHandleMode::ReadWrite,
            None,
            LsmCompressionLib::ZStd,
        );

        let mut db: LsmDb = Default::default();

        let mut rc = db.initialize(db_conf.clone());
        assert_eq!(rc, Ok(()));

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let mut prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, None, 0);

        test_disconnect(&mut db);

        // To open the exact same file, we reuse the the same configuration but
        // marked as read-only.
        db_conf.handle_mode = LsmHandleMode::ReadOnly;

        // Initialize the read-only configuration.
        let mut db_ro: LsmDb = Default::default();
        rc = db_ro.initialize(db_conf);
        assert_eq!(rc, Ok(()));

        // Connect to the database.
        rc = db_ro.connect();
        assert_eq!(rc, Ok(()));

        // Let us try to write one more blob...and fail
        let extra_blob = construct_random_blob(size_blob, &mut prng);
        rc = Disk::persist(
            &mut db_ro,
            &Vec::from((num_blobs + 1).to_be_bytes()),
            &extra_blob,
        );
        assert_eq!(rc.unwrap_err(), LsmErrorCode::LsmReadOnly);

        // Asking for compression on a read-only database without any cursor opened
        // is considered misused, as db parameters are loaded on the first transaction.
        assert_eq!(db.get_compression_id(), Err(LsmErrorCode::LsmMisuse));

        // Without our fix to lsm1.c this test would fail/crash/forever loop, who knows.
        test_forward_cursor(&mut db_ro, num_blobs, size_blob, 0);
    }

    #[test]
    fn cannot_open_with_different_compression() {
        // We first produce a file we can work on
        let now = Utc::now();
        let db_path = "/tmp".to_string();
        let db_base_name = format!(
            "{}-{}-{}",
            "test-cannot-open-with-different-compression",
            0,
            now.timestamp_nanos()
        );

        let mut db_conf = DbConf::new_with_parameters(
            db_path,
            db_base_name,
            LsmMode::LsmBackgroundMerger,
            LsmHandleMode::ReadWrite,
            None,
            LsmCompressionLib::ZStd,
        );

        let mut db: LsmDb = Default::default();

        let mut rc = db.initialize(db_conf.clone());
        assert_eq!(rc, Ok(()));

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // We now produce certain amount of blobs and persist them.
        let num_blobs = 10000_usize;
        let size_blob = 1 << 10; // 1 KB

        let mut prng: Mt64 = SeedableRng::seed_from_u64(0x41bd56915d5c7804);
        // Let's persist some blobs.
        test_persist_blobs(&mut db, num_blobs, size_blob, None, 0);

        test_disconnect(&mut db);

        // Now let's open without compression.
        db_conf.compression = LsmCompressionLib::NoCompression;

        let mut db: LsmDb = Default::default();

        rc = db.initialize(db_conf.clone());
        assert_eq!(rc, Ok(()));

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // Let us try to write one more blob...and fail because compression is different.
        let extra_blob = construct_random_blob(size_blob, &mut prng);
        rc = Disk::persist(
            &mut db,
            &Vec::from((num_blobs + 1).to_be_bytes()),
            &extra_blob,
        );
        assert_eq!(rc.unwrap_err(), LsmErrorCode::LsmMismatch);

        // But this also happens when reading.
        let Err(cursor) = Disk::cursor_open(&db) else { panic!("Unexpected result.") };
        assert_eq!(cursor, LsmErrorCode::LsmMismatch);

        test_disconnect(&mut db);

        // Now let's open with another compression but also in read-only mode.
        db_conf.compression = LsmCompressionLib::ZLib;
        db_conf.handle_mode = LsmHandleMode::ReadOnly;

        let mut db: LsmDb = Default::default();

        rc = db.initialize(db_conf);
        assert_eq!(rc, Ok(()));

        // Let's connect to it via a main memory handle.
        test_connect(&mut db);

        // But this also happens when reading.
        let Err(cursor) = Disk::cursor_open(&db) else { panic!("Unexpected result.") };
        assert_eq!(cursor, LsmErrorCode::LsmMismatch);
    }

    #[test]
    fn test_try_from_error_code() {
        assert_eq!(LsmErrorCode::LsmError, LsmErrorCode::try_from(1).unwrap());
        assert_eq!(LsmErrorCode::LsmBusy, LsmErrorCode::try_from(5).unwrap());
        assert_eq!(LsmErrorCode::LsmNoMem, LsmErrorCode::try_from(7).unwrap());
        assert_eq!(
            LsmErrorCode::LsmReadOnly,
            LsmErrorCode::try_from(8).unwrap()
        );
        assert_eq!(LsmErrorCode::LsmIOErr, LsmErrorCode::try_from(10).unwrap());
        assert_eq!(
            LsmErrorCode::LsmCorrupt,
            LsmErrorCode::try_from(11).unwrap()
        );
        assert_eq!(LsmErrorCode::LsmFull, LsmErrorCode::try_from(13).unwrap());
        assert_eq!(
            LsmErrorCode::LsmCantOpen,
            LsmErrorCode::try_from(14).unwrap()
        );
        assert_eq!(
            LsmErrorCode::LsmProtocol,
            LsmErrorCode::try_from(15).unwrap()
        );
        assert_eq!(LsmErrorCode::LsmMisuse, LsmErrorCode::try_from(21).unwrap());
        assert_eq!(
            LsmErrorCode::LsmMismatch,
            LsmErrorCode::try_from(50).unwrap()
        );
        assert_eq!(
            LsmErrorCode::LsmConversionErr,
            LsmErrorCode::try_from(55).unwrap()
        );
        assert_eq!(
            LsmErrorCode::LsmMetricCreation,
            LsmErrorCode::try_from(56).unwrap()
        );
        assert_eq!(
            LsmErrorCode::LsmMetricRegistration,
            LsmErrorCode::try_from(57).unwrap()
        );
        assert_eq!(
            LsmErrorCode::LsmMetricsEmpty,
            LsmErrorCode::try_from(58).unwrap()
        );
        assert_eq!(
            LsmErrorCode::LsmBgThreadUnavailable,
            LsmErrorCode::try_from(59).unwrap(),
        );
        assert_eq!(
            LsmErrorCode::LsmUnknownCode,
            LsmErrorCode::try_from(61).unwrap_err(),
        );
    }

    #[test]
    fn test_try_from_seek_op() {
        assert_eq!(
            LsmCursorSeekOp::LsmCursorSeekLe,
            LsmCursorSeekOp::try_from(-1).unwrap()
        );
        assert_eq!(
            LsmCursorSeekOp::LsmCursorSeekEq,
            LsmCursorSeekOp::try_from(0).unwrap()
        );
        assert_eq!(
            LsmCursorSeekOp::LsmCursorSeekGe,
            LsmCursorSeekOp::try_from(1).unwrap()
        );
        assert_eq!(
            LsmCursorSeekOp::try_from(2).unwrap_err(),
            LsmErrorCode::LsmUnknownCode
        );
    }

    #[test]
    fn test_try_from_safety() {
        assert_eq!(LsmSafety::Off, LsmSafety::try_from(0).unwrap());
        assert_eq!(LsmSafety::Normal, LsmSafety::try_from(1).unwrap());
        assert_eq!(LsmSafety::Full, LsmSafety::try_from(2).unwrap());
        assert_eq!(
            LsmSafety::try_from(-1).unwrap_err(),
            LsmErrorCode::LsmUnknownCode
        );
    }

    #[test]
    fn test_try_from_param() {
        assert_eq!(LsmParam::AutoFlush, LsmParam::try_from(1).unwrap());
        assert_eq!(LsmParam::PageSize, LsmParam::try_from(2).unwrap());
        assert_eq!(LsmParam::Safety, LsmParam::try_from(3).unwrap());
        assert_eq!(LsmParam::BlockSize, LsmParam::try_from(4).unwrap());
        assert_eq!(LsmParam::AutoWork, LsmParam::try_from(5).unwrap());
        assert_eq!(LsmParam::Mmap, LsmParam::try_from(7).unwrap());
        assert_eq!(LsmParam::UseLog, LsmParam::try_from(8).unwrap());
        assert_eq!(LsmParam::AutoMerge, LsmParam::try_from(9).unwrap());
        assert_eq!(LsmParam::MaxFreeList, LsmParam::try_from(10).unwrap());
        assert_eq!(LsmParam::MultipleProcesses, LsmParam::try_from(11).unwrap());
        assert_eq!(LsmParam::AutoCheckPoint, LsmParam::try_from(12).unwrap());
        assert_eq!(LsmParam::SetCompression, LsmParam::try_from(13).unwrap());
        assert_eq!(LsmParam::GetCompression, LsmParam::try_from(14).unwrap());
        assert_eq!(
            LsmParam::SetCompressionFactory,
            LsmParam::try_from(15).unwrap()
        );
        assert_eq!(LsmParam::ReadOnly, LsmParam::try_from(16).unwrap());
        assert_eq!(
            LsmParam::try_from(6).unwrap_err(),
            LsmErrorCode::LsmUnknownCode
        );
    }

    #[test]
    fn test_try_from_mode() {
        assert_eq!(
            LsmMode::LsmNoBackgroundThreads,
            LsmMode::try_from(0).unwrap()
        );
        assert_eq!(LsmMode::LsmBackgroundMerger, LsmMode::try_from(1).unwrap());
        assert_eq!(
            LsmMode::LsmBackgroundCheckpointer,
            LsmMode::try_from(2).unwrap()
        );
        assert_eq!(
            LsmMode::try_from(-1).unwrap_err(),
            LsmErrorCode::LsmUnknownCode
        );
    }

    #[test]
    fn test_try_from_compression_lib() {
        assert_eq!(
            LsmCompressionLib::NoCompression,
            LsmCompressionLib::try_from(1).unwrap()
        );
        assert_eq!(
            LsmCompressionLib::LZ4,
            LsmCompressionLib::try_from(10001).unwrap()
        );
        assert_eq!(
            LsmCompressionLib::ZLib,
            LsmCompressionLib::try_from(10002).unwrap()
        );
        assert_eq!(
            LsmCompressionLib::ZStd,
            LsmCompressionLib::try_from(10003).unwrap()
        );
        for id in 2..10000 {
            assert_eq!(
                LsmCompressionLib::try_from(id).unwrap_err(),
                LsmErrorCode::LsmMismatch
            )
        }
        assert_eq!(
            LsmCompressionLib::try_from(10005).unwrap_err(),
            LsmErrorCode::LsmMismatch
        );
    }

    #[test]
    fn test_try_from_info() {
        assert_eq!(LsmInfo::Lsm4KbPagesWritten, LsmInfo::try_from(1).unwrap());
        assert_eq!(LsmInfo::Lsm4KbPagesRead, LsmInfo::try_from(2).unwrap());
        assert_eq!(LsmInfo::LsmDbStructure, LsmInfo::try_from(3).unwrap());
        assert_eq!(LsmInfo::LsmLogStructure, LsmInfo::try_from(4).unwrap());
        assert_eq!(LsmInfo::LsmPageDumpAscii, LsmInfo::try_from(6).unwrap());
        assert_eq!(LsmInfo::LsmPageDumpHex, LsmInfo::try_from(7).unwrap());
        assert_eq!(LsmInfo::LsmCheckpointSize, LsmInfo::try_from(10).unwrap());
        assert_eq!(LsmInfo::LsmTreeSize, LsmInfo::try_from(11).unwrap());
        assert_eq!(LsmInfo::LsmCompressionId, LsmInfo::try_from(13).unwrap());
        assert_eq!(
            LsmInfo::try_from(5).unwrap_err(),
            LsmErrorCode::LsmUnknownCode
        );
    }
}
