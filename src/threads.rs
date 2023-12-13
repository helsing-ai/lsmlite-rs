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
use std::convert::TryFrom;
use std::ffi::{CStr, CString};
use std::ptr::null_mut;
use std::sync::mpsc::TrySendError;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::compression::lz4::LsmLz4;
use crate::compression::zlib::LsmZLib;
use crate::compression::zstd::LsmZStd;
use crate::compression::Compression;
use crate::lsmdb::{
    lsm_checkpoint, lsm_close, lsm_config, lsm_info, lsm_new, lsm_open, lsm_work, BLOCK_SIZE_KB,
    MAX_CHECKPOINT_SIZE_KB, MIN_CHECKPOINT_SIZE_KB, PAGE_SIZE_B,
};
use crate::{
    DbConf, LsmBgWorker, LsmBgWorkerMessage, LsmBgWorkers, LsmCompressionLib, LsmDb, LsmErrorCode,
    LsmInfo, LsmMode, LsmParam,
};

// Do not modify these constants unless you know what you are doing.
pub(crate) const NUM_MERGE_SEGMENTS: i32 = 4;
const WORK_KB: i32 = 64 << 10; // X KiBs * 1024 = X MiB

/// A thread is spawn here with the right mode of execution (either merger or checkpointer).
/// For the time being we do not support two background threads.
impl LsmBgWorker {
    /// `n_segments` of the same age are searched and merged into a bigger one of
    /// a more recent age. If after doing the work less than `n_kb` KiB of work
    /// has been done, then the function continues with the next `n_segments`,
    /// otherwise it returns.
    fn merge(db: &LsmDb, n_segments: i32, n_kb: i32) {
        let mut rc: i32;
        // Let's do the work. This work is mostly trigger by the main writer when
        // the memory components have become large enough.
        let mut written_kb: i32 = 0;
        let mut overall_written_kb: i32 = 0;
        let mut old_tree_size: i32 = 0;
        let mut new_tree_size: i32 = 0;

        loop {
            unsafe {
                // Let's try to perform some work and see the situation afterwards.
                rc = lsm_work(db.db_handle, n_segments, n_kb, &mut written_kb);
                overall_written_kb += written_kb;

                // Anything different than ok (0), or busy (5) is wrong!
                if rc != 0 && rc != 5 {
                    let ec = LsmErrorCode::try_from(rc);
                    tracing::error!(
                            datafile = ?db.get_full_db_path(),
                            rc = ?ec,
                        "Error occurred while working on the datafile.",
                    );
                    return;
                }

                // After having performed some work, we query the sizes of the
                // main-memory components to decide whether another iteration will be done or not.
                rc = lsm_info(
                    db.db_handle,
                    LsmInfo::LsmTreeSize as i32,
                    &mut old_tree_size,
                    &mut new_tree_size,
                );

                // Something went wrong!
                if rc != 0 {
                    let ec = LsmErrorCode::try_from(rc);
                    tracing::error!(
                        datafile = ?db.get_full_db_path(),
                        rc = ?ec,
                        "Error occurred while obtaining segment information for background thread."
                    );
                    return;
                }

                if (old_tree_size == 0) || (overall_written_kb >= n_kb) {
                    // We get out of the loop under two conditions:
                    // 1. We have got rid of the old main-memory component, or
                    // 2. We have written enough information in the file.
                    break;
                }
            }
        }

        // We update the metric on the amount of data written.
        match &db.db_conf.metrics {
            None => {}
            Some(metrics) => metrics.work_kbs.observe(overall_written_kb as f64),
        }
    }

    /// Checkpoint the database (to disk). Making durable all information found
    /// until that moment. This function updates the database file header and syncing
    /// the contents to disk.
    fn checkpoint(db: &LsmDb) {
        // Let's checkpoint.
        let mut rc: i32;
        let mut written_kb: i32 = 0;
        let mut overall_written_kb: i32 = 0;
        let mut amount_volatile_data: i32 = -1;

        loop {
            unsafe {
                rc = lsm_info(
                    db.db_handle,
                    LsmInfo::LsmCheckpointSize as i32,
                    &mut amount_volatile_data,
                );

                if rc != 0 {
                    let ec = LsmErrorCode::try_from(rc);
                    tracing::error!(
                        datafile = ?db.get_full_db_path(),
                        rc = ?ec,
                        "Error occurred while obtaining last checkpoint information.",
                    );
                    return;
                }

                // We avoid running the checkpointer procedure to often as it's expensive.
                if amount_volatile_data >= MIN_CHECKPOINT_SIZE_KB {
                    rc = lsm_checkpoint(db.db_handle, &mut written_kb);

                    overall_written_kb += written_kb;

                    // Anything different than ok (0), or busy (5) is wrong!
                    if rc != 0 && rc != 5 {
                        let ec = LsmErrorCode::try_from(rc);
                        tracing::error!(
                            datafile = ?db.get_full_db_path(),
                            rc = ?ec,
                            "Error occurred while checkpointing the database file.",
                        );
                        return;
                    }
                } else {
                    // If the amount of data not yet committed is within thresholds, then we
                    // get out.
                    break;
                }
            }
        }

        // We update the metric on the amount of data written.
        match &db.db_conf.metrics {
            None => {}
            Some(metrics) => metrics.checkpoint_kbs.observe(overall_written_kb as f64),
        }
    }

    /// This frees up the resources occupied by the thread handle. Not calling this function
    /// when the thread finishes, or encounters a fatal problem, is a leak of resources.
    fn close_thread_connection(db: &mut LsmDb) {
        let rc: i32;
        unsafe {
            rc = lsm_close(db.db_handle);
        }

        if rc != 0 {
            let ec = LsmErrorCode::try_from(rc);
            tracing::error!(
                datafile = ?db.get_full_db_path(),
                rc = ?ec,
                "Error occurred while closing a thread handle.",
            );
        }

        // We reset the pointer once we know we were able to cleanly close the database.
        db.db_handle = null_mut();
    }

    /// Produces a new background worker for the corresponding data segment.
    fn new(
        master_db_conf: &DbConf,
        master_fqn: &CStr,
        receiver: Arc<Mutex<mpsc::Receiver<LsmBgWorkerMessage>>>,
    ) -> LsmBgWorker {
        // Every thread produces its handle to the given database so that it can
        // always work on it. That is, the handle is valid for as long as the thread
        // is alive.
        let mut rc: i32;
        let mut db: LsmDb = Default::default();
        // These settings are needed otherwise the newly spawn thread does not
        // know the database it has to connect to (which should be the same one
        // the writer is currently working on.
        db.db_conf = master_db_conf.clone();
        db.db_fq_name = CString::from(master_fqn);
        db.initialized = true;

        unsafe {
            // Get a new handle to connect to the database on disk.
            rc = lsm_new(null_mut(), &mut db.db_handle);

            if rc != 0 {
                let ec = LsmErrorCode::try_from(rc);
                tracing::error!(
                    datafile = ?db.get_full_db_path(),
                    rc = ?ec,
                    "Error occurred while allocating new handle for background thread.",
                );

                LsmBgWorker::close_thread_connection(&mut db);
                return LsmBgWorker { thread: None };
            }
        }

        // Once the handle is valid, we configure it according to the given mode.
        match db.db_conf.mode {
            LsmMode::LsmNoBackgroundThreads => {
                // This mode makes no sense in a multi-threading setup. We get out
                // and free up resources.
                LsmBgWorker::close_thread_connection(&mut db);
                return LsmBgWorker { thread: None };
            }
            LsmMode::LsmBackgroundMerger => {
                // We now set this connection to auto-checkpoint after MAX_CHECKPOINT_SIZE_KB
                // have been written to the database.
                let after_bytes: i32 = MAX_CHECKPOINT_SIZE_KB;
                unsafe {
                    rc = lsm_config(db.db_handle, LsmParam::AutoCheckPoint as i32, &after_bytes);
                }

                if rc != 0 {
                    let ec = LsmErrorCode::try_from(rc);
                    tracing::error!(
                        datafile = ?db.get_full_db_path(),
                        rc = ?ec,
                        "Error occurred while setting thread handle parameter.",
                    );

                    LsmBgWorker::close_thread_connection(&mut db);
                    return LsmBgWorker { thread: None };
                }
            }
            LsmMode::LsmBackgroundCheckpointer => {
                // Disable auto work, which will be delegated to a thread.
                let autowork: i32 = 0;
                unsafe {
                    rc = lsm_config(db.db_handle, LsmParam::AutoWork as i32, &autowork);
                }

                if rc != 0 {
                    tracing::error!(
                        datafile = ?db.get_full_db_path(),
                        rc = ?LsmErrorCode::try_from(rc),
                        "Error occurred while setting thread handle parameter.",
                    );

                    LsmBgWorker::close_thread_connection(&mut db);
                    return LsmBgWorker { thread: None };
                }
            }
        }

        // If we are instructed to configure compression, we do it now. Setting the
        // compression can be done only when the database was created, once set,
        // trying to do it again or not setting it to the same compression scheme
        // will be considered an error (LsmErrorCode::LsmMismatch).
        match db.db_conf.compression {
            LsmCompressionLib::NoCompression => {}
            LsmCompressionLib::LZ4 => {
                let lz4 = LsmLz4::new();
                db.db_compress = lz4.get_compression_methods().ok();
            }
            LsmCompressionLib::ZLib => {
                let zlib = LsmZLib::new();
                db.db_compress = zlib.get_compression_methods().ok();
            }
            LsmCompressionLib::ZStd => {
                let zstd = LsmZStd::new();
                db.db_compress = zstd.get_compression_methods().ok();
            }
        };

        // Only if the compression library is defined we pass it onto
        // the engine. Otherwise no compression whatsoever.
        if let Some(lsm_compress) = db.db_compress.as_ref() {
            unsafe {
                rc = lsm_config(db.db_handle, LsmParam::SetCompression as i32, lsm_compress);
            }

            if rc != 0 {
                tracing::error!(
                    datafile = ?db.get_full_db_path(),
                    rc = ?LsmErrorCode::try_from(rc),
                    "Error occurred while setting compression hooks.",
                );

                LsmBgWorker::close_thread_connection(&mut db);
                return LsmBgWorker { thread: None };
            }
        }

        // Whichever worker connection sets page size the same.
        unsafe {
            rc = lsm_config(db.db_handle, LsmParam::PageSize as i32, &PAGE_SIZE_B);
        }

        if rc != 0 {
            tracing::error!(
                datafile = ?db.get_full_db_path(),
                rc = ?LsmErrorCode::try_from(rc),
                "Error occurred while setting thread handle parameter.",
            );

            LsmBgWorker::close_thread_connection(&mut db);
            return LsmBgWorker { thread: None };
        }

        // Whichever worker connection sets the block size the same.
        unsafe {
            rc = lsm_config(db.db_handle, LsmParam::BlockSize as i32, &BLOCK_SIZE_KB);
        }

        if rc != 0 {
            tracing::error!(
                datafile = ?db.get_full_db_path(),
                rc = ?LsmErrorCode::try_from(rc),
                "Error occurred while setting thread handle parameter.",
            );

            LsmBgWorker::close_thread_connection(&mut db);
            return LsmBgWorker { thread: None };
        }

        // Whichever worker connection disables multi-process support to
        // improve performance (no OS advisory locks are used to synchronize access
        // to the database file).
        let multi_process: i32 = 0;
        unsafe {
            rc = lsm_config(
                db.db_handle,
                LsmParam::MultipleProcesses as i32,
                &multi_process,
            );
        }

        if rc != 0 {
            tracing::error!(
                datafile = ?db.get_full_db_path(),
                rc = ?LsmErrorCode::try_from(rc),
                "Error occurred while setting thread handle parameter.",
            );

            LsmBgWorker::close_thread_connection(&mut db);
            return LsmBgWorker { thread: None };
        }

        // Whichever worker connection, do not mmap any part of the file.
        let mmap_size: i32 = 0;
        unsafe {
            rc = lsm_config(db.db_handle, LsmParam::Mmap as i32, &mmap_size);
        }

        if rc != 0 {
            tracing::error!(
                datafile = ?db.get_full_db_path(),
                rc = ?LsmErrorCode::try_from(rc),
                "Error occurred while setting thread handle parameter.",
            );

            LsmBgWorker::close_thread_connection(&mut db);
            return LsmBgWorker { thread: None };
        }

        // We finally open the handle to the database.
        unsafe {
            rc = lsm_open(db.db_handle, db.db_fq_name.as_ptr());

            if rc != 0 {
                tracing::error!(
                    datafile = ?db.get_full_db_path(),
                    rc = ?LsmErrorCode::try_from(rc),
                    "Error occurred while opening the database file. Exiting background thread.",
                );
                LsmBgWorker::close_thread_connection(&mut db);
                return LsmBgWorker { thread: None };
            }
        }

        let thread = thread::spawn(move || loop {
            // The thread will yield if no message is received.
            let message = receiver.lock().unwrap().recv().unwrap();

            // We process the kind of message we got.
            match message {
                LsmBgWorkerMessage::Checkpoint => LsmBgWorker::checkpoint(&db),
                LsmBgWorkerMessage::Merge => LsmBgWorker::merge(&db, NUM_MERGE_SEGMENTS, WORK_KB),
                LsmBgWorkerMessage::Stop => {
                    // When we stop, we free up the resources of the handle as it won't be used
                    // any longer.
                    LsmBgWorker::close_thread_connection(&mut db);
                    break;
                }
            }
        });

        LsmBgWorker {
            thread: Some(thread),
        }
    }
}

impl LsmBgWorkers {
    /// This creates the actual thread pool.
    pub fn new(master_db_conf: &DbConf, master_fqn: &CStr, id: usize) -> LsmBgWorkers {
        tracing::info!(
            thread_id = id,
            "Spawning one thread to take care of background tasks",
        );

        // This is the communication channel among threads.
        let (tx, rx) = mpsc::sync_channel(1);
        let receiver = Arc::new(Mutex::new(rx));
        let bg_thread = LsmBgWorker::new(master_db_conf, master_fqn, receiver);
        if master_db_conf.mode != LsmMode::LsmNoBackgroundThreads && bg_thread.thread.is_none() {
            tracing::error!(
                "Spawning background thread failed. Changing execution mode to \
            single-threaded for the current database segment."
            );
        }
        Self {
            bg_threads: vec![bg_thread],
            sender: Some(tx),
            id,
        }
    }

    /// This is how we execute a worker thread, by sending it the right message
    pub fn execute(&self, message: LsmBgWorkerMessage) -> Result<(), LsmErrorCode> {
        match self.sender.as_ref().unwrap().try_send(message) {
            Ok(_) => {}
            Err(e) => match e {
                TrySendError::Full(_) => {
                    // This is fine. If background thread is busy, we don't
                    // add overheads to it. Perhaps next time it will be free.
                }
                TrySendError::Disconnected(_) => {
                    // TODO: Respawn background thread(s) if this happens.
                    tracing::error!(
                        "Background thread is not reachable. No background task \
                    will be performed."
                    );
                    return Err(LsmErrorCode::LsmBgThreadUnavailable);
                }
            },
        }
        Ok(())
    }

    /// This is how we shutdown all worker thread(s).
    pub fn shutdown(&mut self) {
        // If the channel of communication is down, we get out. We assume that the
        // background threads are not running.
        if self.sender.is_none() {
            return;
        }

        tracing::info!(
            thread_id = self.id,
            "Sending stop message to background thread.",
        );
        for _ in &self.bg_threads {
            if self
                .sender
                .as_ref()
                .unwrap()
                .send(LsmBgWorkerMessage::Stop)
                .is_err()
            {
                tracing::warn!(
                    thread_id = self.id,
                    "Unable to send termination message to background thread. \
                Most probably it has been terminated already.",
                );
            }
        }

        for worker in &mut self.bg_threads {
            if let Some(thread) = worker.thread.take() {
                tracing::info!(thread_id = self.id, "Shutting down background thread.");
                thread
                    .join()
                    .expect("Couldn't join on the associated background thread.");
            }
        }
    }
}

/// Being idiomatic, this is automatic shutting down of worker threads.
impl Drop for LsmBgWorkers {
    fn drop(&mut self) {
        self.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use crate::{DbConf, LsmBgWorkers, LsmMode};
    use chrono::Utc;
    use std::ffi::CString;

    #[test]
    fn create_background_thread_with_no_thread() {
        let now = Utc::now();
        let db_path = "/tmp".to_string();
        let db_fqn_name = format!(
            "{}/test-create-background-thread-with-no-thread-{}",
            db_path,
            now.timestamp_nanos_opt().unwrap()
        );
        let conf = DbConf {
            mode: LsmMode::LsmNoBackgroundThreads,
            ..Default::default()
        };
        let workers = LsmBgWorkers::new(&conf, &CString::new(db_fqn_name).unwrap(), 0);
        assert!(workers.bg_threads[0].thread.is_none());
    }

    #[test]
    fn create_background_thread_merger() {
        let now = Utc::now();
        let db_path = "/tmp".to_string();
        let db_fqn_name = format!(
            "{}/test-create-background-thread-merger-{}",
            db_path,
            now.timestamp_nanos_opt().unwrap()
        );
        let conf = DbConf {
            mode: LsmMode::LsmBackgroundMerger,
            ..Default::default()
        };
        let workers = LsmBgWorkers::new(&conf, &CString::new(db_fqn_name).unwrap(), 0);
        assert!(workers.bg_threads[0].thread.is_some());
    }

    #[test]
    fn create_background_thread_checkpointer() {
        let now = Utc::now();
        let db_path = "/tmp".to_string();
        let db_fqn_name = format!(
            "{}/test-create-background-thread-checkpointer-{}",
            db_path,
            now.timestamp_nanos_opt().unwrap()
        );
        let conf = DbConf {
            mode: LsmMode::LsmBackgroundCheckpointer,
            ..Default::default()
        };
        let workers = LsmBgWorkers::new(&conf, &CString::new(db_fqn_name).unwrap(), 0);
        assert!(workers.bg_threads[0].thread.is_some());
    }
}
