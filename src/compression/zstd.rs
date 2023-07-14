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
use std::ffi::{c_char, c_int, c_void};
use std::ptr::null_mut;
use zstd_sys::{
    ZSTD_compress, ZSTD_compressBound, ZSTD_decompress, ZSTD_isError, ZSTD_CLEVEL_DEFAULT,
};

use crate::compression::{lsm_compress, Compression};
use crate::{LsmCompressionLib, LsmErrorCode};

/// This encloses the methods of the compression library.
#[derive(Clone, Debug)]
pub struct LsmZStd {
    compression: lsm_compress,
}

impl LsmZStd {
    /// This function produces a struct containing all relevant
    /// function pointers set to zstd functions.
    pub fn new() -> Self {
        Self {
            compression: lsm_compress {
                ctx: null_mut(),
                // This conversion is infallible.
                id: u32::try_from(LsmCompressionLib::ZStd as i32).unwrap(),
                bound: Some(LsmZStd::compress_bound_zstd),
                compress: Some(LsmZStd::compress_zstd),
                uncompress: Some(LsmZStd::uncompress_zstd),
                free: None,
            },
        }
    }

    #[no_mangle]
    unsafe extern "C" fn compress_bound_zstd(_: *const c_void, input_size: i32) -> i32 {
        ZSTD_compressBound(input_size as usize) as i32
    }

    #[no_mangle]
    unsafe extern "C" fn compress_zstd(
        _: *const c_void,
        dst: *mut c_char,
        written_bytes_p: *mut i32,
        src: *const c_char,
        src_size: i32,
    ) -> i32 {
        // If we cannot write to this address, then we get out signaling that
        // we performed no work.
        if written_bytes_p.is_null() {
            return LsmErrorCode::LsmError as i32;
        }

        // The buffer we write to is of this size.
        let buffer_size: usize = *written_bytes_p as usize;

        // Let's do it.
        let written_bytes: usize = ZSTD_compress(
            dst as *mut c_void,
            buffer_size,
            src as *const c_void,
            src_size as usize,
            ZSTD_CLEVEL_DEFAULT as c_int,
        );

        // Non-zero iff the code is an error.
        if ZSTD_isError(written_bytes) != 0 {
            LsmErrorCode::LsmError as i32
        } else {
            *written_bytes_p = written_bytes as i32;
            0
        }
    }

    #[no_mangle]
    unsafe extern "C" fn uncompress_zstd(
        _: *const c_void,
        dst: *mut c_char,
        written_bytes_p: *mut i32,
        src: *const c_char,
        src_size: i32,
    ) -> i32 {
        // If we cannot write to this address, then we get out signaling that
        // we performed no work.
        if written_bytes_p.is_null() {
            return LsmErrorCode::LsmError as i32;
        }

        // The buffer we write to is of this size.
        let buffer_size: usize = *written_bytes_p as usize;

        // Let's do it.
        let written_bytes: usize = ZSTD_decompress(
            dst as *mut c_void,
            buffer_size,
            src as *const c_void,
            src_size as usize,
        );

        // Non-zero iff the code is an error.
        if ZSTD_isError(written_bytes) != 0 {
            LsmErrorCode::LsmError as i32
        } else {
            *written_bytes_p = written_bytes as i32;
            0
        }
    }
}

/// This allows to get the methods that `lsm1` needs.
impl Compression for LsmZStd {
    fn get_compression_methods(&self) -> Result<lsm_compress, LsmErrorCode> {
        Ok(self.compression)
    }
}
