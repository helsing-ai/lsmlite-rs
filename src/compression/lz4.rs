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
use lz4_sys::{LZ4_compressBound, LZ4_compress_default, LZ4_decompress_safe};
use std::ffi::{c_char, c_void};
use std::ptr::null_mut;

use crate::compression::{lsm_compress, Compression};
use crate::{LsmCompressionLib, LsmErrorCode};

/// This encloses the methods of the compression library.
#[derive(Clone, Debug)]
pub struct LsmLz4 {
    compression: lsm_compress,
}

impl LsmLz4 {
    /// This function produces a struct containing all relevant
    /// function pointers set to lz4 functions.
    pub fn new() -> Self {
        Self {
            compression: lsm_compress {
                ctx: null_mut(),
                bound: Some(LsmLz4::compress_bound_lz4),
                compress: Some(LsmLz4::compress_lz4),
                uncompress: Some(LsmLz4::uncompress_lz4),
                free: None,
                // This conversion is infallible.
                id: u32::try_from(LsmCompressionLib::LZ4 as i32).unwrap(),
            },
        }
    }

    #[no_mangle]
    unsafe extern "C" fn compress_bound_lz4(_: *const c_void, input_size: i32) -> i32 {
        LZ4_compressBound(input_size)
    }

    #[no_mangle]
    unsafe extern "C" fn compress_lz4(
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

        // Should be >= LZ4_compressBound(src_size).
        let buffer_size = *written_bytes_p;

        // This will succeed if and only if `dst` has been allocated with
        // the proper size >= LZ4_compressBound(src_size), otherwise it will return
        // zero and the contents of the buffer can be considered trash.
        let written_bytes = LZ4_compress_default(src, dst, src_size, buffer_size);
        if written_bytes <= 0 {
            LsmErrorCode::LsmError as i32
        } else {
            *written_bytes_p = written_bytes;
            // This is LSM_OK.
            0
        }
    }

    #[no_mangle]
    unsafe extern "C" fn uncompress_lz4(
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

        let buffer_size = *written_bytes_p;

        // This will succeed if and only if `dst` has been allocated with
        // the proper size, otherwise it will return a negative value and the
        // contents of the buffer can be considered trash.
        let written_bytes = LZ4_decompress_safe(src, dst, src_size, buffer_size);
        if written_bytes < 0 {
            LsmErrorCode::LsmError as i32
        } else {
            *written_bytes_p = written_bytes;
            // This is LSM_OK.
            0
        }
    }
}

/// This allows to get the methods that `lsm1` needs.
impl Compression for LsmLz4 {
    fn get_compression_methods(&self) -> Result<lsm_compress, LsmErrorCode> {
        Ok(self.compression)
    }
}
