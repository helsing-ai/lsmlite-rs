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
use libz_sys::{compress, compressBound, uLong, uncompress, Bytef, Z_OK};
use std::ffi::{c_char, c_int, c_ulong, c_void};
use std::ptr::null_mut;

use crate::compression::{lsm_compress, Compression};
use crate::{LsmCompressionLib, LsmErrorCode};

/// This encloses the methods of the compression library.
#[derive(Clone, Debug)]
pub struct LsmZLib {
    compression: lsm_compress,
}

impl LsmZLib {
    /// This function produces a struct containing all relevant
    /// function pointers set to zlib functions.
    pub fn new() -> Self {
        Self {
            compression: lsm_compress {
                ctx: null_mut(),
                // This conversion is infallible.
                id: u32::try_from(LsmCompressionLib::ZLib as i32).unwrap(),
                bound: Some(LsmZLib::compress_bound_zlib),
                compress: Some(LsmZLib::compress_zlib),
                uncompress: Some(LsmZLib::uncompress_zlib),
                free: None,
            },
        }
    }

    #[no_mangle]
    unsafe extern "C" fn compress_bound_zlib(_: *const c_void, input_size: i32) -> i32 {
        compressBound(input_size.try_into().unwrap())
            .try_into()
            .unwrap()
    }

    #[no_mangle]
    unsafe extern "C" fn compress_zlib(
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
        let mut buffer_size: uLong = *written_bytes_p as uLong;

        // Let's do it.
        let rc: c_int = compress(
            dst as *mut Bytef,
            &mut buffer_size,
            src as *const Bytef,
            src_size as c_ulong,
        );
        if rc != Z_OK {
            LsmErrorCode::LsmError as i32
        } else {
            *written_bytes_p = buffer_size as i32;
            0
        }
    }

    #[no_mangle]
    unsafe extern "C" fn uncompress_zlib(
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
        let mut buffer_size: uLong = *written_bytes_p as uLong;

        // Let's do it.
        let rc: c_int = uncompress(
            dst as *mut Bytef,
            &mut buffer_size,
            src as *const Bytef,
            src_size.try_into().unwrap(),
        );

        if rc != Z_OK {
            LsmErrorCode::LsmError as i32
        } else {
            *written_bytes_p = buffer_size as i32;
            0
        }
    }
}

/// This allows to get the methods that `lsm1` needs.
impl Compression for LsmZLib {
    fn get_compression_methods(&self) -> Result<lsm_compress, LsmErrorCode> {
        Ok(self.compression)
    }
}
