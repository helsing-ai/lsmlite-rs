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
pub(crate) mod lz4;
pub(crate) mod zlib;
pub(crate) mod zstd;

use crate::LsmErrorCode;

use std::ffi::{c_char, c_void};

/// Unlike the other lsm structs. This cannot be opaque as its fields
/// need to be set on our side, and not on lsm's side, see `lz4.rs`
/// for a reference implementation of these hooks. Keep in mind that
/// the order in which the elements are declared is relevant.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct lsm_compress {
    pub(crate) ctx: *mut c_void,
    pub(crate) id: u32,
    pub(crate) bound: Option<unsafe extern "C" fn(ctx: *const c_void, src_len: i32) -> i32>,
    pub(crate) compress: Option<
        unsafe extern "C" fn(
            ctx: *const c_void,
            dst: *mut c_char,
            dst_size: *mut i32,
            src: *const c_char,
            src_size: i32,
        ) -> i32,
    >,
    pub(crate) uncompress: Option<
        unsafe extern "C" fn(
            ctx: *const c_void,
            dst: *mut c_char,
            dst_size: *mut i32,
            src: *const c_char,
            src_size: i32,
        ) -> i32,
    >,
    pub(crate) free: Option<unsafe extern "C" fn(ctx: *const c_void)>,
}

/// In the future there could be more compression/encryption libraries.
/// We do not really care about their implementation, but in the end they
/// have to produce an element of type `lms_compress`. So this trait
/// allows us to simply rely on a method that allows us to get such element.
pub trait Compression {
    fn get_compression_methods(&self) -> Result<lsm_compress, LsmErrorCode>;
}
