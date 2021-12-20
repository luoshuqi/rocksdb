use std::ascii::escape_default;
use std::fmt::{Debug, Formatter, Write};
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::slice::{from_raw_parts, from_raw_parts_mut};

use crate::free;

pub struct Bytes {
    ptr: *mut c_char,
    len: usize,
}

impl Bytes {
    pub(crate) fn new(ptr: *mut c_char, len: usize) -> Self {
        debug_assert!(!ptr.is_null());
        Self { ptr, len }
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr as _, self.len) }
    }
}

impl AsMut<[u8]> for Bytes {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.ptr as _, self.len) }
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format_slice(self.as_ref(), f)
    }
}

impl Drop for Bytes {
    fn drop(&mut self) {
        free(self.ptr)
    }
}

pub struct Slice<'a> {
    ptr: *const c_char,
    len: usize,
    _marker: PhantomData<&'a ()>,
}

impl<'a> Slice<'a> {
    pub(crate) fn new(ptr: *const c_char, len: usize) -> Self {
        debug_assert!(!ptr.is_null());
        let _marker = PhantomData;
        Self { ptr, len, _marker }
    }
}

impl<'a> AsRef<[u8]> for Slice<'a> {
    fn as_ref(&self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr as _, self.len) }
    }
}

impl<'a> Debug for Slice<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format_slice(self.as_ref(), f)
    }
}

fn format_slice(s: &[u8], f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "\"")?;
    for byte in s.iter().flat_map(|&b| escape_default(b)) {
        f.write_char(byte as char)?;
    }
    write!(f, "\"")
}
