use std::marker::PhantomData;
use std::os::raw::c_char;
use std::ptr::null_mut;

use librocksdb_sys::*;

use crate::{Error, Slice};

pub struct Iterator<'a> {
    inner: *mut rocksdb_iterator_t,
    _marker: PhantomData<&'a ()>,
}

impl<'a> Iterator<'a> {
    pub fn new(inner: *mut rocksdb_iterator_t) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    pub fn valid(&self) -> bool {
        unsafe { rocksdb_iter_valid(self.inner) != 0 }
    }

    pub fn get_error(&self) -> Option<Error> {
        let mut errptr: *mut c_char = null_mut();
        unsafe { rocksdb_iter_get_error(self.inner, &mut errptr) };
        if !errptr.is_null() {
            Some(Error::new(errptr))
        } else {
            None
        }
    }

    pub fn seek(&mut self, key: impl AsRef<[u8]>) {
        let key = key.as_ref();
        unsafe { rocksdb_iter_seek(self.inner, key.as_ptr() as _, key.len()) }
    }

    pub fn seek_for_prev(&mut self, key: impl AsRef<[u8]>) {
        let key = key.as_ref();
        unsafe { rocksdb_iter_seek_for_prev(self.inner, key.as_ptr() as _, key.len()) }
    }

    pub fn seek_to_first(&mut self) {
        unsafe { rocksdb_iter_seek_to_first(self.inner) }
    }

    pub fn seek_to_last(&mut self) {
        unsafe { rocksdb_iter_seek_to_last(self.inner) }
    }

    pub fn next(&mut self) {
        unsafe { rocksdb_iter_next(self.inner) }
    }

    pub fn prev(&mut self) {
        unsafe { rocksdb_iter_prev(self.inner) }
    }

    // REQUIRES: valid()
    pub unsafe fn key(&self) -> Slice<'_> {
        let mut len: usize = 0;
        Slice::new(rocksdb_iter_key(self.inner, &mut len), len)
    }

    // REQUIRES: valid()
    pub unsafe fn value(&self) -> Slice<'_> {
        let mut len: usize = 0;
        Slice::new(rocksdb_iter_value(self.inner, &mut len), len)
    }
}

unsafe impl<'a> Send for Iterator<'a> {}

unsafe impl<'a> Sync for Iterator<'a> {}

impl<'a> Drop for Iterator<'a> {
    fn drop(&mut self) {
        unsafe { rocksdb_iter_destroy(self.inner) }
    }
}
