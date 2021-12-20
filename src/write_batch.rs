use std::os::raw::c_int;

use librocksdb_sys::*;

define!(
    WriteBatch,
    rocksdb_writebatch_t,
    rocksdb_writebatch_create,
    rocksdb_writebatch_destroy
);

impl WriteBatch {
    pub fn clear(&mut self) {
        unsafe { rocksdb_writebatch_clear(self.inner) }
    }

    pub fn count(&self) -> c_int {
        unsafe { rocksdb_writebatch_count(self.inner) }
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        let key = key.as_ref();
        let value = value.as_ref();
        unsafe {
            rocksdb_writebatch_put(
                self.inner,
                key.as_ptr() as _,
                key.len(),
                value.as_ptr() as _,
                value.len(),
            )
        }
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        let key = key.as_ref();
        unsafe { rocksdb_writebatch_delete(self.inner, key.as_ptr() as _, key.len()) }
    }
}
