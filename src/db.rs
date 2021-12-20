use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr::null_mut;

use librocksdb_sys::*;

use crate::options::{Options, WriteOptions};
use crate::snapshot::{OwnedSnapshot, ReleaseSnapshot};
use crate::{Bytes, Error, FlushOptions, ReadOptions, Result, WriteBatch};

pub struct DB {
    pub(crate) inner: *mut rocksdb_t,
}

impl DB {
    pub fn open(options: &Options, name: &str) -> Result<Self> {
        let name = CString::new(name).unwrap();
        Ok(Self {
            inner: ffi!(rocksdb_open(options.inner, name.as_ptr())),
        })
    }

    pub fn destroy(options: &Options, name: &str) -> Result<()> {
        let name = CString::new(name).unwrap();
        Ok(ffi!(rocksdb_destroy_db(options.inner, name.as_ptr())))
    }

    pub fn repair(options: &Options, name: &str) -> Result<()> {
        let name = CString::new(name).unwrap();

        Ok(ffi!(rocksdb_repair_db(options.inner, name.as_ptr())))
    }

    pub fn create_iterator(&self, options: &ReadOptions) -> crate::Iterator {
        crate::Iterator::new(unsafe { rocksdb_create_iterator(self.inner, options.inner) })
    }

    pub fn get(&self, options: &ReadOptions, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let mut len: usize = 0;
        let key = key.as_ref();
        let value = ffi!(rocksdb_get(
            self.inner,
            options.inner,
            key.as_ptr() as _,
            key.len(),
            &mut len
        ));
        if !value.is_null() {
            Ok(Some(Bytes::new(value, len)))
        } else {
            Ok(None)
        }
    }

    pub fn multi_get(
        &self,
        options: &ReadOptions,
        keys: &[impl AsRef<[u8]>],
    ) -> Vec<Result<Option<Bytes>>> {
        let num_keys = keys.len();
        let mut keys_list = Vec::with_capacity(num_keys);
        let mut keys_list_sizes = Vec::with_capacity(num_keys);
        let mut values_list: Vec<*mut c_char> = vec![null_mut(); num_keys];
        let mut values_list_sizes: Vec<usize> = vec![0; num_keys];
        let mut errs: Vec<*mut c_char> = vec![null_mut(); num_keys];

        for key in keys {
            let key = key.as_ref();
            keys_list.push(key.as_ptr() as *const c_char);
            keys_list_sizes.push(key.len());
        }

        unsafe {
            rocksdb_multi_get(
                self.inner,
                options.inner,
                num_keys,
                keys_list.as_ptr(),
                keys_list_sizes.as_ptr(),
                values_list.as_mut_ptr(),
                values_list_sizes.as_mut_ptr(),
                errs.as_mut_ptr(),
            );
        }

        let mut ret = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let err = errs[i];
            let v = if err.is_null() {
                let value = values_list[i];
                if !value.is_null() {
                    Ok(Some(Bytes::new(value, values_list_sizes[i])))
                } else {
                    Ok(None)
                }
            } else {
                Err(Error::new(err))
            };
            ret.push(v);
        }
        ret
    }

    pub fn put(
        &self,
        options: &WriteOptions,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        Ok(ffi!(rocksdb_put(
            self.inner,
            options.inner,
            key.as_ptr() as _,
            key.len(),
            value.as_ptr() as _,
            value.len()
        )))
    }

    pub fn write(&self, options: &WriteOptions, batch: &WriteBatch) -> Result<()> {
        Ok(ffi!(rocksdb_write(self.inner, options.inner, batch.inner)))
    }

    pub fn delete(&self, options: &WriteOptions, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        Ok(ffi!(rocksdb_delete(
            self.inner,
            options.inner,
            key.as_ptr() as _,
            key.len()
        )))
    }

    pub fn flush(&self, options: &FlushOptions) -> Result<()> {
        Ok(ffi!(rocksdb_flush(self.inner, options.inner)))
    }

    pub fn create_snapshot(&self) -> OwnedSnapshot<'_, Self> {
        let inner = unsafe { rocksdb_create_snapshot(self.inner) };
        debug_assert!(!inner.is_null());
        OwnedSnapshot { inner, db: self }
    }
}

impl ReleaseSnapshot for DB {
    fn release_snapshot(&self, snapshot: *const rocksdb_snapshot_t) {
        unsafe { rocksdb_release_snapshot(self.inner, snapshot) }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        unsafe { rocksdb_close(self.inner) }
    }
}

unsafe impl Send for DB {}

unsafe impl Sync for DB {}

#[cfg(test)]
mod tests {
    use crate::options::tests::DBPath;
    use crate::snapshot::NullSnapshot;
    use crate::{Options, ReadOptions, WriteBatch, WriteOptions, DB};

    #[test]
    fn test_open() {
        let mut options = Options::new();
        options.set_create_if_missing(true);
        options.set_error_if_exists(true);
        let path = DBPath::new();
        assert!(DB::open(&options, path.as_ref()).is_ok());
    }

    #[test]
    fn test_destroy() {
        let mut options = Options::new();
        options.set_create_if_missing(true);
        let path = DBPath::new();
        assert!(DB::open(&options, path.as_ref()).is_ok());

        options.set_create_if_missing(false);
        assert!(DB::open(&options, path.as_ref()).is_ok());

        assert!(DB::destroy(&options, path.as_ref()).is_ok());
        assert!(DB::open(&options, path.as_ref()).is_err());
    }

    fn open_new_db(name: &str) -> DB {
        let mut options = Options::new();
        options.set_create_if_missing(true);
        options.set_error_if_exists(true);
        DB::open(&options, name).unwrap()
    }

    #[test]
    fn test_get_put_delete() {
        let path = DBPath::new();
        let db = open_new_db(path.as_ref());

        let read_op = ReadOptions::new();
        assert!(db.get(&read_op, "foo").unwrap().is_none());

        let write_op = WriteOptions::new();
        assert!(db.put(&write_op, "foo", "bar").is_ok());
        assert_eq!(db.get(&read_op, "foo").unwrap().unwrap().as_ref(), b"bar");

        assert!(db.delete(&write_op, "foo").is_ok());
        assert!(db.get(&read_op, "foo").unwrap().is_none());
    }

    #[test]
    fn test_multi_get() {
        let path = DBPath::new();
        let db = open_new_db(path.as_ref());

        let read_op = ReadOptions::new();
        let values = db.multi_get(&read_op, &["foo", "bar"]);
        assert!(values[0].as_ref().unwrap().is_none());
        assert!(values[1].as_ref().unwrap().is_none());

        let write_op = WriteOptions::new();
        db.put(&write_op, "foo", "bar").unwrap();
        db.put(&write_op, "bar", "baz").unwrap();
        let values = db.multi_get(&read_op, &["foo", "bar"]);
        assert_eq!(
            values[0].as_ref().unwrap().as_ref().unwrap().as_ref(),
            b"bar"
        );
        assert_eq!(
            values[1].as_ref().unwrap().as_ref().unwrap().as_ref(),
            b"baz"
        );
    }

    #[test]
    fn test_write_batch() {
        let path = DBPath::new();
        let db = open_new_db(path.as_ref());

        let mut wb = WriteBatch::new();
        assert_eq!(wb.count(), 0);
        wb.put("foo", "bar");
        wb.put("bar", "baz");
        assert_eq!(wb.count(), 2);

        let write_op = WriteOptions::new();
        assert!(db.write(&write_op, &wb).is_ok());

        let read_op = ReadOptions::new();
        let values = db.multi_get(&read_op, &["foo", "bar"]);
        assert_eq!(
            values[0].as_ref().unwrap().as_ref().unwrap().as_ref(),
            b"bar"
        );
        assert_eq!(
            values[1].as_ref().unwrap().as_ref().unwrap().as_ref(),
            b"baz"
        );

        wb.clear();
        assert_eq!(wb.count(), 0);
        wb.delete("foo");
        assert!(db.write(&write_op, &wb).is_ok());

        let values = db.multi_get(&read_op, &["foo", "bar"]);
        assert!(values[0].as_ref().unwrap().is_none());
        assert_eq!(
            values[1].as_ref().unwrap().as_ref().unwrap().as_ref(),
            b"baz"
        );
    }

    #[test]
    fn test_snapshot() {
        let path = DBPath::new();
        let db = open_new_db(path.as_ref());
        let sp = db.create_snapshot();

        let write_op = WriteOptions::new();
        db.put(&write_op, "foo", "bar").unwrap();

        let mut read_op = ReadOptions::new();
        assert_eq!(db.get(&read_op, "foo").unwrap().unwrap().as_ref(), b"bar");

        read_op.set_snapshot(&sp);
        assert!(db.get(&read_op, "foo").unwrap().is_none());

        read_op.set_snapshot(&NullSnapshot);
        assert_eq!(db.get(&read_op, "foo").unwrap().unwrap().as_ref(), b"bar");
    }

    #[test]
    fn test_iterator() {
        let path = DBPath::new();
        let db = open_new_db(path.as_ref());

        let write_op = WriteOptions::new();
        db.put(&write_op, "foo1", "bar1").unwrap();
        db.put(&write_op, "foo2", "bar2").unwrap();

        let read_op = ReadOptions::new();
        let mut iter = db.create_iterator(&read_op);
        assert!(!iter.valid());

        iter.seek_to_first();
        assert!(iter.valid());
        unsafe {
            assert_eq!(iter.key().as_ref(), b"foo1");
            assert_eq!(iter.value().as_ref(), b"bar1");
        }

        iter.next();
        assert!(iter.valid());
        unsafe {
            assert_eq!(iter.key().as_ref(), b"foo2");
            assert_eq!(iter.value().as_ref(), b"bar2");
        }

        iter.next();
        assert!(!iter.valid());

        iter.seek_to_last();
        assert!(iter.valid());
        unsafe {
            assert_eq!(iter.key().as_ref(), b"foo2");
            assert_eq!(iter.value().as_ref(), b"bar2");
        }

        iter.prev();
        assert!(iter.valid());
        unsafe {
            assert_eq!(iter.key().as_ref(), b"foo1");
            assert_eq!(iter.value().as_ref(), b"bar1");
        }

        iter.prev();
        assert!(!iter.valid());

        iter.seek("foo3");
        assert!(!iter.valid());

        iter.seek_for_prev("foo3");
        assert!(iter.valid());
    }
}
