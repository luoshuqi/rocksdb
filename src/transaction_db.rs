use std::ffi::CString;
use std::ptr::null_mut;

use librocksdb_sys::*;

use crate::snapshot::{OwnedSnapshot, ReleaseSnapshot};
use crate::{
    Bytes, OldTransaction, Options, ReadOptions, Result, Transaction, WriteBatch, WriteOptions,
};

pub struct TransactionDB {
    inner: *mut rocksdb_transactiondb_t,
}

impl TransactionDB {
    pub fn open(
        options: &Options,
        txn_db_options: &TransactionDBOptions,
        name: &str,
    ) -> Result<Self> {
        let name = CString::new(name).unwrap();
        let inner = ffi!(rocksdb_transactiondb_open(
            options.inner,
            txn_db_options.inner,
            name.as_ptr()
        ));
        Ok(Self { inner })
    }

    pub fn create_snapshot(&self) -> OwnedSnapshot<'_, Self> {
        let inner = unsafe { rocksdb_transactiondb_create_snapshot(self.inner) };
        debug_assert!(!inner.is_null());
        OwnedSnapshot { inner, db: self }
    }

    pub fn get(&self, options: &ReadOptions, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let mut len: usize = 0;
        let key = key.as_ref();
        let value = ffi!(rocksdb_transactiondb_get(
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

    pub fn put(
        &self,
        options: &WriteOptions,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        Ok(ffi!(rocksdb_transactiondb_put(
            self.inner,
            options.inner,
            key.as_ptr() as _,
            key.len(),
            value.as_ptr() as _,
            value.len()
        )))
    }

    pub fn write(&self, options: &WriteOptions, batch: &WriteBatch) -> Result<()> {
        Ok(ffi!(rocksdb_transactiondb_write(
            self.inner,
            options.inner,
            batch.inner
        )))
    }

    pub fn delete(&self, options: &WriteOptions, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        Ok(ffi!(rocksdb_transactiondb_delete(
            self.inner,
            options.inner,
            key.as_ptr() as _,
            key.len()
        )))
    }

    pub fn create_iterator(&self, options: &ReadOptions) -> crate::Iterator {
        crate::Iterator::new(unsafe {
            rocksdb_transactiondb_create_iterator(self.inner, options.inner)
        })
    }

    pub fn begin<'a>(
        &self,
        write_options: &WriteOptions,
        txn_options: &TransactionOptions,
        old_txn: impl Into<Option<OldTransaction<'a>>>,
    ) -> Transaction {
        let old_txn = match old_txn.into() {
            Some(txn) => txn.into_raw(),
            None => null_mut(),
        };
        let inner = unsafe {
            rocksdb_transaction_begin(self.inner, write_options.inner, txn_options.inner, old_txn)
        };
        Transaction::new(inner)
    }
}

impl ReleaseSnapshot for TransactionDB {
    fn release_snapshot(&self, snapshot: *const rocksdb_snapshot_t) {
        unsafe { rocksdb_transactiondb_release_snapshot(self.inner, snapshot) }
    }
}

impl Drop for TransactionDB {
    fn drop(&mut self) {
        unsafe { rocksdb_transactiondb_close(self.inner) }
    }
}

define!(
    TransactionDBOptions,
    rocksdb_transactiondb_options_t,
    rocksdb_transactiondb_options_create,
    rocksdb_transactiondb_options_destroy
);

impl TransactionDBOptions {
    pub fn set_max_num_locks(&mut self, max_num_locks: i64) {
        unsafe { rocksdb_transactiondb_options_set_max_num_locks(self.inner, max_num_locks) }
    }

    pub fn set_num_stripes(&mut self, num_stripes: usize) {
        unsafe { rocksdb_transactiondb_options_set_num_stripes(self.inner, num_stripes) }
    }

    pub fn set_transaction_lock_timeout(&mut self, txn_lock_timeout: i64) {
        unsafe {
            rocksdb_transactiondb_options_set_transaction_lock_timeout(self.inner, txn_lock_timeout)
        }
    }

    pub fn set_default_lock_timeout(&mut self, default_lock_timeout: i64) {
        unsafe {
            rocksdb_transactiondb_options_set_default_lock_timeout(self.inner, default_lock_timeout)
        }
    }
}

define!(
    TransactionOptions,
    rocksdb_transaction_options_t,
    rocksdb_transaction_options_create,
    rocksdb_transaction_options_destroy
);

impl TransactionOptions {
    pub fn set_set_snapshot(&mut self, set_snapshot: bool) {
        unsafe { rocksdb_transaction_options_set_set_snapshot(self.inner, set_snapshot as _) }
    }

    pub fn set_deadlock_detect(&mut self, deadlock_detect: bool) {
        unsafe { rocksdb_transaction_options_set_deadlock_detect(self.inner, deadlock_detect as _) }
    }

    pub fn set_lock_timeout(&mut self, lock_timeout: i64) {
        unsafe { rocksdb_transaction_options_set_lock_timeout(self.inner, lock_timeout) }
    }

    pub fn set_expiration(&mut self, expiration: i64) {
        unsafe { rocksdb_transaction_options_set_expiration(self.inner, expiration) }
    }

    pub fn set_deadlock_detect_depth(&mut self, depth: i64) {
        unsafe { rocksdb_transaction_options_set_deadlock_detect_depth(self.inner, depth) }
    }

    pub fn set_max_write_batch_size(&mut self, size: usize) {
        unsafe { rocksdb_transaction_options_set_max_write_batch_size(self.inner, size) }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::options::tests::DBPath;
    use crate::snapshot::NullSnapshot;
    use crate::{
        Options, ReadOptions, TransactionDB, TransactionDBOptions, WriteBatch, WriteOptions,
    };

    pub fn open_new_db(name: &str) -> TransactionDB {
        let mut options = Options::new();
        options.set_create_if_missing(true);
        options.set_error_if_exists(true);
        let txn_options = TransactionDBOptions::new();
        TransactionDB::open(&options, &txn_options, name).unwrap()
    }

    #[test]
    fn test_open() {
        let mut options = Options::new();
        options.set_create_if_missing(true);
        options.set_error_if_exists(true);
        let txn_options = TransactionDBOptions::new();
        let path = DBPath::new();
        assert!(TransactionDB::open(&options, &txn_options, path.as_ref()).is_ok());
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
        assert_eq!(db.get(&read_op, "foo").unwrap().unwrap().as_ref(), b"bar");
        assert_eq!(db.get(&read_op, "bar").unwrap().unwrap().as_ref(), b"baz");

        wb.clear();
        assert_eq!(wb.count(), 0);
        wb.delete("foo");
        assert!(db.write(&write_op, &wb).is_ok());
        assert!(db.get(&read_op, "foo").unwrap().is_none());
        assert_eq!(db.get(&read_op, "bar").unwrap().unwrap().as_ref(), b"baz");
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
