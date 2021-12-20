use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::forget;
use std::ptr::null_mut;

use librocksdb_sys::*;

use crate::snapshot::BorrowedSnapshot;
use crate::{Bytes, Error, ReadOptions, Result, TransactionDB};

pub struct Transaction<'a> {
    inner: *mut rocksdb_transaction_t,
    _marker: PhantomData<&'a TransactionDB>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(inner: *mut rocksdb_transaction_t) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    pub fn set_savepoint(&self) {
        unsafe { rocksdb_transaction_set_savepoint(self.inner) }
    }

    pub fn rollback_to_savepoint(&self) -> Result<()> {
        Ok(ffi!(rocksdb_transaction_rollback_to_savepoint(self.inner)))
    }

    pub fn commit(self) -> std::result::Result<OldTransaction<'a>, TransactionError<'a>> {
        let mut errptr = null_mut();
        unsafe { rocksdb_transaction_commit(self.inner, &mut errptr) };
        if errptr.is_null() {
            Ok(OldTransaction(self))
        } else {
            Err(TransactionError {
                txn: self,
                error: Error::new(errptr),
            })
        }
    }

    pub fn rollback(self) -> std::result::Result<OldTransaction<'a>, TransactionError<'a>> {
        let mut errptr = null_mut();
        unsafe { rocksdb_transaction_rollback(self.inner, &mut errptr) };
        if errptr.is_null() {
            Ok(OldTransaction(self))
        } else {
            Err(TransactionError {
                txn: self,
                error: Error::new(errptr),
            })
        }
    }

    pub fn get_snapshot(&self) -> Option<BorrowedSnapshot<'_>> {
        let inner = unsafe { rocksdb_transaction_get_snapshot(self.inner) };
        if !inner.is_null() {
            Some(BorrowedSnapshot::new(inner))
        } else {
            None
        }
    }

    pub fn get(&self, read_options: &ReadOptions, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let key = key.as_ref();
        let mut len = 0;
        let value = ffi!(rocksdb_transaction_get(
            self.inner,
            read_options.inner,
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

    pub fn get_for_update(
        &self,
        read_options: &ReadOptions,
        key: impl AsRef<[u8]>,
        exclusive: bool,
    ) -> Result<Option<Bytes>> {
        let key = key.as_ref();
        let mut len = 0;
        let value = ffi!(rocksdb_transaction_get_for_update(
            self.inner,
            read_options.inner,
            key.as_ptr() as _,
            key.len(),
            &mut len,
            exclusive as _
        ));
        if !value.is_null() {
            Ok(Some(Bytes::new(value, len)))
        } else {
            Ok(None)
        }
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        Ok(ffi!(rocksdb_transaction_put(
            self.inner,
            key.as_ptr() as _,
            key.len(),
            value.as_ptr() as _,
            value.len()
        )))
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        Ok(ffi!(rocksdb_transaction_delete(
            self.inner,
            key.as_ptr() as _,
            key.len()
        )))
    }

    pub fn create_iterator(&self, options: &ReadOptions) -> crate::Iterator {
        crate::Iterator::new(unsafe {
            rocksdb_transaction_create_iterator(self.inner, options.inner)
        })
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        unsafe { rocksdb_transaction_destroy(self.inner) }
    }
}

unsafe impl<'a> Send for Transaction<'a> {}

pub struct OldTransaction<'a>(Transaction<'a>);

impl<'a> OldTransaction<'a> {
    pub(crate) fn into_raw(self) -> *mut rocksdb_transaction_t {
        let inner = self.0.inner;
        forget(self);
        inner
    }
}

pub struct TransactionError<'a> {
    txn: Transaction<'a>,
    error: Error,
}

impl<'a> TransactionError<'a> {
    pub fn unwrap(self) -> (Transaction<'a>, Error) {
        (self.txn, self.error)
    }
}

impl<'a> From<TransactionError<'a>> for Error {
    fn from(e: TransactionError<'a>) -> Self {
        e.error
    }
}

impl<'a> Display for TransactionError<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.error, f)
    }
}

impl<'a> Debug for TransactionError<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.error, f)
    }
}

impl<'a> std::error::Error for TransactionError<'a> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

#[cfg(test)]
mod tests {
    use crate::options::tests::DBPath;
    use crate::transaction_db::tests::open_new_db;
    use crate::{ReadOptions, TransactionOptions, WriteOptions};

    #[test]
    fn test_get_put_delete() {
        let path = DBPath::new();
        let db = open_new_db(path.as_ref());

        let write_op = WriteOptions::new();
        let txn_op = TransactionOptions::new();
        let txn = db.begin(&write_op, &txn_op, None);

        let read_op = ReadOptions::new();
        assert!(txn.get(&read_op, "foo").unwrap().is_none());

        assert!(txn.put("foo", "bar").is_ok());
        assert_eq!(txn.get(&read_op, "foo").unwrap().unwrap().as_ref(), b"bar");

        assert!(txn.delete("foo").is_ok());
        assert!(txn.get(&read_op, "foo").unwrap().is_none());
    }

    #[test]
    fn test_commit() {
        let path = DBPath::new();
        let db = open_new_db(path.as_ref());

        let write_op = WriteOptions::new();
        let txn_op = TransactionOptions::new();
        let txn = db.begin(&write_op, &txn_op, None);

        let read_op = ReadOptions::new();
        assert!(db.get(&read_op, "foo").unwrap().is_none());

        assert!(txn.put("foo", "bar").is_ok());

        let read_op = ReadOptions::new();
        assert!(db.get(&read_op, "foo").unwrap().is_none());

        assert!(txn.commit().is_ok());
        assert_eq!(db.get(&read_op, "foo").unwrap().unwrap().as_ref(), b"bar");
    }
}
