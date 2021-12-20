use std::marker::PhantomData;

use librocksdb_sys::*;

use crate::free;

pub trait Snapshot {
    fn as_ref(&self) -> Option<&rocksdb_snapshot_t>;
}

pub trait ReleaseSnapshot {
    fn release_snapshot(&self, snapshot: *const rocksdb_snapshot_t);
}

pub struct OwnedSnapshot<'a, DB: ReleaseSnapshot> {
    pub(crate) inner: *const rocksdb_snapshot_t,
    pub(crate) db: &'a DB,
}

impl<'a, DB: ReleaseSnapshot> Snapshot for OwnedSnapshot<'a, DB> {
    fn as_ref(&self) -> Option<&rocksdb_snapshot_t> {
        Some(unsafe { &*self.inner })
    }
}

impl<'a, DB: ReleaseSnapshot> Drop for OwnedSnapshot<'a, DB> {
    fn drop(&mut self) {
        self.db.release_snapshot(self.inner)
    }
}

unsafe impl<'a, DB: ReleaseSnapshot + Sync> Send for OwnedSnapshot<'a, DB> {}

unsafe impl<'a, DB: ReleaseSnapshot + Sync> Sync for OwnedSnapshot<'a, DB> {}

pub struct BorrowedSnapshot<'a> {
    inner: *const rocksdb_snapshot_t,
    _marker: PhantomData<&'a ()>,
}

impl<'a> BorrowedSnapshot<'a> {
    pub(crate) fn new(inner: *const rocksdb_snapshot_t) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<'a> Snapshot for BorrowedSnapshot<'a> {
    fn as_ref(&self) -> Option<&rocksdb_snapshot_t> {
        Some(unsafe { &*self.inner })
    }
}

impl<'a> Drop for BorrowedSnapshot<'a> {
    fn drop(&mut self) {
        free(self.inner as *mut rocksdb_snapshot_t)
    }
}

unsafe impl<'a> Send for BorrowedSnapshot<'a> {}

unsafe impl<'a> Sync for BorrowedSnapshot<'a> {}

pub struct NullSnapshot;

impl Snapshot for NullSnapshot {
    fn as_ref(&self) -> Option<&rocksdb_snapshot_t> {
        None
    }
}
