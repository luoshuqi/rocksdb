use std::marker::PhantomData;
use std::os::raw::c_uchar;
use std::ptr::null;

use librocksdb_sys::*;

use crate::snapshot::Snapshot;

define!(
    Options,
    rocksdb_options_t,
    rocksdb_options_create,
    rocksdb_options_destroy
);

impl Options {
    pub fn set_create_if_missing(&mut self, create: bool) {
        unsafe { rocksdb_options_set_create_if_missing(self.inner, create as c_uchar) }
    }

    pub fn get_create_if_missing(&self) -> bool {
        unsafe { rocksdb_options_get_create_if_missing(self.inner) != 0 }
    }

    pub fn set_error_if_exists(&self, error: bool) {
        unsafe { rocksdb_options_set_error_if_exists(self.inner, error as _) }
    }

    pub fn get_error_if_exists(&self) -> bool {
        unsafe { rocksdb_options_get_error_if_exists(self.inner) != 0 }
    }
}

impl Clone for Options {
    fn clone(&self) -> Self {
        Self {
            inner: unsafe { rocksdb_options_create_copy(self.inner) },
        }
    }
}

pub struct ReadOptions<'a> {
    pub(crate) inner: *mut rocksdb_readoptions_t,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ReadOptions<'a> {
    pub fn new() -> Self {
        Self {
            inner: unsafe { rocksdb_readoptions_create() },
            _marker: PhantomData,
        }
    }

    pub fn set_snapshot(&mut self, snapshot: &'a impl Snapshot) {
        let ptr = match snapshot.as_ref() {
            Some(s) => s as *const _,
            None => null(),
        };
        unsafe { rocksdb_readoptions_set_snapshot(self.inner, ptr) }
    }

    pub fn set_iterate_upper_bound<T: AsRef<[u8]> + ?Sized>(&mut self, upper_bound: &'a T) {
        let b = upper_bound.as_ref();
        unsafe { rocksdb_readoptions_set_iterate_upper_bound(self.inner, b.as_ptr() as _, b.len()) }
    }

    pub fn set_iterate_lower_bound<T: AsRef<[u8]> + ?Sized>(&mut self, lower_bound: &'a T) {
        let b = lower_bound.as_ref();
        unsafe { rocksdb_readoptions_set_iterate_lower_bound(self.inner, b.as_ptr() as _, b.len()) }
    }
}

impl<'a> Drop for ReadOptions<'a> {
    fn drop(&mut self) {
        unsafe { rocksdb_readoptions_destroy(self.inner) }
    }
}

unsafe impl<'a> Send for ReadOptions<'a> {}

unsafe impl<'a> Sync for ReadOptions<'a> {}

define!(
    WriteOptions,
    rocksdb_writeoptions_t,
    rocksdb_writeoptions_create,
    rocksdb_writeoptions_destroy
);

define!(
    FlushOptions,
    rocksdb_flushoptions_t,
    rocksdb_flushoptions_create,
    rocksdb_flushoptions_destroy
);

impl FlushOptions {
    pub fn set_wait(&mut self, wait: bool) {
        unsafe { rocksdb_flushoptions_set_wait(self.inner, wait as c_uchar) }
    }

    pub fn get_wait(&self) -> bool {
        unsafe { rocksdb_flushoptions_get_wait(self.inner) != 0 }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs::remove_dir_all;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{Options, DB};

    pub struct DBPath(String);

    impl DBPath {
        pub fn new() -> Self {
            Self(format!(
                "/tmp/rocksdb_test_{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
        }
    }

    impl AsRef<str> for DBPath {
        fn as_ref(&self) -> &str {
            &self.0
        }
    }

    impl Drop for DBPath {
        fn drop(&mut self) {
            let _ = remove_dir_all(&self.0);
        }
    }

    #[test]
    fn test_options_create_if_missing() {
        let mut options = Options::new();
        options.set_error_if_exists(true);
        options.set_create_if_missing(false);
        assert_eq!(options.get_create_if_missing(), false);
        let path = DBPath::new();
        assert!(DB::open(&options, path.as_ref()).is_err());

        options.set_create_if_missing(true);
        assert_eq!(options.get_create_if_missing(), true);
        let path = DBPath::new();
        assert!(DB::open(&options, path.as_ref()).is_ok());
    }

    #[test]
    fn test_options_set_error_if_exists() {
        let mut options = Options::new();
        options.set_create_if_missing(true);
        let path = DBPath::new();
        assert!(DB::open(&options, path.as_ref()).is_ok());

        options.set_error_if_exists(false);
        assert_eq!(options.get_error_if_exists(), false);
        assert!(DB::open(&options, path.as_ref()).is_ok());

        options.set_error_if_exists(true);
        assert_eq!(options.get_error_if_exists(), true);
        assert!(DB::open(&options, path.as_ref()).is_err());
    }
}
