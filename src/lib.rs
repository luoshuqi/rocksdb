use librocksdb_sys::rocksdb_free;

pub use bytes::*;
pub use db::*;
pub use error::*;
pub use iterator::*;
pub use options::*;
pub use transaction::*;
pub use transaction_db::*;
pub use write_batch::*;

macro_rules! ffi {
    ($f:ident($($args:expr),*)) => {{
        let mut errptr = std::ptr::null_mut();
        let ret = unsafe { $f($($args,)* &mut errptr) };
        if errptr.is_null() {
            ret
        } else {
            return Err(crate::Error::new(errptr))
        }
    }};
}

macro_rules! define {
    ($r:ident, $c:ident, $create:ident, $destroy:ident) => {
        pub struct $r {
            pub(crate) inner: *mut $c,
        }

        impl $r {
            pub fn new() -> Self {
                Self {
                    inner: unsafe { $create() },
                }
            }
        }

        impl Drop for $r {
            fn drop(&mut self) {
                unsafe { $destroy(self.inner) }
            }
        }

        unsafe impl Send for $r {}

        unsafe impl Sync for $r {}
    };
}

mod bytes;
mod db;
mod error;
mod iterator;
mod options;
mod snapshot;
mod transaction;
mod transaction_db;
mod write_batch;

fn free<T>(ptr: *mut T) {
    unsafe { rocksdb_free(ptr as _) };
}
