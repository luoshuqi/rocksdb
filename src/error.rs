use std::error::Error as StdError;
use std::ffi::{CStr, CString};
use std::fmt::{Debug, Display, Formatter};
use std::os::raw::c_char;

use crate::free;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error(CString);

impl Error {
    pub(crate) fn new(errptr: *mut c_char) -> Self {
        debug_assert!(!errptr.is_null());
        let err = unsafe { CStr::from_ptr(errptr) }.to_owned();
        free(errptr);
        Self(err)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl StdError for Error {}
