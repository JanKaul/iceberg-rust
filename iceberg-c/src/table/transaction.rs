use std::{
    ffi::{c_char, c_uint, CStr},
    slice,
};

use iceberg_rust::table::transaction::TableTransaction;

use crate::block_on;

#[repr(C)]
pub struct CTableTransaction<'a>(pub TableTransaction<'a>);

#[no_mangle]
pub extern "C" fn table_transaction_new_append(
    transaction: Box<CTableTransaction>,
    paths: *const *const c_char,
    num_paths: c_uint,
) -> Box<CTableTransaction> {
    let paths: &[*const c_char] =
        unsafe { slice::from_raw_parts(paths, num_paths.try_into().unwrap()) };
    let paths = paths
        .into_iter()
        .map(|path| unsafe { CStr::from_ptr(*path) })
        .map(|path| path.to_str().unwrap().to_string())
        .collect();
    Box::new(CTableTransaction(transaction.0.append(paths)))
}

#[no_mangle]
pub extern "C" fn table_transaction_commit(transaction: Box<CTableTransaction>) {
    block_on(transaction.0.commit()).unwrap();
}
