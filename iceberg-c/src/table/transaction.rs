use std::{
    ffi::{c_char, c_uint, CStr},
    slice,
};

use iceberg_rust::table::transaction::TableTransaction;

use crate::block_on;

/// Add new append operation to transaction
#[no_mangle]
pub extern "C" fn table_transaction_new_append(
    transaction: Box<TableTransaction>,
    paths: *const *const c_char,
    num_paths: c_uint,
) -> Box<TableTransaction> {
    let paths: &[*const c_char] =
        unsafe { slice::from_raw_parts(paths, num_paths.try_into().unwrap()) };
    let paths = paths
        .into_iter()
        .map(|path| unsafe { CStr::from_ptr(*path) })
        .map(|path| path.to_str().unwrap().to_string())
        .collect();
    Box::new(transaction.append(paths))
}

/// Commit transaction freeing its memmory
#[no_mangle]
pub extern "C" fn table_transaction_commit(transaction: Box<TableTransaction>) {
    block_on(transaction.commit()).unwrap();
}
