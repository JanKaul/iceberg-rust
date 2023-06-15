use iceberg_rust::table::transaction::TableTransaction;

use crate::block_on;

/// Commit transaction freeing its memmory
#[no_mangle]
pub extern "C" fn table_transaction_commit(transaction: Box<TableTransaction>) {
    block_on(transaction.commit()).unwrap();
}
