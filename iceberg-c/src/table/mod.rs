use iceberg_rust::table::{transaction::TableTransaction, Table};

pub mod transaction;

/// Create new table transaction
#[no_mangle]
pub extern "C" fn table_new_transaction(table: &mut Table) -> Box<TableTransaction> {
    let transaction = table.new_transaction();
    Box::new(transaction)
}

/// Destructor for table
#[no_mangle]
pub extern "C" fn table_free(_catalog: Option<Box<Table>>) {}
