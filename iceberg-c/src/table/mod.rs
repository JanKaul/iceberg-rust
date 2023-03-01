use iceberg_rust::table::Table;

use self::transaction::CTableTransaction;

pub mod transaction;

pub struct CTable(pub Table);

#[no_mangle]
pub extern "C" fn table_new_transaction(table: &mut CTable) -> Box<CTableTransaction> {
    let transaction = table.0.new_transaction();
    Box::new(CTableTransaction(transaction))
}
