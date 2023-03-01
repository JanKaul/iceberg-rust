use iceberg_rust::catalog::relation::Relation;

use crate::table::CTable;

pub struct CRelation(pub Relation);

#[no_mangle]
pub extern "C" fn relation_to_table(relation: Box<CRelation>) -> Box<CTable> {
    match relation.0 {
        Relation::Table(table) => Box::new(CTable(table)),
        _ => panic!(),
    }
}
