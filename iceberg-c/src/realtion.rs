use iceberg_rust::{catalog::relation::Relation, table::Table};

/// Convert relation to table. Panics if conversion fails.
#[no_mangle]
pub extern "C" fn relation_to_table(relation: Box<Relation>) -> Box<Table> {
    match *relation {
        Relation::Table(table) => Box::new(table),
        _ => panic!(),
    }
}

/// Destructor for relation
#[no_mangle]
pub extern "C" fn relation_free(_catalog: Option<Box<Relation>>) {}
