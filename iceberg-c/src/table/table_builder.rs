use std::ffi::{c_char, CStr};

use iceberg_rust::{
    catalog::identifier::Identifier,
    spec::schema::SchemaV2,
    table::{table_builder::TableBuilder, Table},
};

use crate::{block_on, catalog::ArcCatalog};

/// Create new metastore table
#[no_mangle]
pub extern "C" fn table_builder_new_metastore(
    base_path: *const c_char,
    schema: *const c_char,
    identifier: *const c_char,
    catalog: &ArcCatalog,
) -> Box<TableBuilder> {
    let base_path = unsafe { CStr::from_ptr(base_path) };
    let schema = unsafe { CStr::from_ptr(schema) };
    let schema: SchemaV2 = serde_json::from_str(schema.to_str().unwrap()).unwrap();
    let identifier = unsafe { CStr::from_ptr(identifier) };
    let identifier = Identifier::parse(identifier.to_str().unwrap()).unwrap();
    Box::new(
        TableBuilder::new(
            base_path.to_str().unwrap(),
            schema,
            identifier,
            catalog.0.clone(),
        )
        .unwrap(),
    )
}

/// Commit table builder and create table
#[no_mangle]
pub extern "C" fn table_builder_commit(table_builder: Box<TableBuilder>) -> Box<Table> {
    let table = block_on(table_builder.commit()).unwrap();
    Box::new(table)
}
