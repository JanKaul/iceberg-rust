use std::ffi::{c_char, CStr};

use iceberg_rust::{
    catalog::identifier::Identifier,
    spec::schema::Schema,
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
    let schema: Schema = serde_json::from_str(schema.to_str().unwrap()).unwrap();
    let identifier = unsafe { CStr::from_ptr(identifier) };
    let identifier = Identifier::parse(identifier.to_str().unwrap()).unwrap();
    let mut builder = TableBuilder::new(identifier, catalog.0.clone()).unwrap();
    builder
        .location(base_path.to_str().unwrap())
        .with_schema((schema.schema_id, schema));
    Box::new(builder)
}

/// Commit table builder and create table
#[no_mangle]
pub extern "C" fn table_builder_commit(mut table_builder: Box<TableBuilder>) -> Box<Table> {
    let table = block_on(table_builder.build()).unwrap();
    Box::new(table)
}
