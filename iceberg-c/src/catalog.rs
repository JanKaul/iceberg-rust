use std::{
    ffi::{c_char, CStr},
    sync::Arc,
};

use iceberg_rust::catalog::{identifier::Identifier, relation::Relation, Catalog};

use crate::block_on;

pub struct ArcCatalog(pub Arc<dyn Catalog>);

/// Load a table
#[no_mangle]
pub extern "C" fn catalog_load_table(
    catalog: &ArcCatalog,
    identifier: *const c_char,
) -> Box<Relation> {
    let identifier = unsafe { CStr::from_ptr(identifier) };
    let identifier = Identifier::parse(identifier.to_str().unwrap()).unwrap();

    let relation = block_on(catalog.0.clone().load_table(&identifier)).unwrap();

    Box::new(relation)
}

/// Check if table exists
#[no_mangle]
pub extern "C" fn catalog_table_exists(catalog: &ArcCatalog, identifier: *const c_char) -> bool {
    let identifier = unsafe { CStr::from_ptr(identifier) };
    let identifier = Identifier::parse(identifier.to_str().unwrap()).unwrap();

    let exists = block_on(catalog.0.clone().table_exists(&identifier)).unwrap();

    exists
}
