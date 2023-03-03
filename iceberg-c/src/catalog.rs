use std::{
    ffi::{c_char, CStr},
    sync::Arc,
};

use iceberg_catalog_rest_client::{apis::configuration::Configuration, catalog::RestCatalog};
use iceberg_rust::catalog::{identifier::Identifier, relation::Relation, Catalog};

use crate::{block_on, object_store::CObjectStore};

pub struct CCatalog(pub Arc<dyn Catalog>);

/// Constructor for rest catalog
#[no_mangle]
pub extern "C" fn catalog_new_rest(
    name: *const c_char,
    base_bath: *const c_char,
    access_token: *const c_char,
    object_store: &CObjectStore,
) -> Box<CCatalog> {
    let name = unsafe { CStr::from_ptr(name) };
    let base_bath = unsafe { CStr::from_ptr(base_bath) };
    let access_token = unsafe { CStr::from_ptr(access_token) };
    let configuration = Configuration {
        base_path: base_bath.to_str().unwrap().to_owned(),
        user_agent: None,
        client: reqwest::Client::new(),
        basic_auth: None,
        oauth_access_token: None,
        bearer_access_token: Some(access_token.to_str().unwrap().to_owned()),
        api_key: None,
    };
    Box::new(CCatalog(Arc::new(RestCatalog::new(
        name.to_str().unwrap(),
        configuration,
        object_store.0.clone(),
    ))))
}

/// Destructor for catalog
#[no_mangle]
pub extern "C" fn catalog_free(_object_store: Option<Box<CCatalog>>) {}

/// Load a table
#[no_mangle]
pub extern "C" fn catalog_load_table(
    catalog: &CCatalog,
    identifier: *const c_char,
) -> Box<Relation> {
    let identifier = unsafe { CStr::from_ptr(identifier) };
    let identifier = Identifier::parse(identifier.to_str().unwrap()).unwrap();

    let relation = block_on(catalog.0.clone().load_table(&identifier)).unwrap();

    Box::new(relation)
}
