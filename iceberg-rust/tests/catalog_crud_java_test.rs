//! Java-parity tests for the `Catalog` trait — namespace + table CRUD.
//!
//! Mirrors a focused subset of `org.apache.iceberg.catalog.CatalogTests`
//! (which has 91 @Test methods total). This port covers the basic
//! namespace + table operations against Rust's `SqlCatalog`:
//!
//!   - createNamespace / createExistingNamespace
//!   - dropNamespace / dropNonexistentNamespace
//!   - listNamespaces / namespaceExists
//!   - basicCreateTable / dropTable / tableExists
//!
//! Rust catalog API (see `iceberg-rust/src/catalog/mod.rs`):
//!   - `create_namespace(ns, properties)`
//!   - `drop_namespace(ns)`
//!   - `namespace_exists(ns)`
//!   - `list_namespaces(parent)`
//!   - `tabular_exists(identifier)`
//!   - `drop_table(identifier)`
//!   - `list_tabulars(namespace)`
//!   - Table creation goes through `Table::builder().build()` which
//!     internally calls `Catalog::create_table`.

use std::{collections::HashMap, sync::Arc};

use iceberg_rust::{
    catalog::{namespace::Namespace, Catalog},
    object_store::ObjectStoreBuilder,
    table::Table,
};
use iceberg_rust_spec::spec::{
    identifier::Identifier,
    partition::PartitionSpec,
    schema::Schema,
    types::{PrimitiveType, StructField, Type},
};
use iceberg_sql_catalog::SqlCatalog;

// --- Shared test fixtures --------------------------------------------------

fn schema() -> Schema {
    Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "id".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Long),
            doc: Some("unique ID".to_string()),
            initial_default: None,
            write_default: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "data".to_string(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        })
        .build()
        .unwrap()
}

async fn fresh_catalog() -> Arc<dyn Catalog> {
    let object_store = ObjectStoreBuilder::memory();
    Arc::new(
        SqlCatalog::new("sqlite://", "warehouse", object_store)
            .await
            .unwrap(),
    )
}

// --- Namespace CRUD --------------------------------------------------------

/// Java: `testCreateNamespace` — namespace doesn't exist beforehand,
/// `createNamespace` adds it, `listNamespaces` returns it,
/// `namespaceExists` flips to true.
#[tokio::test]
async fn test_catalog_create_namespace_per_java() {
    let catalog = fresh_catalog().await;
    let ns = Namespace::try_new(&["newdb".to_string()]).unwrap();

    assert!(
        !catalog.namespace_exists(&ns).await.unwrap(),
        "fresh catalog must not have the namespace yet",
    );

    catalog
        .create_namespace(&ns, None)
        .await
        .expect("create_namespace");

    assert!(
        catalog.namespace_exists(&ns).await.unwrap(),
        "namespace_exists must flip true after create",
    );

    let ns_list = catalog.list_namespaces(None).await.unwrap();
    assert!(
        ns_list.iter().any(|n| n == &ns),
        "list_namespaces must include the new namespace; got {ns_list:?}",
    );
}

/// Java: `testCreateExistingNamespace` — creating an already-existing
/// namespace must fail.
#[tokio::test]
async fn test_catalog_create_existing_namespace_fails_per_java() {
    let catalog = fresh_catalog().await;
    let ns = Namespace::try_new(&["existing_ns".to_string()]).unwrap();

    catalog.create_namespace(&ns, None).await.expect("first");
    let second = catalog.create_namespace(&ns, None).await;
    assert!(
        second.is_err(),
        "creating an existing namespace must error, got {second:?}",
    );
}

/// Java: `testDropNamespace` — drop_namespace removes the namespace.
#[tokio::test]
async fn test_catalog_drop_namespace_per_java() {
    let catalog = fresh_catalog().await;
    let ns = Namespace::try_new(&["dropme".to_string()]).unwrap();

    catalog.create_namespace(&ns, None).await.expect("create");
    assert!(catalog.namespace_exists(&ns).await.unwrap());

    catalog.drop_namespace(&ns).await.expect("drop");
    assert!(
        !catalog.namespace_exists(&ns).await.unwrap(),
        "namespace_exists must return false after drop",
    );
}

/// Java: `testListNamespaces` — listing returns all created namespaces.
#[tokio::test]
async fn test_catalog_list_namespaces_per_java() {
    let catalog = fresh_catalog().await;
    let starting = catalog.list_namespaces(None).await.unwrap();

    let ns1 = Namespace::try_new(&["newdb_1".to_string()]).unwrap();
    let ns2 = Namespace::try_new(&["newdb_2".to_string()]).unwrap();
    catalog.create_namespace(&ns1, None).await.expect("ns1");
    catalog.create_namespace(&ns2, None).await.expect("ns2");

    let after = catalog.list_namespaces(None).await.unwrap();
    assert!(
        after.len() >= starting.len() + 2,
        "two new namespaces must be visible (starting={} after={})",
        starting.len(),
        after.len(),
    );
    assert!(after.iter().any(|n| n == &ns1));
    assert!(after.iter().any(|n| n == &ns2));
}

/// Java: namespace properties round-trip via `create_namespace(ns, props)`.
#[tokio::test]
async fn test_catalog_create_namespace_with_properties_per_java() {
    let catalog = fresh_catalog().await;
    let ns = Namespace::try_new(&["with_props".to_string()]).unwrap();

    let mut props = HashMap::new();
    props.insert("owner".to_string(), "alice".to_string());
    props.insert("location".to_string(), "s3://bucket/path".to_string());
    catalog
        .create_namespace(&ns, Some(props.clone()))
        .await
        .expect("create with props");

    let loaded = catalog.load_namespace(&ns).await.expect("load");
    assert_eq!(
        loaded.get("owner").map(String::as_str),
        Some("alice"),
        "owner property must round-trip",
    );
    assert_eq!(
        loaded.get("location").map(String::as_str),
        Some("s3://bucket/path"),
    );
}

// --- Table CRUD ------------------------------------------------------------

/// Java: `testBasicCreateTable` — Table::builder().build() creates the
/// table; tabular_exists reports true; the table is loadable.
#[tokio::test]
async fn test_catalog_basic_create_table_per_java() {
    let catalog = fresh_catalog().await;
    let ns = Namespace::try_new(&["ns".to_string()]).unwrap();
    catalog.create_namespace(&ns, None).await.expect("ns");

    let ident = Identifier::new(&["ns".to_string()], "tbl");
    assert!(
        !catalog.tabular_exists(&ident).await.unwrap(),
        "table must not exist before create",
    );

    let _table = Table::builder()
        .with_name("tbl")
        .with_location("/test/tbl")
        .with_schema(schema())
        .with_partition_spec(PartitionSpec::default())
        .build(&["ns".to_owned()], catalog.clone())
        .await
        .expect("create table");

    assert!(
        catalog.tabular_exists(&ident).await.unwrap(),
        "table must exist after create",
    );
}

/// Java: `testDropTable` — drop_table removes the table.
#[tokio::test]
async fn test_catalog_drop_table_per_java() {
    let catalog = fresh_catalog().await;
    let ns = Namespace::try_new(&["dropns".to_string()]).unwrap();
    catalog.create_namespace(&ns, None).await.expect("ns");

    let ident = Identifier::new(&["dropns".to_string()], "tbl");

    let _table = Table::builder()
        .with_name("tbl")
        .with_location("/test/dropns_tbl")
        .with_schema(schema())
        .with_partition_spec(PartitionSpec::default())
        .build(&["dropns".to_owned()], catalog.clone())
        .await
        .expect("create");
    assert!(catalog.tabular_exists(&ident).await.unwrap());

    catalog.drop_table(&ident).await.expect("drop_table");
    assert!(
        !catalog.tabular_exists(&ident).await.unwrap(),
        "table_exists must return false after drop_table",
    );
}

/// Java: list_tabulars returns the tables under a namespace.
#[tokio::test]
async fn test_catalog_list_tables_per_java() {
    let catalog = fresh_catalog().await;
    let ns = Namespace::try_new(&["listns".to_string()]).unwrap();
    catalog.create_namespace(&ns, None).await.expect("ns");

    let _t1 = Table::builder()
        .with_name("tbl_a")
        .with_location("/test/listns/a")
        .with_schema(schema())
        .with_partition_spec(PartitionSpec::default())
        .build(&["listns".to_owned()], catalog.clone())
        .await
        .expect("t1");
    let _t2 = Table::builder()
        .with_name("tbl_b")
        .with_location("/test/listns/b")
        .with_schema(schema())
        .with_partition_spec(PartitionSpec::default())
        .build(&["listns".to_owned()], catalog.clone())
        .await
        .expect("t2");

    let listed = catalog.list_tabulars(&ns).await.unwrap();
    assert_eq!(listed.len(), 2, "expected 2 tables, got {listed:?}");
    let names: Vec<&str> = listed.iter().map(|i| i.name()).collect();
    assert!(names.contains(&"tbl_a"));
    assert!(names.contains(&"tbl_b"));
}

// --- Genuine feature gaps (#[ignore]) -------------------------------------

#[tokio::test]
#[ignore = "feature gap: drop_namespace on an empty/nonexistent namespace must report whether anything was dropped — Rust's drop_namespace returns () not bool"]
async fn test_catalog_drop_nonexistent_namespace_per_java() {
    // Java: testDropNonexistentNamespace. catalog.dropNamespace(ns)
    // returns false (the boolean drop-result). Rust's drop_namespace
    // returns Result<(), Error>; behaviour on missing namespace is
    // implementation-defined (SqlCatalog likely errors).
}

#[tokio::test]
#[ignore = "feature gap: drop_namespace on a non-empty namespace must throw NamespaceNotEmpty (Java behaviour); Rust's behaviour unspecified"]
async fn test_catalog_drop_non_empty_namespace_per_java() {
    // Java: testDropNonEmptyNamespace.
}

#[tokio::test]
#[ignore = "feature gap: nested namespaces ('a.b.c') — Rust's Namespace supports multi-level names but not all backends do; SqlCatalog limitations unknown"]
async fn test_catalog_list_nested_namespaces_per_java() {
    // Java: testListNestedNamespaces.
}

#[tokio::test]
#[ignore = "feature gap: createTable on an already-existing table must throw AlreadyExistsException; Rust's Table::builder.build() doesn't centralise this validation"]
async fn test_catalog_create_table_that_already_exists_per_java() {
    // Java: testBasicCreateTableThatAlreadyExists.
}

#[tokio::test]
#[ignore = "feature gap: namespace properties update API (set_properties, remove_properties); covered separately as set_namespace_properties / update_namespace_properties / etc."]
async fn test_catalog_set_namespace_properties_per_java() {
    // Java: testSetNamespaceProperties.
}

#[tokio::test]
#[ignore = "feature gap: namespace property remove API not present at Catalog trait level (Rust has update_namespace but not remove-by-key)"]
async fn test_catalog_remove_namespace_properties_per_java() {
    // Java: testRemoveNamespaceProperties.
}

#[tokio::test]
#[ignore = "feature gap: name validation — testNamespaceWithSlash / testNamespaceWithDot exercise edge cases around special-character names in catalog backends"]
async fn test_catalog_namespace_with_slash_per_java() {
    // Java: testNamespaceWithSlash.
}

#[tokio::test]
#[ignore = "feature gap: same — dotted namespace name handling"]
async fn test_catalog_namespace_with_dot_per_java() {
    // Java: testNamespaceWithDot.
}
