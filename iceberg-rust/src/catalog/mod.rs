/*!
Defines traits to communicate with an iceberg catalog.
*/

use std::fmt::Debug;
use std::sync::Arc;

pub mod identifier;
pub mod namespace;

use iceberg_rust_spec::spec::materialized_view_metadata::MaterializedViewMetadata;
use iceberg_rust_spec::spec::table_metadata::TableMetadata;
use iceberg_rust_spec::spec::view_metadata::ViewMetadata;
use identifier::Identifier;
use object_store::ObjectStore;

use crate::error::Error;
use crate::materialized_view::MaterializedView;
use crate::table::Table;
use crate::view::View;

use self::bucket::Bucket;
use self::commit::{CommitTable, CommitView};
use self::namespace::Namespace;
use self::tabular::Tabular;

pub mod bucket;
pub mod commit;
pub mod tabular;

/// Trait to create, replace and drop tables in an iceberg catalog.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + Debug {
    /// Lists all tables in the given namespace.
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<Identifier>, Error>;
    /// Lists all namespaces in the catalog.
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, Error>;
    /// Create a table from an identifier and a schema
    /// Check if a table exists
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool, Error>;
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Load a table.
    async fn load_tabular(self: Arc<Self>, identifier: &Identifier) -> Result<Tabular, Error>;
    /// Register a table with the catalog if it doesn't exist.
    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: TableMetadata,
    ) -> Result<Table, Error>;
    /// Register a view with the catalog if it doesn't exist.
    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: ViewMetadata,
    ) -> Result<View, Error>;
    /// Register a materialized view with the catalog if it doesn't exist.
    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: MaterializedViewMetadata,
    ) -> Result<MaterializedView, Error>;
    /// perform commit table operation
    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, Error>;
    /// perform commit view operation
    async fn update_view(self: Arc<Self>, commit: CommitView) -> Result<View, Error>;
    /// perform commit view operation
    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView,
    ) -> Result<MaterializedView, Error>;
    /// Return the associated object store for a bucket
    fn object_store(&self, bucket: Bucket) -> Arc<dyn ObjectStore>;
}

/// Trait to obtain a catalog by name
#[async_trait::async_trait]
pub trait CatalogList: Send + Sync + Debug {
    /// Get catalog from list by name
    async fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>>;
    /// Get the list of available catalogs
    async fn list_catalogs(&self) -> Vec<String>;
}
