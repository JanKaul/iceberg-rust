/*!
Defines traits to communicate with an iceberg catalog.
*/

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub mod identifier;
pub mod namespace;

use iceberg_rust_spec::view_metadata::FullIdentifier;
use identifier::Identifier;
use object_store::ObjectStore;

use crate::error::Error;
use crate::materialized_view::MaterializedView;
use crate::table::Table;
use crate::view::View;

use self::bucket::Bucket;
use self::commit::{CommitTable, CommitView};
use self::create::{CreateMaterializedView, CreateTable, CreateView};
use self::namespace::Namespace;
use self::tabular::Tabular;

pub mod bucket;
pub mod commit;
pub mod create;
pub mod tabular;

/// Trait to create, replace and drop tables in an iceberg catalog.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + Debug {
    /// Name of the catalog
    fn name(&self) -> &str;
    /// Create a namespace in the catalog
    async fn create_namespace(
        &self,
        namespace: &Namespace,
        properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, Error>;
    /// Drop a namespace in the catalog
    async fn drop_namespace(&self, namespace: &Namespace) -> Result<(), Error>;
    /// Load the namespace properties from the catalog
    async fn load_namespace(&self, namespace: &Namespace)
        -> Result<HashMap<String, String>, Error>;
    /// Update the namespace properties in the catalog
    async fn update_namespace(
        &self,
        namespace: &Namespace,
        updates: Option<HashMap<String, String>>,
        removals: Option<Vec<String>>,
    ) -> Result<(), Error>;
    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, Error>;
    /// Lists all tables in the given namespace.
    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, Error>;
    /// Lists all namespaces in the catalog.
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, Error>;
    /// Check if a table exists
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, Error>;
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Drop a table and delete all data and metadata files.
    async fn drop_view(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Drop a table and delete all data and metadata files.
    async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Load a table.
    async fn load_tabular(self: Arc<Self>, identifier: &Identifier) -> Result<Tabular, Error>;
    /// Create a table in the catalog if it doesn't exist.
    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        create_table: CreateTable,
    ) -> Result<Table, Error>;
    /// Create a view with the catalog if it doesn't exist.
    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateView<Option<()>>,
    ) -> Result<View, Error>;
    /// Register a materialized view with the catalog if it doesn't exist.
    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, Error>;
    /// perform commit table operation
    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, Error>;
    /// perform commit view operation
    async fn update_view(self: Arc<Self>, commit: CommitView<Option<()>>) -> Result<View, Error>;
    /// perform commit view operation
    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView<FullIdentifier>,
    ) -> Result<MaterializedView, Error>;
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_location: &str,
    ) -> Result<Table, Error>;
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
