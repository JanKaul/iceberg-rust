/*!
Defines traits to communicate with an iceberg catalog.
*/

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use iceberg_rust_spec::identifier::FullIdentifier;
use identifier::Identifier;
use object_store::ObjectStore;

use crate::error::Error;
use crate::materialized_view::MaterializedView;
use crate::table::Table;
use crate::view::View;

use self::commit::{CommitTable, CommitView};
use self::create::{CreateMaterializedView, CreateTable, CreateView};
use self::namespace::Namespace;
use self::tabular::Tabular;
use crate::object_store::Bucket;

pub mod commit;
pub mod create;
pub mod tabular;

/// A trait representing an Iceberg catalog that manages tables, views, and namespaces.
///
/// The Catalog trait provides methods to:
/// - Create, update, and delete namespaces
/// - Create, load, and drop tables and views
/// - List available tables and namespaces
/// - Manage table and view metadata
/// - Access object storage
///
/// Implementations must be Send + Sync for concurrent access and Debug for logging/debugging.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + Debug {
    /// Returns the name of this catalog.
    ///
    /// The catalog name is a unique identifier used to:
    /// - Distinguish between multiple catalogs in a catalog list
    /// - Reference this catalog in configuration
    /// - Identify the catalog in logging and error messages
    fn name(&self) -> &str;
    /// Creates a new namespace in the catalog with optional properties.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to create
    /// * `properties` - Optional key-value properties to associate with the namespace
    ///
    /// # Returns
    /// * `Result<HashMap<String, String>, Error>` - The namespace properties after creation
    ///
    /// # Errors
    /// Returns an error if:
    /// * The namespace already exists
    /// * The namespace name is invalid
    /// * The catalog fails to create the namespace
    /// * Properties cannot be set
    async fn create_namespace(
        &self,
        namespace: &Namespace,
        properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, Error>;
    /// Removes a namespace and all its properties from the catalog.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to remove
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if the namespace was successfully removed
    ///
    /// # Errors
    /// Returns an error if:
    /// * The namespace doesn't exist
    /// * The namespace contains tables or views
    /// * The catalog fails to remove the namespace
    async fn drop_namespace(&self, namespace: &Namespace) -> Result<(), Error>;
    /// Loads a namespace's properties from the catalog.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to load properties for
    ///
    /// # Returns
    /// * `Result<HashMap<String, String>, Error>` - The namespace properties if found
    ///
    /// # Errors
    /// Returns an error if:
    /// * The namespace doesn't exist
    /// * The catalog fails to load the namespace properties
    /// * The properties cannot be deserialized
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

/// A trait representing a collection of Iceberg catalogs that can be accessed by name.
///
/// The CatalogList trait provides methods to:
/// - Look up individual catalogs by name
/// - List all available catalogs
/// - Manage multiple catalogs in a unified interface
///
/// Implementations must be Send + Sync for concurrent access and Debug for logging/debugging.
#[async_trait::async_trait]
pub trait CatalogList: Send + Sync + Debug {
    /// Get catalog from list by name
    fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>>;
    /// Get the list of available catalogs
    async fn list_catalogs(&self) -> Vec<String>;
}

pub mod identifier {
    //! Catalog identifier
    pub use iceberg_rust_spec::identifier::Identifier;
}

pub mod namespace {
    //! Catalog namespace
    pub use iceberg_rust_spec::namespace::Namespace;
}
