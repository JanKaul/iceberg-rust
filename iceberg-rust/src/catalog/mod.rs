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
    /// Updates a namespace's properties by applying updates and removals.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to update
    /// * `updates` - Optional map of property key-value pairs to add or update
    /// * `removals` - Optional list of property keys to remove
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if the namespace was successfully updated
    ///
    /// # Errors
    /// Returns an error if:
    /// * The namespace doesn't exist
    /// * The properties cannot be updated
    /// * The catalog fails to persist the changes
    async fn update_namespace(
        &self,
        namespace: &Namespace,
        updates: Option<HashMap<String, String>>,
        removals: Option<Vec<String>>,
    ) -> Result<(), Error>;
    /// Checks if a namespace exists in the catalog.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to check for existence
    ///
    /// # Returns
    /// * `Result<bool, Error>` - True if the namespace exists, false otherwise
    ///
    /// # Errors
    /// Returns an error if:
    /// * The catalog cannot be accessed
    /// * The namespace check operation fails
    async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, Error>;
    /// Lists all tables, views, and materialized views in the given namespace.
    ///
    /// # Arguments
    /// * `namespace` - The namespace to list tabular objects from
    ///
    /// # Returns
    /// * `Result<Vec<Identifier>, Error>` - List of identifiers for all tabular objects
    ///
    /// # Errors
    /// Returns an error if:
    /// * The namespace doesn't exist
    /// * The catalog cannot be accessed
    /// * The listing operation fails
    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, Error>;
    /// Lists all namespaces under an optional parent namespace.
    ///
    /// # Arguments
    /// * `parent` - Optional parent namespace to list children under. If None, lists top-level namespaces.
    ///
    /// # Returns
    /// * `Result<Vec<Namespace>, Error>` - List of namespace objects
    ///
    /// # Errors
    /// Returns an error if:
    /// * The parent namespace doesn't exist (if specified)
    /// * The catalog cannot be accessed
    /// * The listing operation fails
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, Error>;
    /// Checks if a table, view, or materialized view exists in the catalog.
    ///
    /// # Arguments
    /// * `identifier` - The identifier of the tabular object to check
    ///
    /// # Returns
    /// * `Result<bool, Error>` - True if the tabular object exists, false otherwise
    ///
    /// # Errors
    /// Returns an error if:
    /// * The namespace doesn't exist
    /// * The catalog cannot be accessed
    /// * The existence check operation fails
    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, Error>;
    /// Drops a table from the catalog and deletes all associated data and metadata files.
    ///
    /// # Arguments
    /// * `identifier` - The identifier of the table to drop
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if the table was successfully dropped
    ///
    /// # Errors
    /// Returns an error if:
    /// * The table doesn't exist
    /// * The table is locked or in use
    /// * The catalog fails to delete the table metadata
    /// * The data files cannot be deleted
    async fn drop_table(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Drops a view from the catalog and deletes its metadata.
    ///
    /// # Arguments
    /// * `identifier` - The identifier of the view to drop
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if the view was successfully dropped
    ///
    /// # Errors
    /// Returns an error if:
    /// * The view doesn't exist
    /// * The view is in use
    /// * The catalog fails to delete the view metadata
    async fn drop_view(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Drops a materialized view from the catalog and deletes its metadata and data files.
    ///
    /// # Arguments
    /// * `identifier` - The identifier of the materialized view to drop
    ///
    /// # Returns
    /// * `Result<(), Error>` - Ok if the materialized view was successfully dropped
    ///
    /// # Errors
    /// Returns an error if:
    /// * The materialized view doesn't exist
    /// * The materialized view is in use
    /// * The catalog fails to delete the view metadata
    /// * The associated data files cannot be deleted
    async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), Error>;
    /// Loads a table, view, or materialized view from the catalog.
    ///
    /// # Arguments
    /// * `identifier` - The identifier of the tabular object to load
    ///
    /// # Returns
    /// * `Result<Tabular, Error>` - The loaded tabular object wrapped in an enum
    ///
    /// # Errors
    /// Returns an error if:
    /// * The tabular object doesn't exist
    /// * The metadata cannot be loaded
    /// * The metadata is invalid or corrupted
    /// * The catalog cannot be accessed
    async fn load_tabular(self: Arc<Self>, identifier: &Identifier) -> Result<Tabular, Error>;
    /// Creates a new table in the catalog with the specified configuration.
    ///
    /// # Arguments
    /// * `identifier` - The identifier for the new table
    /// * `create_table` - Configuration for the table creation including schema, partitioning, etc.
    ///
    /// # Returns
    /// * `Result<Table, Error>` - The newly created table object
    ///
    /// # Errors
    /// Returns an error if:
    /// * The table already exists
    /// * The namespace doesn't exist
    /// * The schema is invalid
    /// * The catalog fails to create the table metadata
    /// * The table location cannot be initialized
    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        create_table: CreateTable,
    ) -> Result<Table, Error>;
    /// Creates a new view in the catalog with the specified configuration.
    ///
    /// # Arguments
    /// * `identifier` - The identifier for the new view
    /// * `create_view` - Configuration for the view creation including view definition and properties
    ///
    /// # Returns
    /// * `Result<View, Error>` - The newly created view object
    ///
    /// # Errors
    /// Returns an error if:
    /// * The view already exists
    /// * The namespace doesn't exist
    /// * The view definition is invalid
    /// * The catalog fails to create the view metadata
    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateView<Option<()>>,
    ) -> Result<View, Error>;
    /// Creates a new materialized view in the catalog with the specified configuration.
    ///
    /// # Arguments
    /// * `identifier` - The identifier for the new materialized view
    /// * `create_view` - Configuration for the materialized view creation including view definition,
    ///                  storage properties, and refresh policies
    ///
    /// # Returns
    /// * `Result<MaterializedView, Error>` - The newly created materialized view object
    ///
    /// # Errors
    /// Returns an error if:
    /// * The materialized view already exists
    /// * The namespace doesn't exist
    /// * The view definition is invalid
    /// * The catalog fails to create the view metadata
    /// * The storage location cannot be initialized
    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, Error>;
    /// Updates a table's metadata by applying the specified commit operation.
    ///
    /// # Arguments
    /// * `commit` - The commit operation containing metadata updates to apply
    ///
    /// # Returns
    /// * `Result<Table, Error>` - The updated table object
    ///
    /// # Errors
    /// Returns an error if:
    /// * The table doesn't exist
    /// * The table is locked by another operation
    /// * The commit operation is invalid
    /// * The catalog fails to update the metadata
    /// * Concurrent modifications conflict with this update
    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, Error>;
    /// Updates a view's metadata by applying the specified commit operation.
    ///
    /// # Arguments
    /// * `commit` - The commit operation containing metadata updates to apply
    ///
    /// # Returns
    /// * `Result<View, Error>` - The updated view object
    ///
    /// # Errors
    /// Returns an error if:
    /// * The view doesn't exist
    /// * The view is locked by another operation
    /// * The commit operation is invalid
    /// * The catalog fails to update the metadata
    /// * Concurrent modifications conflict with this update
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
