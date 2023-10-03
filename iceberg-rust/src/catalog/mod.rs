/*!
Defines traits to communicate with an iceberg catalog.
*/

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

pub mod identifier;
pub mod namespace;

use identifier::Identifier;
use object_store::ObjectStore;

use self::namespace::Namespace;
use self::relation::Relation;

pub mod relation;

#[cfg(not(target_arch = "wasm32"))]
pub mod memory;

/// Trait to create, replace and drop tables in an iceberg catalog.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
    /// Lists all tables in the given namespace.
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<Identifier>>;
    /// Lists all namespaces in the catalog.
    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>>;
    /// Create a table from an identifier and a schema
    /// Check if a table exists
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool>;
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &Identifier) -> Result<()>;
    /// Load a table.
    async fn load_table(self: Arc<Self>, identifier: &Identifier) -> Result<Relation>;
    /// Invalidate cached table metadata from current catalog.
    async fn invalidate_table(&self, identifier: &Identifier) -> Result<()>;
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_file_location: &str,
    ) -> Result<Relation>;
    /// Update a table by atomically changing the pointer to the metadata file
    async fn update_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata_file_location: &str,
        previous_metadata_file_location: &str,
    ) -> Result<Relation>;
    /// Initialize a catalog given a custom name and a map of catalog properties.
    /// A custom Catalog implementation must have a no-arg constructor. A compute engine like Spark
    /// or Flink will first initialize the catalog without any arguments, and then call this method to
    /// complete catalog initialization with properties passed into the engine.
    async fn initialize(self: Arc<Self>, properties: &HashMap<String, String>) -> Result<()>;
    /// Return the associated object store to the catalog
    fn object_store(&self) -> Arc<dyn ObjectStore>;
}
