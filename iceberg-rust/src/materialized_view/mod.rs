//AI! write module documentation

use std::sync::Arc;

use iceberg_rust_spec::spec::{
    materialized_view_metadata::MaterializedViewMetadata, schema::Schema,
};
use object_store::ObjectStore;

use crate::{
    catalog::{
        create::CreateMaterializedViewBuilder, identifier::Identifier, tabular::Tabular, Catalog,
    },
    error::Error,
    object_store::Bucket,
};

use self::{storage_table::StorageTable, transaction::Transaction as MaterializedViewTransaction};

mod storage_table;
pub mod transaction;

/// Default postfix for the storage table identifier
pub static STORAGE_TABLE_POSTFIX: &str = "__storage";
/// Flag to mark a table as a storage table
pub static STORAGE_TABLE_FLAG: &str = "materialize.storage_table";

#[derive(Debug, Clone)]
/// A materialized view in Apache Iceberg that maintains a physical copy of query results
/// in a storage table. The view provides ACID guarantees and can be refreshed to
/// stay in sync with changes in the source tables.
///
/// The materialized view consists of:
/// * A view definition (SQL or other representation)
/// * A storage table containing the materialized data
/// * Metadata tracking the freshness state relative to source tables
pub struct MaterializedView {
    /// Type of the View, either filesystem or metastore.
    identifier: Identifier,
    /// Metadata for the iceberg view according to the iceberg view spec
    metadata: MaterializedViewMetadata,
    /// Catalog of the table
    catalog: Arc<dyn Catalog>,
}

/// Storage table states
#[derive(Debug)]
pub enum SourceTableState {
    /// Data in storage table is fresh
    Fresh,
    /// Data in storage table is outdated
    Outdated(i64),
    /// Data in storage table is invalid
    Invalid,
}

/// Public interface of the table.
impl MaterializedView {
    /// Creates a new builder for configuring and creating a materialized view
    ///
    /// Returns a `CreateMaterializedViewBuilder` that provides a fluent interface for:
    /// - Setting the view name and location
    /// - Configuring view properties
    /// - Defining the view schema and SQL representation
    /// - Specifying the catalog and storage table settings
    pub fn builder() -> CreateMaterializedViewBuilder {
        CreateMaterializedViewBuilder::default()
    }

    /// Creates a new materialized view instance with the given identifier, catalog and metadata
    ///
    /// # Arguments
    /// * `identifier` - The unique identifier for this view in the catalog
    /// * `catalog` - The catalog that will store this view's metadata
    /// * `metadata` - The view metadata containing schema, properties, etc.
    ///
    /// # Returns
    /// * `Result<MaterializedView, Error>` - The created materialized view or an error
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: MaterializedViewMetadata,
    ) -> Result<Self, Error> {
        Ok(MaterializedView {
            identifier,
            metadata,
            catalog,
        })
    }
    /// Returns the unique identifier for this materialized view in the catalog
    ///
    /// The identifier contains the namespace and name that uniquely identify
    /// this view within its catalog
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    /// Returns a reference to the catalog that stores this materialized view
    ///
    /// The catalog manages the view's metadata and provides ACID guarantees
    /// for operations on the view
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    /// Returns the object store used by this materialized view for data storage
    ///
    /// The object store provides access to the underlying storage system (e.g. S3, local filesystem)
    /// where the view's data files are stored. The store is configured based on the view's location.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog
            .object_store(Bucket::from_path(&self.metadata.location).unwrap())
    }
    /// Returns the current schema for this materialized view
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to get the schema for. If None, returns the main branch schema
    ///
    /// # Returns
    /// * `Result<&Schema, Error>` - The current schema or an error if it cannot be found
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    /// Returns a reference to this materialized view's metadata
    ///
    /// The metadata contains the view's schema, properties, version history,
    /// and other configuration details as defined by the Apache Iceberg spec
    pub fn metadata(&self) -> &MaterializedViewMetadata {
        &self.metadata
    }
    /// Creates a new transaction for performing atomic operations on this materialized view
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to perform the transaction on. If None, uses the main branch
    ///
    /// # Returns
    /// A new transaction that can be used to perform multiple operations atomically
    pub fn new_transaction(&mut self, branch: Option<&str>) -> MaterializedViewTransaction {
        MaterializedViewTransaction::new(self, branch)
    }
    /// Returns the storage table that contains the materialized data for this view
    ///
    /// The storage table is a regular Iceberg table that stores the physical data
    /// for this materialized view. It is managed internally by the view and should
    /// not be modified directly.
    ///
    /// # Returns
    /// * `Result<StorageTable, Error>` - The storage table or an error if it cannot be loaded
    pub async fn storage_table(&self) -> Result<StorageTable, Error> {
        let identifier = self.metadata().current_version(None)?.storage_table();
        if let Tabular::Table(table) = self.catalog().load_tabular(&identifier.into()).await? {
            Ok(StorageTable::new(table))
        } else {
            Err(Error::InvalidFormat("storage table".to_string()))
        }
    }
}
