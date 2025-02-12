/*!
 * Defines the [MaterializedView] struct that represents an iceberg materialized view.
*/

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
    //AI! Write documentation
    pub fn builder() -> CreateMaterializedViewBuilder {
        CreateMaterializedViewBuilder::default()
    }

    /// Create a new metastore view
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
    /// Get the table identifier in the catalog. Returns None of it is a filesystem view.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
    /// Get the catalog associated to the view. Returns None if the view is a filesystem view
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
    /// Get the object_store associated to the view
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.catalog
            .object_store(Bucket::from_path(&self.metadata.location).unwrap())
    }
    /// Get the schema of the view
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    /// Get the metadata of the view
    pub fn metadata(&self) -> &MaterializedViewMetadata {
        &self.metadata
    }
    /// Create a new transaction for this view
    pub fn new_transaction(&mut self, branch: Option<&str>) -> MaterializedViewTransaction {
        MaterializedViewTransaction::new(self, branch)
    }
    /// Get the storage table of the materialized view
    pub async fn storage_table(&self) -> Result<StorageTable, Error> {
        let identifier = self.metadata().current_version(None)?.storage_table();
        if let Tabular::Table(table) = self.catalog().load_tabular(&identifier.into()).await? {
            Ok(StorageTable::new(table))
        } else {
            Err(Error::InvalidFormat("storage table".to_string()))
        }
    }
}
