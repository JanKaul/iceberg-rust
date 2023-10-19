/*!
 * Defines the [MaterializedView] struct that represents an iceberg materialized view.
*/

use std::sync::Arc;

use anyhow::anyhow;
use object_store::ObjectStore;

use crate::{
    catalog::{identifier::Identifier, relation::Relation, Catalog},
    spec::{
        materialized_view_metadata::{MaterializedViewMetadata, MaterializedViewRepresentation},
        schema::Schema,
    },
    table::Table,
};

use self::transaction::Transaction as MaterializedViewTransaction;

pub mod transaction;

/// An iceberg materialized view
pub struct MaterializedView {
    /// Type of the View, either filesystem or metastore.
    identifier: Identifier,
    /// Metadata for the iceberg view according to the iceberg view spec
    metadata: MaterializedViewMetadata,
    /// Path to the current metadata location
    metadata_location: String,
    /// Catalog of the table
    catalog: Arc<dyn Catalog>,
    /// Storage table
    storage_table: Table,
}

/// Public interface of the table.
impl MaterializedView {
    /// Create a new metastore view
    pub async fn new(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: MaterializedViewMetadata,
        metadata_location: &str,
    ) -> Result<Self, anyhow::Error> {
        let storage_table = match &metadata.current_version()?.representations[0] {
            MaterializedViewRepresentation::SqlMaterialized {
                sql: _sql,
                dialect: _dialect,
                format_version: _format_version,
                storage_table,
            } => storage_table,
        };
        let storage_table = if let Relation::Table(table) = catalog
            .clone()
            .load_table(&Identifier::parse(
                storage_table.trim_start_matches("catalog:"),
            )?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Storage table must be a table."))
        }?;
        Ok(MaterializedView {
            identifier,
            metadata,
            metadata_location: metadata_location.to_string(),
            catalog,
            storage_table,
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
        self.catalog.object_store()
    }
    /// Get the schema of the view
    pub fn schema(&self) -> Result<&Schema, anyhow::Error> {
        self.metadata.current_schema()
    }
    /// Get the metadata of the view
    pub fn metadata(&self) -> &MaterializedViewMetadata {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Get storage table of the materialized view
    pub fn storage_table(&self) -> &Table {
        &self.storage_table
    }
    /// Create a new transaction for this view
    pub fn new_transaction(&mut self) -> MaterializedViewTransaction {
        MaterializedViewTransaction::new(self)
    }
}
