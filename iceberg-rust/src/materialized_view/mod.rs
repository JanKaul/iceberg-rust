/*!
 * Defines the [MaterializedView] struct that represents an iceberg materialized view.
*/

use std::sync::Arc;

use iceberg_rust_spec::spec::{
    materialized_view_metadata::{MaterializedViewMetadata, MaterializedViewRepresentation},
    schema::Schema,
};
use object_store::ObjectStore;

use crate::{
    catalog::{identifier::Identifier, tabular::Tabular, Catalog, CatalogList},
    error::Error,
};

use self::{storage_table::StorageTable, transaction::Transaction as MaterializedViewTransaction};

pub mod materialized_view_builder;
mod storage_table;
pub mod transaction;

#[derive(Debug)]
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
    /// List of catalogs that may be referenced by the query definition
    catalog_list: Arc<dyn CatalogList>,
}

/// Public interface of the table.
impl MaterializedView {
    /// Create a new metastore view
    pub async fn new(
        identifier: Identifier,
        metadata: MaterializedViewMetadata,
        metadata_location: &str,
        catalog_name: &str,
        catalog_list: Arc<dyn CatalogList>,
    ) -> Result<Self, Error> {
        let catalog = catalog_list
            .catalog(catalog_name)
            .await
            .ok_or(Error::NotFound(
                "Catalog".to_owned(),
                catalog_name.to_owned(),
            ))?;
        Ok(MaterializedView {
            identifier,
            metadata,
            metadata_location: metadata_location.to_string(),
            catalog,
            catalog_list,
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
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        self.metadata.current_schema(branch).map_err(Error::from)
    }
    /// Get the metadata of the view
    pub fn metadata(&self) -> &MaterializedViewMetadata {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Create a new transaction for this view
    pub fn new_transaction(&mut self, branch: Option<&str>) -> MaterializedViewTransaction {
        MaterializedViewTransaction::new(self, branch)
    }
    /// Get the storage table of the materialized view
    pub async fn storage_table(&self, branch: Option<&str>) -> Result<StorageTable, Error> {
        let (storage_table_name, sql) =
            match &self.metadata.current_version(branch)?.representations[0] {
                MaterializedViewRepresentation::SqlMaterialized {
                    sql,
                    dialect: _dialect,
                    format_version: _format_version,
                    storage_table,
                } => (storage_table, sql),
            };
        if let Tabular::Table(table) = self
            .catalog
            .clone()
            .load_table(&Identifier::parse(
                storage_table_name.trim_start_matches("identifier:"),
            )?)
            .await?
        {
            Ok(StorageTable {
                table,
                sql: sql.to_owned(),
                catalog_list: self.catalog_list.clone(),
            })
        } else {
            Err(Error::InvalidFormat("storage table".to_string()))
        }
    }
}
