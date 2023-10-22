/*!
 * Defines the [MaterializedView] struct that represents an iceberg materialized view.
*/

use std::sync::Arc;

use anyhow::{anyhow, Ok};
use object_store::ObjectStore;

use futures::{stream, StreamExt, TryStreamExt};

use crate::{
    catalog::{identifier::Identifier, relation::Relation, Catalog},
    file_format::DatafileMetadata,
    spec::{
        materialized_view_metadata::{
            Freshness, MaterializedViewMetadata, MaterializedViewRepresentation,
        },
        schema::Schema,
        table_metadata::TableMetadataBuilder,
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
    /// Check if the materialized view is fresh
    pub async fn is_fresh(&self) -> Result<bool, anyhow::Error> {
        let catalog = self.storage_table.catalog().clone();
        let snapshot = if let Some(snapshot) = self.storage_table.metadata().current_snapshot()? {
            snapshot
        } else {
            return Ok(false);
        };
        let json = if let Some(json) = snapshot.summary.other.get("freshness") {
            json
        } else {
            return Ok(false);
        };
        let freshness = serde_json::from_str::<Freshness>(json)?;
        stream::iter(freshness.base_tables.into_iter())
            .then(|(pointer, snapshot_id)| {
                let catalog = catalog.clone();
                async move {
                    if !pointer.starts_with("identifier:") {
                        return Err(anyhow!("Only identifiers supported as base table pointers"));
                    }
                    let base_table = match catalog
                        .load_table(&Identifier::parse(
                            &pointer.trim_start_matches("identifier:"),
                        )?)
                        .await?
                    {
                        Relation::Table(table) => table,
                        Relation::MaterializedView(mv) => mv.storage_table,
                        _ => return Err(anyhow!("Base table must be a table")),
                    };
                    if base_table
                        .metadata()
                        .current_snapshot()?
                        .unwrap()
                        .snapshot_id
                        == snapshot_id
                    {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
            })
            .try_fold(true, |acc, x| async move { Ok(acc && x) })
            .await
    }
    /// Replace the entire storage table with new datafiles
    pub async fn full_refresh(
        &mut self,
        files: Vec<(String, DatafileMetadata)>,
    ) -> Result<(), anyhow::Error> {
        let table_identifier = self.storage_table.identifier().clone();
        let table_catalog = self.storage_table.catalog().clone();
        let table_metadata_location = self.storage_table.metadata_location();
        let table_metadata = self.storage_table.metadata();
        let table_metadata = TableMetadataBuilder::default()
            .format_version(table_metadata.format_version.clone())
            .location(table_metadata.location.clone())
            .schemas(table_metadata.schemas.clone())
            .current_schema_id(table_metadata.current_schema_id)
            .partition_specs(table_metadata.partition_specs.clone())
            .default_spec_id(table_metadata.default_spec_id)
            .build()?;
        let metadata_location = self.storage_table.new_metadata_location()?;

        let mut table = Table::new(
            table_identifier.clone(),
            table_catalog.clone(),
            table_metadata,
            &metadata_location,
        )
        .await?;
        table.new_transaction().append(files).commit().await?;
        table_catalog
            .update_table(
                table_identifier,
                &metadata_location,
                table_metadata_location,
            )
            .await?;
        let storage_table = std::mem::replace(&mut self.storage_table, table);
        storage_table.drop().await?;
        Ok(())
    }
}
