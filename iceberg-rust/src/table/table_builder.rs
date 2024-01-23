/*!
Defining the [TableBuilder] struct for creating catalog tables and starting create/replace transactions
*/

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use uuid::Uuid;

use crate::catalog::{bucket::parse_bucket, tabular::Tabular};
use crate::table::Table;
use crate::{catalog::identifier::Identifier, error::Error};
use iceberg_rust_spec::{spec::table_metadata::TableMetadataBuilder, util::strip_prefix};

use super::Catalog;

///Builder pattern to create a table
pub struct TableBuilder {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: TableMetadataBuilder,
}

impl Deref for TableBuilder {
    type Target = TableMetadataBuilder;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl DerefMut for TableBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl TableBuilder {
    /// Creates a new [TableBuilder] to create a Metastore Table with some default metadata entries already set.
    pub fn new(identifier: impl ToString, catalog: Arc<dyn Catalog>) -> Result<Self, Error> {
        Ok(TableBuilder {
            metadata: TableMetadataBuilder::default(),
            catalog,
            identifier: Identifier::parse(&identifier.to_string())?,
        })
    }
    /// Building a table writes the metadata file and commits the table to either the metastore or the filesystem
    pub async fn build(&mut self) -> Result<Table, Error> {
        // Create metadata
        let mut metadata = self.metadata.build()?;
        let last_column_id = metadata
            .schemas
            .values()
            .flat_map(|x| x.fields.fields.iter())
            .map(|x| x.id)
            .max()
            .unwrap_or(0);
        metadata.last_column_id = last_column_id;
        let last_column_id = metadata
            .partition_specs
            .values()
            .flat_map(|x| x.fields.iter())
            .map(|x| x.field_id)
            .max()
            .unwrap_or(0);
        metadata.last_partition_id = last_column_id;
        metadata.properties.insert(
            "write.parquet.compression-codec".to_owned(),
            "zstd".to_owned(),
        );
        metadata
            .properties
            .insert("write.parquet.compression-level".to_owned(), 1.to_string());

        // Write metadata to object_store
        let bucket = parse_bucket(&metadata.location)?;
        let object_store = self.catalog.object_store(bucket);

        let location = &metadata.location;
        let uuid = Uuid::new_v4();
        let version = &metadata.last_sequence_number;
        let metadata_json = serde_json::to_string(&metadata)?;
        let path = location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json";
        object_store
            .put(&strip_prefix(&path).into(), metadata_json.into())
            .await?;

        // Register table in catalog
        if let Tabular::Table(table) = self
            .catalog
            .clone()
            .register_table(self.identifier.clone(), path.as_ref())
            .await?
        {
            Ok(table)
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }
    }
}
