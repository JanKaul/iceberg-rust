/*!
Defining the [TableBuilder] struct for creating catalog tables and starting create/replace transactions
*/

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use object_store::path::Path;
use uuid::Uuid;

use crate::catalog::identifier::Identifier;
use crate::catalog::relation::Relation;
use crate::spec::table_metadata::TableMetadataBuilder;
use crate::table::Table;
use anyhow::{anyhow, Result};

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
    pub fn new(identifier: impl ToString, catalog: Arc<dyn Catalog>) -> Result<Self> {
        Ok(TableBuilder {
            metadata: TableMetadataBuilder::default(),
            catalog,
            identifier: Identifier::parse(&identifier.to_string())?,
        })
    }
    /// Building a table writes the metadata file and commits the table to either the metastore or the filesystem
    pub async fn build(&mut self) -> Result<Table> {
        let object_store = self.catalog.object_store();
        let metadata = self.metadata.build()?;
        let location = &metadata.location;
        let uuid = Uuid::new_v4();
        let version = &metadata.last_sequence_number;
        let metadata_json =
            serde_json::to_string(&metadata).map_err(|err| anyhow!(err.to_string()))?;
        let path: Path = (location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json")
            .into();
        object_store
            .put(&path, metadata_json.into())
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        if let Relation::Table(table) = self
            .catalog
            .clone()
            .register_table(self.identifier.clone(), path.as_ref())
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Building the table failed because registering the table in the catalog didn't return a table."))
        }
    }
}
