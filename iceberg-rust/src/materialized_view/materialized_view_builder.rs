/*!
Defining the [MaterializedViewBuilder] struct for creating catalog views and starting create/replace transactions
*/

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use iceberg_rust_spec::{
    spec::{
        materialized_view_metadata::{
            FormatVersion, MaterializedViewMetadataBuilder, MaterializedViewRepresentation,
        },
        schema::Schema,
        table_metadata::TableMetadataBuilder,
        view_metadata::VersionBuilder,
    },
    util::strip_prefix,
};
use uuid::Uuid;

use crate::{
    catalog::{bucket::parse_bucket, identifier::Identifier, tabular::Tabular, Catalog},
    error::Error,
};

use super::MaterializedView;

///Builder pattern to create a view
pub struct MaterializedViewBuilder {
    identifier: Identifier,
    catalog: Arc<dyn Catalog>,
    metadata: MaterializedViewMetadataBuilder,
}

impl Deref for MaterializedViewBuilder {
    type Target = MaterializedViewMetadataBuilder;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl DerefMut for MaterializedViewBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl MaterializedViewBuilder {
    /// Create new builder for materialized view metadata
    pub fn new(
        sql: impl ToString,
        identifier: impl ToString,
        schema: Schema,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self, Error> {
        let mut builder = MaterializedViewMetadataBuilder::default();
        builder
            .with_schema((1, schema))
            .with_version((
                1,
                VersionBuilder::default()
                    .version_id(1)
                    .with_representation(MaterializedViewRepresentation::SqlMaterialized {
                        sql: sql.to_string(),
                        dialect: "ANSI".to_string(),
                        format_version: FormatVersion::V1,
                        storage_table: identifier.to_string() + "_storage",
                    })
                    .schema_id(1)
                    .build()?,
            ))
            .current_version_id(1);
        Ok(Self {
            identifier: Identifier::parse(&identifier.to_string())?,
            catalog,
            metadata: builder,
        })
    }

    /// Building a materialized view writes the metadata file to the object store and commits the table to the metastore
    pub async fn build(self) -> Result<MaterializedView, Error> {
        let metadata = self.metadata.build()?;
        let bucket = parse_bucket(&metadata.location)?;
        let table_identifier =
            Identifier::parse(match &metadata.current_version(None)?.representations[0] {
                MaterializedViewRepresentation::SqlMaterialized {
                    sql: _,
                    dialect: _,
                    format_version: _,
                    storage_table,
                } => storage_table,
            })?;
        let schema_id = &metadata.current_version(None)?.schema_id;
        let table_metadata = TableMetadataBuilder::default()
            .location(&metadata.location)
            .with_schema((
                *schema_id,
                metadata
                    .schemas
                    .get(schema_id)
                    .ok_or(Error::InvalidFormat("schema in metadata".to_string()))?
                    .clone(),
            ))
            .current_schema_id(*schema_id)
            .build()?;
        let object_store = self.catalog.object_store(bucket);
        let location = &metadata.location;
        let metadata_json = serde_json::to_string(&metadata)?;
        let path = location.to_string()
            + "/metadata/"
            + &metadata.current_version_id.to_string()
            + "-"
            + &Uuid::new_v4().to_string()
            + ".metadata.json";
        object_store
            .put(&strip_prefix(&path).into(), metadata_json.into())
            .await?;
        let table_metadata_json = serde_json::to_string(&table_metadata)?;
        let table_path = location.to_string()
            + "/metadata/"
            + &table_metadata.last_sequence_number.to_string()
            + "-"
            + &Uuid::new_v4().to_string()
            + ".metadata.json";
        object_store
            .put(
                &strip_prefix(&table_path).into(),
                table_metadata_json.into(),
            )
            .await?;
        if let Tabular::Table(_) = self
            .catalog
            .clone()
            .register_table(table_identifier, table_path.as_ref())
            .await?
        {
            Ok(())
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }?;
        if let Tabular::MaterializedView(matview) = self
            .catalog
            .register_table(self.identifier, path.as_ref())
            .await?
        {
            Ok(matview)
        } else {
            Err(Error::InvalidFormat(
                "Entity returned from catalog".to_string(),
            ))
        }
    }
}
