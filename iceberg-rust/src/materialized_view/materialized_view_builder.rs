/*!
Defining the [MaterializedViewBuilder] struct for creating catalog views and starting create/replace transactions
*/

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use iceberg_rust_spec::spec::{
    materialized_view_metadata::MaterializedViewMetadataBuilder,
    schema::Schema,
    table_metadata::TableMetadataBuilder,
    view_metadata::{VersionBuilder, ViewProperties, ViewRepresentation, REF_PREFIX},
};

use crate::{
    catalog::{identifier::Identifier, Catalog},
    error::Error,
};

use super::MaterializedView;

static STORAGE_TABLE_POSTFIX: &str = "__storage";

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
                    .with_representation(ViewRepresentation::Sql {
                        sql: sql.to_string(),
                        dialect: "ANSI".to_string(),
                    })
                    .schema_id(1)
                    .build()?,
            ))
            .current_version_id(1)
            .properties(ViewProperties {
                storage_table: identifier.to_string() + STORAGE_TABLE_POSTFIX,
                other: HashMap::from_iter(vec![
                    (REF_PREFIX.to_string() + "main", 1.to_string()),
                    (
                        "iceberg.materialized-views.hide-storage-table".to_owned(),
                        "false".to_owned(),
                    ),
                ]),
            });
        Ok(Self {
            identifier: Identifier::parse(&identifier.to_string())?,
            catalog,
            metadata: builder,
        })
    }

    /// Building a materialized view writes the metadata file to the object store and commits the table to the metastore
    pub async fn build(self) -> Result<MaterializedView, Error> {
        let metadata = self.metadata.build()?;
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
        self.catalog
            .clone()
            .create_table(
                Identifier::parse(&metadata.properties.storage_table)?,
                table_metadata,
            )
            .await?;
        self.catalog
            .create_materialized_view(self.identifier, metadata)
            .await
    }
}
