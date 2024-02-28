/*!
Defining the [MaterializedViewBuilder] struct for creating catalog views and starting create/replace transactions
*/

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use iceberg_rust_spec::{
    spec::{
        materialized_view_metadata::MaterializedViewMetadataBuilder,
        schema::Schema,
        table_metadata::TableMetadataBuilder,
        view_metadata::{VersionBuilder, ViewProperties, ViewRepresentation, REF_PREFIX},
    },
    util::strip_prefix,
};
use uuid::Uuid;

use crate::{
    catalog::{bucket::parse_bucket, identifier::Identifier, Catalog},
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
                    .with_representation(ViewRepresentation::Sql {
                        sql: sql.to_string(),
                        dialect: "ANSI".to_string(),
                    })
                    .schema_id(1)
                    .build()?,
            ))
            .current_version_id(1)
            .properties(ViewProperties {
                metadata_location: "".to_owned(),
                other: HashMap::from_iter(vec![(REF_PREFIX.to_string() + "main", 1.to_string())]),
            });
        Ok(Self {
            identifier: Identifier::parse(&identifier.to_string())?,
            catalog,
            metadata: builder,
        })
    }

    /// Building a materialized view writes the metadata file to the object store and commits the table to the metastore
    pub async fn build(self) -> Result<MaterializedView, Error> {
        let mut metadata = self.metadata.build()?;
        let bucket = parse_bucket(&metadata.location)?;
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
        let table_path = location.to_string()
            + "/metadata/"
            + &table_metadata.last_sequence_number.to_string()
            + "-"
            + &Uuid::new_v4().to_string()
            + ".metadata.json";
        metadata.properties.metadata_location = table_path.clone();
        let table_metadata_json = serde_json::to_string(&table_metadata)?;
        object_store
            .put(
                &strip_prefix(&table_path).into(),
                table_metadata_json.into(),
            )
            .await?;
        self.catalog
            .create_materialized_view(self.identifier, metadata)
            .await
    }
}
