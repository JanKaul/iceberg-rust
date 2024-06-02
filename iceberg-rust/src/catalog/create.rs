/*!
Defining the [CreateTable] struct for creating catalog tables and starting create/replace transactions
*/
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use derive_builder::Builder;
use iceberg_rust_spec::spec::{
    partition::{PartitionSpec, DEFAULT_PARTITION_SPEC_ID},
    schema::{Schema, DEFAULT_SCHEMA_ID},
    sort::{SortOrder, DEFAULT_SORT_ORDER_ID},
    table_metadata::TableMetadata,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{error::Error, table::Table};

use super::{identifier::Identifier, Catalog};

/// Create Table struct
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(build_fn(name = "create"), setter(prefix = "with"))]
pub struct CreateTable {
    #[builder(setter(into))]
    /// Name of the table
    name: String,
    /// Location tables base location
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    location: Option<String>,
    /// Table schemma
    schema: Schema,
    /// Partition spec
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    partition_spec: Option<PartitionSpec>,
    /// Sort order
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option, name = "with_sort_order"), default)]
    write_order: Option<SortOrder>,
    /// stage create
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    stage_create: Option<bool>,
    /// Table properties
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option, each(name = "with_property")), default)]
    properties: Option<HashMap<String, String>>,
}

impl CreateTableBuilder {
    /// Build the table, registering it in a catalog
    pub async fn build(
        &mut self,
        namespace: &[String],
        catalog: Arc<dyn Catalog>,
    ) -> Result<Table, Error> {
        let name = self
            .name
            .as_ref()
            .ok_or(Error::NotFound("Table".to_owned(), "name".to_owned()))?;
        let identifier = Identifier::new(namespace, name);

        let create = self
            .with_property((
                "write.parquet.compression-codec".to_owned(),
                "zstd".to_owned(),
            ))
            .with_property(("write.parquet.compression-level".to_owned(), 1.to_string()))
            .create()?;

        // Register table in catalog
        catalog.clone().create_table(identifier, create).await
    }
}

impl TryInto<TableMetadata> for CreateTable {
    type Error = Error;
    fn try_into(self) -> Result<TableMetadata, Self::Error> {
        let last_column_id = self.schema.fields().iter().map(|x| x.id).max().unwrap_or(0);

        let last_partition_id = self
            .partition_spec
            .as_ref()
            .and_then(|x| x.fields().iter().map(|x| *x.field_id()).max())
            .unwrap_or(0);

        Ok(TableMetadata {
            format_version: Default::default(),
            table_uuid: Uuid::new_v4(),
            location: self
                .location
                .ok_or(Error::NotFound("Table".to_owned(), "location".to_owned()))?,
            last_sequence_number: 0,
            last_updated_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64,
            last_column_id,
            schemas: HashMap::from_iter(vec![(DEFAULT_SCHEMA_ID, self.schema)]),
            current_schema_id: DEFAULT_SCHEMA_ID,
            partition_specs: HashMap::from_iter(vec![(
                DEFAULT_PARTITION_SPEC_ID,
                self.partition_spec.unwrap_or_default(),
            )]),
            default_spec_id: DEFAULT_PARTITION_SPEC_ID,
            last_partition_id,
            properties: self.properties.unwrap_or_default(),
            current_snapshot_id: None,
            snapshots: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: Vec::new(),
            sort_orders: HashMap::from_iter(vec![(
                DEFAULT_SORT_ORDER_ID,
                self.write_order.unwrap_or_default(),
            )]),
            default_sort_order_id: DEFAULT_SORT_ORDER_ID,
            refs: HashMap::new(),
        })
    }
}
