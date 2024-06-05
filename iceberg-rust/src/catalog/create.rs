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
    materialized_view_metadata::MaterializedViewMetadata,
    partition::{PartitionSpec, DEFAULT_PARTITION_SPEC_ID},
    schema::{Schema, DEFAULT_SCHEMA_ID},
    sort::{SortOrder, DEFAULT_SORT_ORDER_ID},
    table_metadata::TableMetadata,
    view_metadata::{Version, ViewMetadata, ViewProperties, DEFAULT_VERSION_ID},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::Error,
    materialized_view::{MaterializedView, STORAGE_TABLE_POSTFIX},
    table::Table,
    view::View,
};

use super::{identifier::Identifier, Catalog};

/// Create Table struct
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(build_fn(name = "create"), setter(prefix = "with"))]
pub struct CreateTable {
    #[builder(setter(into))]
    /// Name of the table
    pub name: String,
    /// Location tables base location
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub location: Option<String>,
    /// Table schemma
    pub schema: Schema,
    /// Partition spec
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub partition_spec: Option<PartitionSpec>,
    /// Sort order
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option, name = "with_sort_order"), default)]
    pub write_order: Option<SortOrder>,
    /// stage create
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub stage_create: Option<bool>,
    /// Table properties
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option, each(name = "with_property")), default)]
    pub properties: Option<HashMap<String, String>>,
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

/// Create view struct
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(build_fn(name = "create"), setter(prefix = "with"))]
pub struct CreateView<T: Clone + Default> {
    /// Name of the view
    #[builder(setter(into))]
    pub name: String,
    /// View base location
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub location: Option<String>,
    /// Schema of the view
    pub schema: Schema,
    /// Viersion of the view
    pub view_version: Version,
    /// View properties
    #[builder(setter(each(name = "with_property")), default)]
    pub properties: ViewProperties<T>,
}

impl CreateViewBuilder<Option<()>> {
    /// Build the table, registering it in a catalog
    pub async fn build(
        &mut self,
        namespace: &[String],
        catalog: Arc<dyn Catalog>,
    ) -> Result<View, Error> {
        let name = self
            .name
            .as_ref()
            .ok_or(Error::NotFound("View".to_owned(), "name".to_owned()))?;
        let identifier = Identifier::new(namespace, name);

        let create = self.create()?;

        // Register table in catalog
        catalog.clone().create_view(identifier, create).await
    }
}

impl TryInto<ViewMetadata> for CreateView<Option<()>> {
    type Error = Error;
    fn try_into(self) -> Result<ViewMetadata, Self::Error> {
        Ok(ViewMetadata {
            view_uuid: Uuid::new_v4(),
            format_version: Default::default(),
            location: self
                .location
                .ok_or(Error::NotFound("Table".to_owned(), "location".to_owned()))?,
            current_version_id: DEFAULT_VERSION_ID,
            versions: HashMap::from_iter(vec![(DEFAULT_VERSION_ID, self.view_version)]),
            version_log: Vec::new(),
            schemas: HashMap::from_iter(vec![(DEFAULT_SCHEMA_ID, self.schema)]),
            properties: self.properties,
        })
    }
}

impl TryInto<MaterializedViewMetadata> for CreateView<String> {
    type Error = Error;
    fn try_into(self) -> Result<MaterializedViewMetadata, Self::Error> {
        Ok(MaterializedViewMetadata {
            view_uuid: Uuid::new_v4(),
            format_version: Default::default(),
            location: self
                .location
                .ok_or(Error::NotFound("Table".to_owned(), "location".to_owned()))?,
            current_version_id: DEFAULT_VERSION_ID,
            versions: HashMap::from_iter(vec![(DEFAULT_VERSION_ID, self.view_version)]),
            version_log: Vec::new(),
            schemas: HashMap::from_iter(vec![(DEFAULT_SCHEMA_ID, self.schema)]),
            properties: self.properties,
        })
    }
}

/// Create materialized view struct
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(build_fn(name = "create"), setter(prefix = "with"))]
pub struct CreateMaterializedView {
    /// Name of the view
    #[builder(setter(into))]
    pub name: String,
    /// View base location
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub location: Option<String>,
    /// Schema of the view
    pub schema: Schema,
    /// Viersion of the view
    pub view_version: Version,
    /// View properties
    #[builder(setter(each(name = "with_property")), default)]
    pub properties: ViewProperties<String>,
    /// Partition spec
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub partition_spec: Option<PartitionSpec>,
    /// Sort order
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option, name = "with_sort_order"), default)]
    pub write_order: Option<SortOrder>,
    /// stage create
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option), default)]
    pub stage_create: Option<bool>,
    /// Table properties
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option, each(name = "with_table_property")), default)]
    pub table_properties: Option<HashMap<String, String>>,
}

impl CreateMaterializedViewBuilder {
    /// Build the table, registering it in a catalog
    pub async fn build(
        &mut self,
        namespace: &[String],
        catalog: Arc<dyn Catalog>,
    ) -> Result<MaterializedView, Error> {
        let name = self
            .name
            .as_ref()
            .ok_or(Error::NotFound("View".to_owned(), "name".to_owned()))?;
        let identifier = Identifier::new(namespace, name);

        if let Some(properties) = &mut self.properties {
            properties.storage_table = identifier.to_string() + STORAGE_TABLE_POSTFIX;
        } else {
            self.properties = Some(ViewProperties {
                storage_table: identifier.to_string() + STORAGE_TABLE_POSTFIX,
                other: HashMap::new(),
            })
        };

        let create = self.create()?;

        // Register materialized view in catalog
        catalog
            .clone()
            .create_materialized_view(identifier.clone(), create)
            .await
    }
}

impl Into<(CreateView<String>, CreateTable)> for CreateMaterializedView {
    fn into(self) -> (CreateView<String>, CreateTable) {
        let storage_table = self.properties.storage_table.clone();
        (
            CreateView {
                name: self.name.clone(),
                location: self.location.clone(),
                schema: self.schema.clone(),
                view_version: self.view_version,
                properties: self.properties,
            },
            CreateTable {
                name: storage_table,
                location: self.location,
                schema: self.schema,
                partition_spec: self.partition_spec,
                write_order: self.write_order,
                stage_create: self.stage_create,
                properties: self.table_properties,
            },
        )
    }
}
