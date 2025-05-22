//! Creation interfaces for Iceberg catalog objects
//!
//! This module provides builder-pattern implementations for creating new objects in an Iceberg catalog:
//!
//! * Tables with schema, partition specs, and sort orders
//! * Views with schema and version specifications
//! * Materialized views with both view metadata and storage tables
//!
//! All builders support fluent configuration and handle default values appropriately.
//! The module ensures proper initialization of metadata like UUIDs and timestamps.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use derive_builder::Builder;
use iceberg_rust_spec::{
    identifier::FullIdentifier,
    spec::{
        materialized_view_metadata::MaterializedViewMetadata,
        partition::{PartitionSpec, DEFAULT_PARTITION_SPEC_ID},
        schema::{Schema, DEFAULT_SCHEMA_ID},
        sort::{SortOrder, DEFAULT_SORT_ORDER_ID},
        table_metadata::TableMetadata,
        view_metadata::{Version, ViewMetadata, DEFAULT_VERSION_ID},
    },
    view_metadata::Materialization,
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

/// Configuration for creating a new Iceberg table in a catalog
///
/// This struct contains all the necessary information to create a new table:
/// * Table name and optional location
/// * Schema definition
/// * Optional partition specification
/// * Optional sort order
/// * Optional properties
///
/// The struct implements Builder pattern for convenient construction and
/// can be serialized/deserialized using serde.
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
    /// Builds and registers a new table in the catalog
    ///
    /// # Arguments
    /// * `namespace` - The namespace where the table will be created
    /// * `catalog` - The catalog where the table will be registered
    ///
    /// # Returns
    /// * `Ok(Table)` - The newly created table
    /// * `Err(Error)` - If table creation fails, e.g. due to missing name or catalog errors
    ///
    /// This method finalizes the table configuration and registers it in the specified catalog.
    /// It uses the builder's current state to create the table metadata.
    pub async fn build(
        &mut self,
        namespace: &[String],
        catalog: Arc<dyn Catalog>,
    ) -> Result<Table, Error> {
        let name = self
            .name
            .as_ref()
            .ok_or(Error::NotFound("Name to create table".to_owned()))?;
        let identifier = Identifier::new(namespace, name);

        let create = self.create()?;

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
                .ok_or(Error::NotFound(format!("Location for table {}", self.name)))?,
            last_sequence_number: 0,
            last_updated_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
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

/// Configuration for creating a new Iceberg view in a catalog
///
/// This struct contains all the necessary information to create a new view:
/// * View name and optional location
/// * Schema definition
/// * View version specification
/// * Optional properties
///
/// # Type Parameters
/// * `T` - The materialization type for the view, typically `Option<()>` for regular views
///   or `FullIdentifier` for materialized views
///
/// The struct implements Builder pattern for convenient construction and
/// can be serialized/deserialized using serde.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Builder)]
#[serde(rename_all = "kebab-case")]
#[builder(build_fn(name = "create"), setter(prefix = "with"))]
pub struct CreateView<T: Materialization> {
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
    pub view_version: Version<T>,
    /// View properties
    #[builder(setter(each(name = "with_property")), default)]
    pub properties: HashMap<String, String>,
}

impl CreateViewBuilder<Option<()>> {
    /// Builds and registers a new view in the catalog
    ///
    /// # Arguments
    /// * `namespace` - The namespace where the view will be created
    /// * `catalog` - The catalog where the view will be registered
    ///
    /// # Returns
    /// * `Ok(View)` - The newly created view
    /// * `Err(Error)` - If view creation fails, e.g. due to missing name or catalog errors
    ///
    /// This method finalizes the view configuration and registers it in the specified catalog.
    /// It automatically sets default namespace and catalog values if not already specified.
    pub async fn build(
        &mut self,
        namespace: &[String],
        catalog: Arc<dyn Catalog>,
    ) -> Result<View, Error> {
        let name = self
            .name
            .as_ref()
            .ok_or(Error::NotFound("Name to create view".to_owned()))?;
        let identifier = Identifier::new(namespace, name);

        if let Some(version) = &mut self.view_version {
            if version.default_namespace().is_empty() {
                version.default_namespace = namespace.to_vec()
            }
            if version.default_catalog().is_none() && !catalog.name().is_empty() {
                version.default_catalog = Some(catalog.name().to_string())
            }
        }

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
                .ok_or(Error::NotFound(format!("Location for view {}", self.name)))?,
            current_version_id: DEFAULT_VERSION_ID,
            versions: HashMap::from_iter(vec![(DEFAULT_VERSION_ID, self.view_version)]),
            version_log: Vec::new(),
            schemas: HashMap::from_iter(vec![(DEFAULT_SCHEMA_ID, self.schema)]),
            properties: self.properties,
        })
    }
}

impl TryInto<MaterializedViewMetadata> for CreateView<FullIdentifier> {
    type Error = Error;
    fn try_into(self) -> Result<MaterializedViewMetadata, Self::Error> {
        Ok(MaterializedViewMetadata {
            view_uuid: Uuid::new_v4(),
            format_version: Default::default(),
            location: self.location.ok_or(Error::NotFound(format!(
                "Location for materialized view {}",
                self.name
            )))?,
            current_version_id: DEFAULT_VERSION_ID,
            versions: HashMap::from_iter(vec![(DEFAULT_VERSION_ID, self.view_version)]),
            version_log: Vec::new(),
            schemas: HashMap::from_iter(vec![(DEFAULT_SCHEMA_ID, self.schema)]),
            properties: self.properties,
        })
    }
}

/// Configuration for creating a new materialized view in an Iceberg catalog
///
/// This struct contains all the necessary information to create both a materialized view
/// and its underlying storage table:
/// * View name and optional location
/// * Schema definition
/// * View version specification with storage table reference
/// * Optional partition specification for the storage table
/// * Optional sort order for the storage table
/// * Separate properties for both view and storage table
///
/// The struct implements Builder pattern for convenient construction and
/// can be serialized/deserialized using serde.
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
    pub view_version: Version<FullIdentifier>,
    /// View properties
    #[builder(setter(each(name = "with_property")), default)]
    pub properties: HashMap<String, String>,
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
    /// Builds and registers a new materialized view in the catalog
    ///
    /// # Arguments
    /// * `namespace` - The namespace where the materialized view will be created
    /// * `catalog` - The catalog where the materialized view will be registered
    ///
    /// # Returns
    /// * `Ok(MaterializedView)` - The newly created materialized view
    /// * `Err(Error)` - If view creation fails, e.g. due to missing name or catalog errors
    ///
    /// This method finalizes the materialized view configuration and registers it in the specified catalog.
    /// It automatically:
    /// * Sets default namespace and catalog values if not specified
    /// * Creates the underlying storage table with the appropriate name suffix
    /// * Registers both the view and its storage table in the catalog
    pub async fn build(
        &mut self,
        namespace: &[String],
        catalog: Arc<dyn Catalog>,
    ) -> Result<MaterializedView, Error> {
        let name = self.name.as_ref().ok_or(Error::NotFound(
            "Name to create materialized view".to_owned(),
        ))?;
        let identifier = Identifier::new(namespace, name);

        if let Some(version) = &mut self.view_version {
            if version.default_namespace().is_empty() {
                version.default_namespace = namespace.to_vec()
            }
            if version.default_catalog().is_none() && !catalog.name().is_empty() {
                version.default_catalog = Some(catalog.name().to_string())
            }
        }

        let mut create = self.create()?;

        let version = Version {
            version_id: create.view_version.version_id,
            schema_id: create.view_version.schema_id,
            timestamp_ms: create.view_version.timestamp_ms,
            summary: create.view_version.summary.clone(),
            representations: create.view_version.representations.clone(),
            default_catalog: create.view_version.default_catalog,
            default_namespace: create.view_version.default_namespace,
            storage_table: FullIdentifier::new(
                None,
                identifier.namespace(),
                &(identifier.name().to_string() + STORAGE_TABLE_POSTFIX),
            ),
        };

        create.view_version = version;

        // Register materialized view in catalog
        catalog
            .clone()
            .create_materialized_view(identifier.clone(), create)
            .await
    }
}

impl From<CreateMaterializedView> for (CreateView<FullIdentifier>, CreateTable) {
    fn from(val: CreateMaterializedView) -> Self {
        let storage_table = val.view_version.storage_table.name().to_owned();
        (
            CreateView {
                name: val.name.clone(),
                location: val.location.clone(),
                schema: val.schema.clone(),
                view_version: val.view_version,
                properties: val.properties,
            },
            CreateTable {
                name: storage_table,
                location: val.location,
                schema: val.schema,
                partition_spec: val.partition_spec,
                write_order: val.write_order,
                stage_create: val.stage_create,
                properties: val.table_properties,
            },
        )
    }
}
