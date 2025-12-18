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
#[builder(
    build_fn(name = "create", error = "Error", validate = "Self::validate"),
    setter(prefix = "with")
)]
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
    /// Validates the table configuration
    ///
    /// Performs the following checks:
    /// * Schema field IDs are unique
    /// * Partition spec fields reference valid schema fields
    /// * Sort order fields reference valid schema fields
    ///
    /// # Returns
    /// * `Ok(())` - If all validations pass
    /// * `Err(Error)` - If any validation fails with contextual error message
    fn validate(&self) -> Result<(), Error> {
        let name = self
            .name
            .as_ref()
            .ok_or(Error::NotFound("Table name is required".to_string()))?;

        let schema = self
            .schema
            .as_ref()
            .ok_or(Error::NotFound("Table schema is required".to_string()))?;

        // Validate schema field IDs are unique
        let field_ids: Vec<i32> = schema.fields().iter().map(|f| f.id).collect();
        let unique_ids: std::collections::HashSet<_> = field_ids.iter().collect();
        if field_ids.len() != unique_ids.len() {
            return Err(Error::InvalidFormat(format!(
                "Schema for table '{}' contains duplicate field IDs",
                name
            )));
        }

        // Validate partition spec references valid schema fields
        if let Some(Some(spec)) = &self.partition_spec {
            for field in spec.fields() {
                let source_id = field.source_id();
                if !schema.fields().iter().any(|f| f.id == *source_id) {
                    return Err(Error::NotFound(format!(
                            "Partition field '{}' references non-existent schema field ID {} in table '{}'",
                            field.name(),
                            source_id,
                            name
                        )));
                }
            }
        }

        // Validate sort order references valid schema fields
        if let Some(Some(order)) = &self.write_order {
            for field in &order.fields {
                let source_id = field.source_id;
                if !schema.fields().iter().any(|f| f.id == source_id) {
                    return Err(Error::NotFound(format!(
                        "Sort order field references non-existent schema field ID {} in table '{}'",
                        source_id, name
                    )));
                }
            }
        }

        Ok(())
    }

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
#[builder(build_fn(name = "create", error = "Error"), setter(prefix = "with"))]
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
#[builder(build_fn(name = "create", error = "Error"), setter(prefix = "with"))]
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

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_rust_spec::spec::{
        partition::{PartitionField, PartitionSpecBuilder, Transform},
        sort::{NullOrder, SortDirection, SortField, SortOrderBuilder},
        types::{PrimitiveType, StructField, Type},
    };

    /// Helper function to create a simple valid schema for testing
    fn create_test_schema() -> Schema {
        Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 2,
                name: "name".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 3,
                name: "timestamp".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::Timestamp),
                doc: None,
            })
            .build()
            .unwrap()
    }

    /// Helper function to create a schema with duplicate field IDs (invalid)
    fn create_duplicate_field_id_schema() -> Schema {
        Schema::builder()
            .with_struct_field(StructField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Long),
                doc: None,
            })
            .with_struct_field(StructField {
                id: 1, // Duplicate ID
                name: "name".to_string(),
                required: false,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
            })
            .build()
            .unwrap()
    }

    #[test]
    fn test_create_table_builder_valid() {
        let schema = create_test_schema();
        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema)
            .create();

        assert!(result.is_ok(), "Valid table creation should succeed");
        let create_table = result.unwrap();
        assert_eq!(create_table.name, "test_table");
        assert_eq!(create_table.location, Some("/test/location".to_string()));
    }

    #[test]
    fn test_create_table_builder_missing_name() {
        let schema = create_test_schema();
        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_location("/test/location")
            .with_schema(schema)
            .create();

        assert!(result.is_err(), "Table creation without name should fail");
    }

    #[test]
    fn test_create_table_builder_missing_schema() {
        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_name("test_table")
            .with_location("/test/location")
            .create();

        assert!(result.is_err(), "Table creation without schema should fail");
    }

    #[test]
    fn test_create_table_validation_duplicate_field_ids() {
        let schema = create_duplicate_field_id_schema();
        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema)
            .create();

        assert!(
            result.is_err(),
            "Table creation with duplicate field IDs should fail"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::InvalidFormat(_)),
            "Error should be InvalidFormat, got: {:?}",
            err
        );
    }

    #[test]
    fn test_create_table_validation_invalid_partition_spec() {
        let schema = create_test_schema();

        // Create partition spec that references non-existent field ID 999
        let mut partition_spec_builder = PartitionSpecBuilder::default();
        let invalid_partition_spec = partition_spec_builder
            .with_spec_id(1)
            .with_partition_field(PartitionField::new(
                999, // Non-existent source field ID
                1000,
                "invalid_partition",
                Transform::Identity,
            ))
            .build()
            .unwrap();

        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema)
            .with_partition_spec(invalid_partition_spec)
            .create();

        assert!(
            result.is_err(),
            "Table creation with invalid partition spec should fail"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::NotFound(_)),
            "Error should be NotFound for invalid partition field reference, got: {:?}",
            err
        );
    }

    #[test]
    fn test_create_table_validation_valid_partition_spec() {
        let schema = create_test_schema();

        // Create partition spec that references valid field ID 1
        let mut partition_spec_builder = PartitionSpecBuilder::default();
        let partition_spec = partition_spec_builder
            .with_spec_id(1)
            .with_partition_field(PartitionField::new(
                1, // Valid source field ID from schema
                1000,
                "id_partition",
                Transform::Identity,
            ))
            .build()
            .unwrap();

        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema)
            .with_partition_spec(partition_spec)
            .create();

        assert!(
            result.is_ok(),
            "Table creation with valid partition spec should succeed"
        );
    }

    #[test]
    fn test_create_table_validation_invalid_sort_order() {
        let schema = create_test_schema();

        // Create sort order that references non-existent field ID 999
        let mut sort_order_builder = SortOrderBuilder::default();
        let invalid_sort_order = sort_order_builder
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 999, // Non-existent source field ID
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build()
            .unwrap();

        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema)
            .with_sort_order(invalid_sort_order)
            .create();

        assert!(
            result.is_err(),
            "Table creation with invalid sort order should fail"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::NotFound(_)),
            "Error should be NotFound for invalid sort order field reference, got: {:?}",
            err
        );
    }

    #[test]
    fn test_create_table_validation_valid_sort_order() {
        let schema = create_test_schema();

        // Create sort order that references valid field ID 1
        let mut sort_order_builder = SortOrderBuilder::default();
        let sort_order = sort_order_builder
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 1, // Valid source field ID from schema
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build()
            .unwrap();

        let mut builder = CreateTableBuilder::default();
        let result = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema)
            .with_sort_order(sort_order)
            .create();

        assert!(
            result.is_ok(),
            "Table creation with valid sort order should succeed"
        );
    }

    #[test]
    fn test_create_table_try_into_metadata() {
        let schema = create_test_schema();
        let mut builder = CreateTableBuilder::default();
        let create_table = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema.clone())
            .create()
            .unwrap();

        let metadata: Result<TableMetadata, Error> = create_table.try_into();
        assert!(
            metadata.is_ok(),
            "Conversion to TableMetadata should succeed"
        );

        let metadata = metadata.unwrap();
        assert_eq!(metadata.location, "/test/location");
        assert_eq!(metadata.current_schema_id, DEFAULT_SCHEMA_ID);
        assert_eq!(metadata.schemas.len(), 1);
        assert_eq!(metadata.default_spec_id, DEFAULT_PARTITION_SPEC_ID);
        assert_eq!(metadata.default_sort_order_id, DEFAULT_SORT_ORDER_ID);
        assert_eq!(metadata.last_column_id, 3); // Max field ID from schema
    }

    #[test]
    fn test_create_table_serialization_round_trip() {
        let schema = create_test_schema();
        let mut partition_spec_builder = PartitionSpecBuilder::default();
        let partition_spec = partition_spec_builder
            .with_spec_id(1)
            .with_partition_field(PartitionField::new(
                1,
                1000,
                "id_partition",
                Transform::Identity,
            ))
            .build()
            .unwrap();

        let mut builder = CreateTableBuilder::default();
        let create_table = builder
            .with_name("test_table")
            .with_location("/test/location")
            .with_schema(schema)
            .with_partition_spec(partition_spec)
            .create()
            .unwrap();

        // Serialize
        let json = serde_json::to_string(&create_table).unwrap();

        // Deserialize
        let deserialized: CreateTable = serde_json::from_str(&json).unwrap();

        // Verify equality
        assert_eq!(create_table, deserialized);
    }
}
