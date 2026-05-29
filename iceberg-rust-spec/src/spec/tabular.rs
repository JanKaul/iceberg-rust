/*!
 * Tabular metadata types and traits
 *
 * This module provides types for working with metadata for different tabular data structures
 * in Iceberg, including tables, views, and materialized views. It defines common traits and
 * implementations that allow working with these different types through a unified interface.
 *
 * The main types are:
 * - TabularMetadata: An enum for owned metadata of different tabular types
 * - TabularMetadataRef: A reference-based version for borrowed metadata
 *
 * These types allow code to handle tables, views, and materialized views generically while
 * preserving their specific metadata structures and behaviors.
 */

use std::{fmt, str};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{error::Error, schema::Schema};

use super::{
    materialized_view_metadata::MaterializedViewMetadata, table_metadata::TableMetadata,
    view_metadata::ViewMetadata,
};

/// Represents metadata for different types of tabular data structures in Iceberg
///
/// This enum provides a unified way to handle metadata for tables, views, and materialized views.
/// It allows working with different tabular types through a common interface while preserving
/// their specific metadata structures.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum TabularMetadata {
    /// Table metadata
    Table(TableMetadata),
    /// View metadata
    View(ViewMetadata),
    /// Materialized view metadata
    MaterializedView(MaterializedViewMetadata),
}

impl TabularMetadata {
    pub fn as_ref(&self) -> TabularMetadataRef<'_> {
        match self {
            TabularMetadata::Table(table) => TabularMetadataRef::Table(table),
            TabularMetadata::View(view) => TabularMetadataRef::View(view),
            TabularMetadata::MaterializedView(matview) => {
                TabularMetadataRef::MaterializedView(matview)
            }
        }
    }
}

impl fmt::Display for TabularMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            &serde_json::to_string(self).map_err(|_| fmt::Error)?,
        )
    }
}

impl str::FromStr for TabularMetadata {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(Error::from)
    }
}

impl From<TableMetadata> for TabularMetadata {
    fn from(value: TableMetadata) -> Self {
        TabularMetadata::Table(value)
    }
}

impl From<ViewMetadata> for TabularMetadata {
    fn from(value: ViewMetadata) -> Self {
        TabularMetadata::View(value)
    }
}

impl From<MaterializedViewMetadata> for TabularMetadata {
    fn from(value: MaterializedViewMetadata) -> Self {
        TabularMetadata::MaterializedView(value)
    }
}

/// A reference wrapper for different types of tabular metadata
///
/// This enum provides a way to reference the different types of tabular metadata
/// (tables, views, materialized views) without taking ownership. It implements
/// common functionality for accessing metadata properties across all tabular types.
#[derive(Serialize)]
#[serde(untagged)]
pub enum TabularMetadataRef<'a> {
    /// Table metadata
    Table(&'a TableMetadata),
    /// View metadata
    View(&'a ViewMetadata),
    /// Materialized view metadata
    MaterializedView(&'a MaterializedViewMetadata),
}

impl TabularMetadataRef<'_> {
    /// Returns the UUID of the tabular object
    ///
    /// # Returns
    /// * A reference to the UUID that uniquely identifies this table, view, or materialized view
    pub fn uuid(&self) -> &Uuid {
        match self {
            TabularMetadataRef::Table(table) => &table.table_uuid,
            TabularMetadataRef::View(view) => &view.view_uuid,
            TabularMetadataRef::MaterializedView(matview) => &matview.view_uuid,
        }
    }

    /// Returns the storage location of the tabular object
    ///
    /// # Returns
    /// * A string reference to the base storage location (e.g. S3 path, file path)
    ///   where this table, view, or materialized view's data is stored
    pub fn location(&self) -> &str {
        match self {
            TabularMetadataRef::Table(table) => &table.location,
            TabularMetadataRef::View(view) => &view.location,
            TabularMetadataRef::MaterializedView(matview) => &matview.location,
        }
    }

    /// Returns the current sequence number or version ID of the tabular object
    ///
    /// # Returns
    /// * For tables: The last sequence number used to create a snapshot
    /// * For views and materialized views: The current version ID
    pub fn sequence_number(&self) -> i64 {
        match self {
            TabularMetadataRef::Table(table) => table.last_sequence_number,
            TabularMetadataRef::View(view) => view.current_version_id,
            TabularMetadataRef::MaterializedView(matview) => matview.current_version_id,
        }
    }

    /// Returns the current schema for the tabular object
    ///
    /// # Arguments
    /// * `branch` - Optional branch name to get schema from
    ///
    /// # Returns
    /// * `Ok(&Schema)` - The current schema for this table, view, or materialized view
    /// * `Err(Error)` - If the schema cannot be retrieved
    pub fn current_schema(&self, branch: Option<&str>) -> Result<&Schema, Error> {
        match self {
            TabularMetadataRef::Table(table) => table.current_schema(branch),
            TabularMetadataRef::View(view) => view.current_schema(branch),
            TabularMetadataRef::MaterializedView(matview) => matview.current_schema(branch),
        }
    }
}

impl<'a> From<&'a TableMetadata> for TabularMetadataRef<'a> {
    fn from(value: &'a TableMetadata) -> Self {
        TabularMetadataRef::Table(value)
    }
}

impl<'a> From<&'a ViewMetadata> for TabularMetadataRef<'a> {
    fn from(value: &'a ViewMetadata) -> Self {
        TabularMetadataRef::View(value)
    }
}

impl<'a> From<&'a MaterializedViewMetadata> for TabularMetadataRef<'a> {
    fn from(value: &'a MaterializedViewMetadata) -> Self {
        TabularMetadataRef::MaterializedView(value)
    }
}

impl<'a> From<&'a TabularMetadata> for TabularMetadataRef<'a> {
    fn from(value: &'a TabularMetadata) -> Self {
        match value {
            TabularMetadata::Table(table) => TabularMetadataRef::Table(table),
            TabularMetadata::View(view) => TabularMetadataRef::View(view),
            TabularMetadata::MaterializedView(matview) => {
                TabularMetadataRef::MaterializedView(matview)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::str::FromStr;

    use uuid::Uuid;

    use crate::{
        error::Error,
        spec::{
            schema::SchemaBuilder,
            table_metadata::{TableMetadata, TableMetadataBuilder},
            types::{PrimitiveType, StructField, Type},
            view_metadata::{Version, ViewMetadata, ViewMetadataBuilder},
        },
        tabular::{TabularMetadata, TabularMetadataRef},
    };

    fn table_fixture(uuid: Uuid, last_sequence_number: i64) -> TableMetadata {
        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                1,
                "id",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .build()
            .unwrap();
        TableMetadataBuilder::default()
            .table_uuid(uuid)
            .location("s3://tabular/table".to_string())
            .current_schema_id(0)
            .schemas(HashMap::from_iter(vec![(0, schema)]))
            .last_sequence_number(last_sequence_number)
            .build()
            .unwrap()
    }

    fn view_fixture(uuid: Uuid, current_version_id: i64) -> ViewMetadata {
        let schema = SchemaBuilder::default()
            .with_schema_id(0)
            .with_struct_field(StructField::new(
                1,
                "id",
                true,
                Type::Primitive(PrimitiveType::Long),
                None,
            ))
            .build()
            .unwrap();
        let version: Version<Option<()>> = Version::builder()
            .version_id(current_version_id)
            .schema_id(0)
            .timestamp_ms(0)
            .default_namespace(vec!["ns".to_string()])
            .build()
            .unwrap();

        ViewMetadataBuilder::default()
            .view_uuid(uuid)
            .location("s3://tabular/view".to_string())
            .current_version_id(current_version_id)
            .with_version((current_version_id, version))
            .with_schema((0, schema))
            .build()
            .unwrap()
    }

    #[test]
    fn test_tabular_metadata_from_table_metadata_wraps_into_table_variant() {
        let metadata = table_fixture(Uuid::nil(), 0);
        let wrapped: TabularMetadata = metadata.clone().into();
        assert!(matches!(wrapped, TabularMetadata::Table(_)));
    }

    #[test]
    fn test_tabular_metadata_from_view_metadata_wraps_into_view_variant() {
        let metadata = view_fixture(Uuid::nil(), 1);
        let wrapped: TabularMetadata = metadata.clone().into();
        assert!(matches!(wrapped, TabularMetadata::View(_)));
    }

    #[test]
    fn test_tabular_metadata_as_ref_matches_owned_variant() {
        let table = TabularMetadata::Table(table_fixture(Uuid::nil(), 0));
        let view = TabularMetadata::View(view_fixture(Uuid::nil(), 1));
        assert!(matches!(table.as_ref(), TabularMetadataRef::Table(_)));
        assert!(matches!(view.as_ref(), TabularMetadataRef::View(_)));
    }

    #[test]
    fn test_tabular_metadata_display_then_fromstr_round_trips_table() {
        let metadata: TabularMetadata = table_fixture(Uuid::from_u128(0xCAFE), 7).into();
        let rendered = format!("{metadata}");
        let parsed = TabularMetadata::from_str(&rendered).unwrap();
        assert_eq!(parsed, metadata);
    }

    #[test]
    fn test_tabular_metadata_ref_uuid_returns_per_variant_uuid() {
        let table_uuid = Uuid::from_u128(0xAA);
        let view_uuid = Uuid::from_u128(0xBB);
        let table: TabularMetadata = table_fixture(table_uuid, 0).into();
        let view: TabularMetadata = view_fixture(view_uuid, 1).into();
        assert_eq!(table.as_ref().uuid(), &table_uuid);
        assert_eq!(view.as_ref().uuid(), &view_uuid);
    }

    #[test]
    fn test_tabular_metadata_ref_location_returns_per_variant_location() {
        let table: TabularMetadata = table_fixture(Uuid::nil(), 0).into();
        let view: TabularMetadata = view_fixture(Uuid::nil(), 1).into();
        assert_eq!(table.as_ref().location(), "s3://tabular/table");
        assert_eq!(view.as_ref().location(), "s3://tabular/view");
    }

    #[test]
    fn test_tabular_metadata_ref_sequence_number_maps_to_table_seq_and_view_version() {
        let table: TabularMetadata = table_fixture(Uuid::nil(), 17).into();
        let view: TabularMetadata = view_fixture(Uuid::nil(), 4).into();
        assert_eq!(table.as_ref().sequence_number(), 17);
        // Views use current_version_id as their sequence-number analogue.
        assert_eq!(view.as_ref().sequence_number(), 4);
    }

    #[test]
    fn test_tabular_metadata_ref_current_schema_returns_per_variant_schema() {
        let table: TabularMetadata = table_fixture(Uuid::nil(), 0).into();
        let view: TabularMetadata = view_fixture(Uuid::nil(), 1).into();
        // Both fixtures pin schema-id 0 with a single Long field named "id".
        assert_eq!(
            table.as_ref().current_schema(None).unwrap().fields()[0].name,
            "id"
        );
        assert_eq!(
            view.as_ref().current_schema(None).unwrap().fields()[0].name,
            "id"
        );
    }

    #[test]
    fn test_deserialize_tabular_view_data_v1() -> Result<(), Error> {
        let data = r#"
        {
        "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
        "format-version" : 1,
        "location" : "s3://bucket/warehouse/default.db/event_agg",
        "current-version-id" : 1,
        "properties" : {
            "comment" : "Daily event counts"
        },
        "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : {
            "operation" : "create",
            "engine-name" : "Spark",
            "engineVersion" : "3.3.2"
            },
            "representations" : [ {
            "type" : "sql",
            "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
            "dialect" : "spark"
            } ]
        } ],
        "schemas": [ {
            "schema-id": 1,
            "type" : "struct",
            "fields" : [ {
            "id" : 1,
            "name" : "event_count",
            "required" : false,
            "type" : "int",
            "doc" : "Count of events"
            }, {
            "id" : 2,
            "name" : "event_date",
            "required" : false,
            "type" : "date"
            } ]
        } ],
        "version-log" : [ {
            "timestamp-ms" : 1573518431292,
            "version-id" : 1
        } ]
        }
        "#;
        let metadata =
            serde_json::from_str::<TabularMetadata>(data).expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: TabularMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        assert_eq!(metadata, metadata_two);

        Ok(())
    }
}
