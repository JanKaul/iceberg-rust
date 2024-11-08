/*! Enum for Metadata of Table, View or Materialized View
*/

use std::{fmt, str};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{error::Error, schema::Schema};

use super::{
    materialized_view_metadata::MaterializedViewMetadata, table_metadata::TableMetadata,
    view_metadata::ViewMetadata,
};
/// Metadata of an iceberg relation
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

pub enum TabularMetadataRef<'a> {
    /// Table metadata
    Table(&'a TableMetadata),
    /// View metadata
    View(&'a ViewMetadata),
    /// Materialized view metadata
    MaterializedView(&'a MaterializedViewMetadata),
}

impl<'a> TabularMetadataRef<'a> {
    /// Get uuid of tabular
    pub fn uuid(&self) -> &Uuid {
        match self {
            TabularMetadataRef::Table(table) => &table.table_uuid,
            TabularMetadataRef::View(view) => &view.view_uuid,
            TabularMetadataRef::MaterializedView(matview) => &matview.view_uuid,
        }
    }
    /// Get location for tabular
    pub fn location(&self) -> &str {
        match self {
            TabularMetadataRef::Table(table) => &table.location,
            TabularMetadataRef::View(view) => &view.location,
            TabularMetadataRef::MaterializedView(matview) => &matview.location,
        }
    }
    /// Get sequence number for tabular
    pub fn sequence_number(&self) -> i64 {
        match self {
            TabularMetadataRef::Table(table) => table.last_sequence_number,
            TabularMetadataRef::View(view) => view.current_version_id,
            TabularMetadataRef::MaterializedView(matview) => matview.current_version_id,
        }
    }
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

    use crate::{error::Error, tabular::TabularMetadata};

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
